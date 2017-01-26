/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.io.encoded;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.Pool.PoolObjectHelper;
import org.apache.hadoop.hive.common.io.DataCache.BooleanRef;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.DataCache.DiskRangeListFactory;
import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch.ColumnStreamData;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.ConsumerFeedback;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.cache.BufferUsageManager;
import org.apache.hadoop.hive.llap.cache.LlapDataBuffer;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.cache.SerDeLowLevelCacheImpl;
import org.apache.hadoop.hive.llap.cache.SerDeLowLevelCacheImpl.FileData;
import org.apache.hadoop.hive.llap.cache.SerDeLowLevelCacheImpl.StripeData;
import org.apache.hadoop.hive.llap.counters.LlapIOCounters;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.decode.GenericColumnVectorProducer.SerDeStripeMetadata;
import org.apache.hadoop.hive.llap.io.decode.OrcEncodedDataConsumer;
import org.apache.hadoop.hive.ql.io.HdfsUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.WriterOptions;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.ql.io.orc.encoded.CacheChunk;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader.OrcEncodedColumnBatch;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.FixedSizedObjectPool;
import org.apache.hive.common.util.Ref;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcUtils;
import org.apache.orc.OrcFile.EncodingStrategy;
import org.apache.orc.OrcFile.Version;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcProto.ColumnEncoding;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.OutStream;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.PhysicalWriter.OutputReceiver;
import org.apache.orc.impl.SchemaEvolution;
import org.apache.orc.impl.StreamName;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.counters.TezCounters;

import com.google.common.collect.Lists;

public class SerDeEncodedDataReader extends CallableWithNdc<Void>
    implements ConsumerFeedback<OrcEncodedColumnBatch> {

  public static final FixedSizedObjectPool<ColumnStreamData> CSD_POOL =
      new FixedSizedObjectPool<>(8192, new PoolObjectHelper<ColumnStreamData>() {
        @Override
        public ColumnStreamData create() {
          return new ColumnStreamData();
        }
        @Override
        public void resetBeforeOffer(ColumnStreamData t) {
          t.reset();
        }
      });
  public static final FixedSizedObjectPool<OrcEncodedColumnBatch> ECB_POOL =
      new FixedSizedObjectPool<>(1024, new PoolObjectHelper<OrcEncodedColumnBatch>() {
        @Override
        public OrcEncodedColumnBatch create() {
          return new OrcEncodedColumnBatch();
        }
        @Override
        public void resetBeforeOffer(OrcEncodedColumnBatch t) {
          t.reset();
        }
      });
  public static final FixedSizedObjectPool<CacheChunk> TCC_POOL =
      new FixedSizedObjectPool<>(1024, new PoolObjectHelper<CacheChunk>() {
      @Override
      public CacheChunk create() {
        return new CacheChunk();
      }
      @Override
      public void resetBeforeOffer(CacheChunk t) {
        t.reset();
      }
    });
  private final static DiskRangeListFactory CC_FACTORY = new DiskRangeListFactory() {
    @Override
    public DiskRangeList createCacheChunk(MemoryBuffer buffer, long offset, long end) {
      CacheChunk tcc = TCC_POOL.take();
      tcc.init(buffer, offset, end);
      return tcc;
    }
  };

  private final SerDeLowLevelCacheImpl cache;
  private final BufferUsageManager bufferManager;
  private final Configuration daemonConf;
  private final FileSplit split;
  private List<Integer> columnIds;
  private final OrcEncodedDataConsumer consumer;
  private final QueryFragmentCounters counters;
  private final UserGroupInformation ugi;
  private final Map<Path, PartitionDesc> parts;

  private final Object fileKey;
  private final FileSystem fs;

  private volatile boolean isStopped = false;
  private final Deserializer sourceSerDe;
  private final InputFormat<?, ?> sourceInputFormat;
  private final Reporter reporter;
  private final JobConf jobConf;
  private final int allocSize;
  private final int targetSliceRowCount;
  private final boolean isLrrEnabled;

  private final boolean[] writerIncludes;
  private EncodingWriter writer = null;
  private CacheWriter cacheWriter = null;
  /**
   * Data from cache currently being processed. We store it here so that we could decref
   * it in case of failures. We remove each slice from the data after it has been sent to
   * the consumer, at which point the consumer is responsible for it.
   */
  private FileData cachedData;

  public SerDeEncodedDataReader(SerDeLowLevelCacheImpl cache,
      BufferUsageManager bufferManager, Configuration daemonConf, FileSplit split,
      List<Integer> columnIds, OrcEncodedDataConsumer consumer, JobConf jobConf, Reporter reporter,
      InputFormat<?, ?> sourceInputFormat, Deserializer sourceSerDe,
      QueryFragmentCounters counters, TypeDescription schema, Map<Path, PartitionDesc> parts)
          throws IOException {
    this.cache = cache;
    this.bufferManager = bufferManager;
    this.parts = parts;
    this.daemonConf = new Configuration(daemonConf);
    // Disable dictionary encoding for the writer.
    this.daemonConf.setDouble(OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD.name(), 0);
    this.split = split;
    this.columnIds = columnIds;
    this.allocSize = determineAllocSize(bufferManager, daemonConf);
    boolean isInTest = HiveConf.getBoolVar(daemonConf, ConfVars.HIVE_IN_TEST);
    Configuration sliceConf = isInTest ? jobConf : daemonConf;
    this.targetSliceRowCount = HiveConf.getIntVar(
        sliceConf, ConfVars.LLAP_IO_ENCODE_SLICE_ROW_COUNT);
    this.isLrrEnabled = HiveConf.getBoolVar(sliceConf, ConfVars.LLAP_IO_ENCODE_SLICE_LRR);
    if (this.columnIds != null) {
      Collections.sort(this.columnIds);
    }
    this.consumer = consumer;
    this.counters = counters;
    try {
      this.ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    fs = split.getPath().getFileSystem(daemonConf);
    fileKey = determineFileId(fs, split,
        HiveConf.getBoolVar(daemonConf, ConfVars.LLAP_CACHE_ALLOW_SYNTHETIC_FILEID));
    this.sourceInputFormat = sourceInputFormat;
    this.sourceSerDe = sourceSerDe;
    this.reporter = reporter;
    this.jobConf = jobConf;
    this.writerIncludes = OrcInputFormat.genIncludedColumns(schema, columnIds);
    SchemaEvolution evolution = new SchemaEvolution(schema,
        new Reader.Options(jobConf).include(writerIncludes));
    consumer.setSchemaEvolution(evolution);
  }

  private static int determineAllocSize(BufferUsageManager bufferManager, Configuration conf) {
    long allocSize = HiveConf.getSizeVar(conf, ConfVars.LLAP_IO_ENCODE_ALLOC_SIZE);
    int maxAllocSize = bufferManager.getAllocator().getMaxAllocation();
    if (allocSize > maxAllocSize) {
      LlapIoImpl.LOG.error("Encode allocation size " + allocSize + " is being capped to the maximum "
          + "allocation size " + bufferManager.getAllocator().getMaxAllocation());
      allocSize = maxAllocSize;
    }
    return (int)allocSize;
  }

  @Override
  public void stop() {
    LlapIoImpl.LOG.debug("Encoded reader is being stopped");
    isStopped = true;
  }

  @Override
  public void pause() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unpause() {
    throw new UnsupportedOperationException();
  }

  // TODO: move to base class?
  @Override
  protected Void callInternal() throws IOException, InterruptedException {
    return ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        return performDataRead();
      }
    });
  }

  public static class CacheOutStream extends OutStream {
    private final CacheOutputReceiver receiver;
    public CacheOutStream(String name, int bufferSize, CompressionCodec codec,
        CacheOutputReceiver receiver) throws IOException {
      super(name, bufferSize, codec, receiver);
      this.receiver = receiver;
    }

    @Override
    public void clear() throws IOException {
      super.clear();
      receiver.clear();
    }
  }

  /** A row-based (Writable) reader that may also be able to report file offsets. */
  interface ReaderWithOffsets {
    /** Moves the reader to the next row. */
    boolean next() throws IOException;
    /** Gets the current row. */
    Writable getCurrentRow();
    /** Closes the reader. */
    void close() throws IOException;

    /** Whether this reader actually supports offsets. */
    boolean hasOffsets();
    /** Gets the start offset of the current row, or -1 if unknown. */
    long getCurrentRowStartOffset();
    /** Gets the end offset of the current row, or -1 if unknown. */
    long getCurrentRowEndOffset();
  }

  public static class CacheWriter implements PhysicalWriter {
    // Struct.
    private static class CacheStreamData {
      private final List<MemoryBuffer> data;
      private final boolean isSuppressed;
      private final StreamName name;
      public CacheStreamData(boolean isSuppressed, StreamName name, List<MemoryBuffer> data) {
        this.isSuppressed = isSuppressed;
        this.name = name;
        this.data = data;
      }
      @Override
      public String toString() {
        return "CacheStreamData [name=" + name + ", isSuppressed="
            + isSuppressed + ", data=" + toString(data) + "]";
      }
      private static String toString(List<MemoryBuffer> data) {
        String s = "";
        for (MemoryBuffer buffer : data) {
          s += LlapDataBuffer.toDataString(buffer) + ", ";
        }
        return s;
      }
    }

    private static class CacheStripeData {
      private List<ColumnEncoding> encodings;
      private long rowCount = -1;
      private long knownTornStart, firstRowStart, lastRowStart, lastRowEnd;
      private Map<Integer, List<CacheStreamData>> colStreams = new HashMap<>();
      @Override
      public String toString() {
        return ("{disk data knownTornStart=" + knownTornStart
            + ", firstRowStart=" + firstRowStart + ", lastRowStart="
            + lastRowStart + ", lastRowEnd=" + lastRowEnd + ", rowCount=" + rowCount
            + ", encodings=" + encodings + ", streams=" + colStreams + "}").replace('\n', ' ');
      }

      public String toCoordinateString() {
        return "knownTornStart=" + knownTornStart + ", firstRowStart=" + firstRowStart
            + ", lastRowStart=" + lastRowStart + ", lastRowEnd=" + lastRowEnd;
      }
    }

    private CacheStripeData currentStripe;
    private final List<CacheStripeData> stripes = new ArrayList<>();
    private final BufferUsageManager bufferManager;
    private final int bufferSize;
    private final List<Integer> columnIds;
    private final boolean[] writerIncludes;
    // These are global since ORC reuses objects between stripes.
    private final Map<StreamName, OutputReceiver> streams = new HashMap<>();
    private final Map<Integer, List<CacheOutputReceiver>> colStreams = new HashMap<>();
    private final boolean doesSourceHaveIncludes;

    public CacheWriter(BufferUsageManager bufferManager, int bufferSize,
        List<Integer> columnIds, boolean[] writerIncludes, boolean doesSourceHaveIncludes) {
      this.bufferManager = bufferManager;
      this.bufferSize = bufferSize;
      this.columnIds = columnIds;
      assert writerIncludes != null; // Taken care of on higher level.
      this.writerIncludes = writerIncludes;
      this.doesSourceHaveIncludes = doesSourceHaveIncludes;
      startStripe();
    }

    private void startStripe() {
      if (currentStripe != null) {
        stripes.add(currentStripe);
      }
      currentStripe = new CacheStripeData();
    }

    @Override
    public void writeFileMetadata(OrcProto.Metadata.Builder builder) throws IOException {
    }

    @Override
    public void writeFileFooter(OrcProto.Footer.Builder builder) throws IOException {
      OrcProto.Footer footer = builder.build();
      validateIncludes(footer);
    }

    public void validateIncludes(OrcProto.Footer footer) throws IOException {
      if (doesSourceHaveIncludes) return; // Irrelevant.
      boolean[] translatedIncludes = columnIds == null ? null : OrcInputFormat.genIncludedColumns(
          OrcUtils.convertTypeFromProtobuf(footer.getTypesList(), 0), columnIds);
      if (translatedIncludes == null) {
        throwIncludesMismatchError(translatedIncludes);
      }
      int len = Math.min(translatedIncludes.length, writerIncludes.length);
      for (int i = 0; i < len; ++i) {
        // Translated includes may be a superset of writer includes due to cache.
        if (!translatedIncludes[i] && writerIncludes[i]) {
          throwIncludesMismatchError(translatedIncludes);
        }
      }
      if (translatedIncludes.length < writerIncludes.length) {
        for (int i = len; i < writerIncludes.length; ++i) {
          if (writerIncludes[i]) {
            throwIncludesMismatchError(translatedIncludes);
          }
        }
      }

    }

    private String throwIncludesMismatchError(boolean[] translated) throws IOException {
      String s = "Includes derived from the original table: " + DebugUtils.toString(writerIncludes)
          + " but the ones derived from writer types are: " + DebugUtils.toString(translated);
      LlapIoImpl.LOG.error(s);
      throw new IOException(s);
    }

    @Override
    public long writePostScript(OrcProto.PostScript.Builder builder) {
      return 0;
    }

    @Override
    public void close() throws IOException {
      // Closed from ORC writer, we still need the data. Do not discard anything.
    }

    public void discardData() {
      LlapIoImpl.LOG.debug("Discarding disk data (if any wasn't cached)");
      for (CacheStripeData stripe : stripes) {
        if (stripe.colStreams == null || stripe.colStreams.isEmpty()) continue;
        for (List<CacheStreamData> streams : stripe.colStreams.values()) {
          for (CacheStreamData cos : streams) {
            for (MemoryBuffer buffer : cos.data) {
              if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
                LlapIoImpl.CACHE_LOGGER.trace("Deallocating " + buffer);
              }
              bufferManager.getAllocator().deallocate(buffer);
            }
          }
        }
        stripe.colStreams.clear();
      }
    }

    @Override
    public OutputReceiver createDataStream(StreamName name) throws IOException {
      OutputReceiver or = streams.get(name);
      if (or != null) return or;
      if (isNeeded(name)) {
        if (LlapIoImpl.LOG.isTraceEnabled()) {
          LlapIoImpl.LOG.trace("Creating cache receiver for " + name);
        }
        CacheOutputReceiver cor = new CacheOutputReceiver(bufferManager, name);
        or = cor;
        List<CacheOutputReceiver> list = colStreams.get(name.getColumn());
        if (list == null) {
          list = new ArrayList<>();
          colStreams.put(name.getColumn(), list);
        }
        list.add(cor);
      } else {
        if (LlapIoImpl.LOG.isTraceEnabled()) {
          LlapIoImpl.LOG.trace("Creating null receiver for " + name);
        }
        or = new NullOutputReceiver(name);
      }
      streams.put(name, or);
      return or;
    }

    @Override
    public void writeHeader() throws IOException {

    }

    @Override
    public void writeIndex(StreamName name, OrcProto.RowIndex.Builder index, CompressionCodec codec) throws IOException {
      // TODO: right now we treat each slice as a stripe with a single RG and never bother
      //       with indexes. In phase 4, we need to add indexing and filtering.
    }

    @Override
    public void writeBloomFilter(StreamName name, OrcProto.BloomFilterIndex.Builder bloom, CompressionCodec codec) throws IOException {

    }

    @Override
    public void finalizeStripe(
        OrcProto.StripeFooter.Builder footer,
        OrcProto.StripeInformation.Builder dirEntry)
        throws IOException {
      List<ColumnEncoding> allEnc = footer.getColumnsList();
      OrcProto.StripeInformation si = dirEntry.build();
      if (LlapIoImpl.LOG.isTraceEnabled()) {
        LlapIoImpl.LOG.trace(("Finalizing stripe " + footer.build() + " => " + si)
            .replace('\n', ' '));
      }
      if (doesSourceHaveIncludes) {
        currentStripe.encodings = new ArrayList<>(writerIncludes.length);
        for (int i = 0; i < writerIncludes.length; ++i) {
          currentStripe.encodings.add(null);
        }
        currentStripe.encodings.set(0, allEnc.get(0));
        for (int i = 1; i < allEnc.size(); ++i) {
          int colIx = getSparseOrcIndexFromDenseDest(i);
          // LlapIoImpl.LOG.info("Setting enc " + i + "; " + colIx + " to " + allEnc.get(i));
          currentStripe.encodings.set(colIx, allEnc.get(i));
        }
      } else {
        currentStripe.encodings = new ArrayList<>(allEnc);
        for (int i = 0; i < currentStripe.encodings.size(); ++i) {
          // Don't record encodings for unneeded columns.
          if (writerIncludes[i]) continue;
          currentStripe.encodings.set(i, null);
        }
      }
      currentStripe.rowCount = si.getNumberOfRows();
      // ORC writer reuses streams, so we need to clean them here and extract data.
      for (Map.Entry<Integer, List<CacheOutputReceiver>> e : colStreams.entrySet()) {
        int colIx = e.getKey();
        List<CacheOutputReceiver> streams = e.getValue();
        List<CacheStreamData> data = new ArrayList<>(streams.size());
        for (CacheOutputReceiver receiver : streams) {
          List<MemoryBuffer> buffers = receiver.buffers;
          if (buffers == null) {
            // This can happen e.g. for a data stream when all the values are null.
            LlapIoImpl.LOG.debug("Buffers are null for " + receiver.name);
          }
          data.add(new CacheStreamData(receiver.suppressed, receiver.name,
              buffers == null ? new ArrayList<MemoryBuffer>() : new ArrayList<>(buffers)));
          receiver.clear();
        }
        if (doesSourceHaveIncludes) {
          int newColIx = getSparseOrcIndexFromDenseDest(colIx);
          if (LlapIoImpl.LOG.isTraceEnabled()) {
            LlapIoImpl.LOG.trace("Mapping the ORC writer column " + colIx + " to " + newColIx);
          }
          colIx = newColIx;
        }
        currentStripe.colStreams.put(colIx, data);
      }
      startStripe();
    }

    private int getSparseOrcIndexFromDenseDest(int denseColIx) {
      // denseColIx is index in ORC writer with includes. We -1 to skip the root column; get the
      // original text file index; then add the root column again. This makes many assumptions.
      // Also this only works for primitive types; vectordeserializer only supports these anyway.
      // The mapping for complex types with sub-cols in ORC would be much more difficult to build.
      return columnIds.get(denseColIx - 1) + 1;
    }

    private boolean isNeeded(StreamName name) {
      return doesSourceHaveIncludes || writerIncludes[name.getColumn()];
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void appendRawStripe(ByteBuffer stripe,
                                OrcProto.StripeInformation.Builder dirEntry
                                ) throws IOException {
      throw new UnsupportedOperationException(); // Only used in ACID writer.
    }

    public void setCurrentStripeOffsets(long currentKnownTornStart,
        long firstStartOffset, long lastStartOffset, long currentFileOffset) {
      currentStripe.knownTornStart = currentKnownTornStart;
      currentStripe.firstRowStart = firstStartOffset;
      currentStripe.lastRowStart = lastStartOffset;
      currentStripe.lastRowEnd = currentFileOffset;
    }
  }

  private interface CacheOutput {
    List<MemoryBuffer> getData();
    StreamName getName();
  }

  private static final class CacheOutputReceiver implements CacheOutput, OutputReceiver {
    private final BufferUsageManager bufferManager;
    private final StreamName name;
    private List<MemoryBuffer> buffers = null;
    private int lastBufferPos = -1;
    private boolean suppressed = false;

    public CacheOutputReceiver(BufferUsageManager bufferManager, StreamName name) {
      this.bufferManager = bufferManager;
      this.name = name;
    }

    public void clear() {
      buffers = null;
      lastBufferPos = -1;
      suppressed = false;
    }

    @Override
    public void suppress() {
      suppressed = true;
      buffers = null;
      lastBufferPos = -1;
    }

    @Override
    public void output(ByteBuffer buffer) throws IOException {
      // TODO: avoid put() by working directly in OutStream?
      if (LlapIoImpl.LOG.isTraceEnabled()) {
        LlapIoImpl.LOG.trace(name + " receiving a buffer of size " + buffer.remaining());
      }
      int size = buffer.remaining();
      ByteBuffer bb = null;
      if (buffers == null) {
        buffers = new ArrayList<>();
      }
      if (!buffers.isEmpty()) {
        MemoryBuffer lastBuffer = buffers.get(buffers.size() - 1);
        bb = lastBuffer.getByteBufferRaw();
        int written = lastBufferPos - bb.position();
        if (bb.remaining() - written < size) {
          lastBufferPos = -1;
          bb = null;
        }
      }
      boolean isNewBuffer = (lastBufferPos == -1);
      if (isNewBuffer) {
        MemoryBuffer[] dest = new MemoryBuffer[1];
        bufferManager.getAllocator().allocateMultiple(dest, size);
        LlapDataBuffer newBuffer = (LlapDataBuffer)dest[0];
        bb = newBuffer.getByteBufferRaw();
        lastBufferPos = bb.position();
        buffers.add(newBuffer);
      }
      // Since there's no close() here, maintain the initial read position between writes.
      int pos = bb.position();
      bb.position(lastBufferPos);
      bb.put(buffer);
      lastBufferPos = bb.position();
      bb.position(pos);
    }

    @Override
    public List<MemoryBuffer> getData() {
      return buffers;
    }

    @Override
    public StreamName getName() {
      return name;
    }
  }

  private static class NullOutputReceiver implements OutputReceiver {
    private final StreamName name;

    public NullOutputReceiver(StreamName name) {
      this.name = name;
    }

    @Override
    public void output(ByteBuffer buffer) throws IOException {
    }

    @Override
    public void suppress() {
    }
  }

  protected Void performDataRead() throws IOException {
    try {
      try {
        long startTime = counters.startTimeCounter();
        LlapIoImpl.LOG.info("Processing data for {}", split.getPath());
        if (processStop()) {
          recordReaderTime(startTime);
          return null;
        }
        Boolean isFromCache = null;
        try {
          isFromCache = readFileWithCache(startTime);
        } finally {
          if (cachedData != null && cachedData.getData() != null) {
            for (StripeData sd : cachedData.getData()) {
              unlockAllBuffers(sd);
            }
          }
        }
        if (isFromCache == null) return null; // Stop requested, and handled inside.
        if (!isFromCache) {
          if (!processOneFileSplit(split, startTime, Ref.from(0), null)) return null;
        }

        // Done with all the things.
        recordReaderTime(startTime);
      } catch (Throwable e) {
        LlapIoImpl.LOG.error("Exception while processing", e);
        consumer.setError(e);
        throw e;
      }
      consumer.setDone();

      LlapIoImpl.LOG.trace("done processing {}", split);
      return null;
    } finally {
      cleanupReaders();
    }
  }

  private void unlockAllBuffers(StripeData si) {
    for (int i = 0; i < si.getData().length; ++i) {
      LlapDataBuffer[][] colData = si.getData()[i];
      if (colData == null) continue;
      for (int j = 0; j < colData.length; ++j) {
        LlapDataBuffer[] streamData = colData[j];
        if (streamData == null) continue;
        for (int k = 0; k < streamData.length; ++k) {
          bufferManager.decRefBuffer(streamData[k]);
        }
      }
    }
  }

  public void cacheFileData(StripeData sd) {
    if (sd == null || sd.getEncodings() == null) return;
    if (fileKey != null) {
      // Note that we cache each slice separately. We could cache them together at the end, but
      // then we won't be able to pass them to users without inc-refing explicitly.
      ColumnEncoding[] encodings = sd.getEncodings();
      for (int i = 0; i < encodings.length; ++i) {
        // Make data consistent with encodings, don't store useless information.
        if (sd.getData()[i] == null) {
          encodings[i] = null;
        }
      }
      FileData fd = new FileData(fileKey, encodings.length);
      fd.addStripe(sd);
      cache.putFileData(fd, Priority.NORMAL, counters);
    } else {
      lockAllBuffers(sd);
    }
    // We assume that if put/lock throws in the middle, it's ok to treat buffers as not being
    // locked and to blindly deallocate them, since they are not going to be used. Therefore
    // we don't remove them from the cleanup list - we will do it after sending to consumer.
    // This relies on sequence of calls to cacheFileData and sendEcb..
  }


  private void lockAllBuffers(StripeData sd) {
    for (int i = 0; i < sd.getData().length; ++i) {
      LlapDataBuffer[][] colData = sd.getData()[i];
      if (colData == null) continue;
      for (int j = 0; j < colData.length; ++j) {
        LlapDataBuffer[] streamData = colData[j];
        if (streamData == null) continue;
        for (int k = 0; k < streamData.length; ++k) {
          boolean canLock = bufferManager.incRefBuffer(streamData[k]);
          assert canLock;
        }
      }
    }
  }

  public Boolean readFileWithCache(long startTime) throws IOException {
    if (fileKey == null) return false;
    BooleanRef gotAllData = new BooleanRef();
    long endOfSplit = split.getStart() + split.getLength();
    this.cachedData = cache.getFileData(fileKey, split.getStart(),
        endOfSplit, writerIncludes, CC_FACTORY, counters, gotAllData);
    if (cachedData == null) {
      LlapIoImpl.CACHE_LOGGER.trace("No data for the split found in cache");
      return false;
    }
    String[] hosts = extractHosts(split, false), inMemoryHosts = extractHosts(split, true);
    List<StripeData> slices = cachedData.getData();
    if (slices.isEmpty()) return false;
    long uncachedPrefixEnd = slices.get(0).getKnownTornStart(),
        uncachedSuffixStart = slices.get(slices.size() - 1).getLastEnd();
    Ref<Integer> stripeIx = Ref.from(0);
    if (uncachedPrefixEnd > split.getStart()) {
      // TODO: can we merge neighboring splits? So we don't init so many readers.
      FileSplit sliceSplit = new FileSplit(split.getPath(), split.getStart(),
          uncachedPrefixEnd - split.getStart(), hosts, inMemoryHosts);
      if (!processOneFileSplit(sliceSplit, startTime, stripeIx, null)) return null;
    }
    while (!slices.isEmpty()) {
      StripeData slice = slices.get(0);
      long start = slice.getKnownTornStart();
      long len = slice.getLastStart() - start; // Will also read the last row.
      FileSplit sliceSplit = new FileSplit(split.getPath(), start, len, hosts, inMemoryHosts);
      if (!processOneFileSplit(sliceSplit, startTime, stripeIx, slice)) return null;
    }
    boolean isUnfortunate = false;
    if (uncachedSuffixStart == endOfSplit) {
      // This is rather obscure. The end of last row cached is precisely at the split end offset.
      // If the split is in the middle of the file, LRR would read one more row after that,
      // therefore as unfortunate as it is, we have to do a one-row read. However, for that to
      // have happened, someone should have supplied a split that ends inside the last row, i.e.
      // a few bytes earlier than the current split, which is pretty unlikely. What is more likely
      // is that the split, and the last row, both end at the end of file. Check for this.
      long size =  split.getPath().getFileSystem(
          daemonConf).getFileStatus(split.getPath()).getLen();
      isUnfortunate = size > endOfSplit;
      if (isUnfortunate) {
        // Log at warn, given how unfortunate this is.
        LlapIoImpl.LOG.warn("One-row mismatch at the end of split " + split.getPath()
            + " at " + endOfSplit + "; file size is " + size);
      }
    }

    if (uncachedSuffixStart < endOfSplit || isUnfortunate) {
      // TODO: will 0-length split work? should we assume 1+ chars and add 1 for isUnfortunate?
      FileSplit splitPart = new FileSplit(split.getPath(), uncachedSuffixStart,
          endOfSplit - uncachedSuffixStart, hosts, inMemoryHosts);
      if (!processOneFileSplit(splitPart, startTime, stripeIx, null)) return null;
    }
    return true;
  }

  public boolean processOneFileSplit(FileSplit split, long startTime,
      Ref<Integer> stripeIxRef, StripeData slice) throws IOException {
    ColumnEncoding[] cacheEncodings = slice == null ? null : slice.getEncodings();
    LlapIoImpl.LOG.info("Processing one split {" + split.getPath() + ", "
        + split.getStart() + ", " + split.getLength() + "}");
    if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
      LlapIoImpl.CACHE_LOGGER.trace("Cache data for the split is " + slice);
    }
    boolean[] splitIncludes = writerIncludes;
    boolean hasAllData = false;
    if (cacheEncodings != null) {
      hasAllData = true;
      splitIncludes = Arrays.copyOf(writerIncludes, writerIncludes.length);
      for (int colIx = 0; colIx < cacheEncodings.length; ++colIx) {
        if (!splitIncludes[colIx]) continue;
        assert (cacheEncodings[colIx] != null) == (slice.getData()[colIx] != null);
        if (cacheEncodings[colIx] != null) {
          splitIncludes[colIx] = false;
        } else {
          hasAllData = false;
        }
      }
    }
    if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
      LlapIoImpl.LOG.trace("Includes accounting for cached data: before " + DebugUtils.toString(
        writerIncludes) + ", after " + DebugUtils.toString(splitIncludes));
    }
    // We have 3 cases here:
    // 1) All the data is in the cache. Always a single slice, no disk read, no cache puts.
    // 2) Some data is in the cache. Always a single slice, disk read and a single cache put.
    // 3) No data is in the cache. Multiple slices, disk read and multiple cache puts.
    if (!hasAllData) {
      // This initializes cacheWriter with data.
      readSplitFromFile(split, splitIncludes, slice);
      assert cacheWriter != null;
    }
    if (slice != null) {
      // If we had a cache range already, it should not have been split.
      assert cacheWriter == null || cacheWriter.stripes.size() == 1;
      CacheWriter.CacheStripeData csd = hasAllData ? null : cacheWriter.stripes.get(0);
      boolean result = processOneSlice(csd, splitIncludes, stripeIxRef.value, slice, startTime);
      ++stripeIxRef.value;
      return result;
    } else {
      for (CacheWriter.CacheStripeData csd : cacheWriter.stripes) {
        if (!processOneSlice(csd, splitIncludes, stripeIxRef.value, null, startTime)) {
          return false;
        }
        ++stripeIxRef.value;
      }
      return true;
    }
  }

  private boolean processOneSlice(CacheWriter.CacheStripeData csd, boolean[] splitIncludes,
      int stripeIx, StripeData slice, long startTime) throws IOException {
    String sliceStr = slice == null ? "null" : slice.toCoordinateString();
    if (LlapIoImpl.LOG.isDebugEnabled()) {
      LlapIoImpl.LOG.debug("Processing slice #" + stripeIx + " " + sliceStr + "; has"
        + ((slice == null) ? " no" : "") + " cache data; has" + ((csd == null) ? " no" : "")
        + " disk data");
    }

    ColumnEncoding[] cacheEncodings = slice == null ? null : slice.getEncodings();
    LlapDataBuffer[][][] cacheData = slice == null ? null : slice.getData();
    long cacheRowCount = slice == null ? -1L : slice.getRowCount();
    SerDeStripeMetadata metadata = new SerDeStripeMetadata(stripeIx);
    StripeData sliceToCache = null;
    boolean hasAllData = csd == null;
    if (!hasAllData) {
      if (slice == null) {
        sliceToCache = new StripeData(
            csd.knownTornStart, csd.firstRowStart, csd.lastRowStart, csd.lastRowEnd,
            csd.rowCount, csd.encodings.toArray(new ColumnEncoding[csd.encodings.size()]));
      } else {
        if (csd.rowCount != slice.getRowCount()) {
          throw new IOException("Row count mismatch; disk " + csd.rowCount + ", cache "
              + slice.getRowCount() + " from " + csd + " and " + slice);
        }
        if (csd.encodings.size() != slice.getEncodings().length) {
          throw new IOException("Column count mismatch; disk " + csd.encodings.size()
              + ", cache " + slice.getEncodings().length + " from " + csd + " and " + slice);
        }
        if (LlapIoImpl.LOG.isDebugEnabled()) {
          LlapIoImpl.LOG.debug("Creating slice to cache in addition to an existing slice "
            + slice.toCoordinateString() + "; disk offsets were " + csd.toCoordinateString());
        }
        sliceToCache = StripeData.duplicateForResults(slice);
        for (int i = 0; i < csd.encodings.size(); ++i) {
          sliceToCache.getEncodings()[i] = csd.encodings.get(i);
        }
        sliceToCache.setKnownTornStart(Math.min(csd.knownTornStart, slice.getKnownTornStart()));
      }
      metadata.setEncodings(combineCacheAndWriterEncodings(cacheEncodings, csd.encodings));
      metadata.setRowCount(csd.rowCount);
    } else {
      assert cacheWriter == null;
      metadata.setEncodings(Lists.newArrayList(cacheEncodings));
      metadata.setRowCount(cacheRowCount);
    }
    if (LlapIoImpl.LOG.isTraceEnabled()) {
      LlapIoImpl.LOG.trace("Derived stripe metadata for this split is " + metadata);
    }
    consumer.setStripeMetadata(metadata);

    OrcEncodedColumnBatch ecb = ECB_POOL.take();
    ecb.init(fileKey, metadata.getStripeIx(), OrcEncodedColumnBatch.ALL_RGS, writerIncludes.length);
    for (int colIx = 0; colIx < writerIncludes.length; ++colIx) {
      if (!writerIncludes[colIx]) continue;
      ecb.initColumn(colIx, OrcEncodedColumnBatch.MAX_DATA_STREAMS);
      if (!hasAllData && splitIncludes[colIx]) {
        // The column has been read from disk.
        List<CacheWriter.CacheStreamData> streams = csd.colStreams.get(colIx);
        if (LlapIoImpl.LOG.isTraceEnabled()) {
          LlapIoImpl.LOG.trace("Processing streams for column " + colIx + ": " + streams);
        }
        LlapDataBuffer[][] newCacheDataForCol = sliceToCache.getData()[colIx]
            = new LlapDataBuffer[OrcEncodedColumnBatch.MAX_DATA_STREAMS][];
        if (streams == null) continue; // Struct column, such as root?
        Iterator<CacheWriter.CacheStreamData> iter = streams.iterator();
        while (iter.hasNext()) {
          CacheWriter.CacheStreamData stream = iter.next();
          if (stream.isSuppressed) {
            LlapIoImpl.LOG.trace("Removing a suppressed stream " + stream.name);
            iter.remove();
            discardUncachedBuffers(stream.data);
            continue;
          }
          // TODO: We write each slice using a separate writer, so we don't share dictionaries. Fix?
          ColumnStreamData cb = CSD_POOL.take();
          cb.incRef();
          int streamIx = stream.name.getKind().getNumber();
          cb.setCacheBuffers(stream.data);
          // This is kinda hacky - we "know" these are LlapDataBuffer-s.
          newCacheDataForCol[streamIx] = stream.data.toArray(
              new LlapDataBuffer[stream.data.size()]);
          ecb.setStreamData(colIx, streamIx, cb);
        }
      } else {
        // The column has been obtained from cache.
        LlapDataBuffer[][] colData = cacheData[colIx];
        if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
          LlapIoImpl.CACHE_LOGGER.trace("Processing cache data for column " + colIx + ": "
              + SerDeLowLevelCacheImpl.toString(colData));
        }
        for (int streamIx = 0; streamIx < colData.length; ++streamIx) {
          if (colData[streamIx] == null) continue;
          ColumnStreamData cb = CSD_POOL.take();
          cb.incRef();
          cb.setCacheBuffers(Lists.<MemoryBuffer>newArrayList(colData[streamIx]));
          ecb.setStreamData(colIx, streamIx, cb);
        }
      }
    }
    if (processStop()) {
      recordReaderTime(startTime);
      return false;
    }
    // Note: we cache slices one by one since we need to lock them before sending to consumer.
    //       We could lock here, then cache them together, then unlock here and in return,
    //       but for now just rely on the cache put to lock them before we send them over.
    if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
      LlapIoImpl.CACHE_LOGGER.trace("Data to cache from the read " + sliceToCache);
    }
    cacheFileData(sliceToCache);
    return sendEcbToConsumer(ecb, slice != null, csd);
  }

  private void discardUncachedBuffers(List<MemoryBuffer> list) {
    for (MemoryBuffer buffer : list) {
      boolean isInvalidated = ((LlapDataBuffer)buffer).invalidate();
      assert isInvalidated;
      bufferManager.getAllocator().deallocate(buffer);
    }
  }

  private static List<ColumnEncoding> combineCacheAndWriterEncodings(
      ColumnEncoding[] cacheEncodings, List<ColumnEncoding> writerEncodings) throws IOException {
    // TODO: refactor with cache impl? it has the same merge logic
    if (cacheEncodings == null) {
      return new ArrayList<>(writerEncodings);
    }
    if (cacheEncodings.length != writerEncodings.size()) {
      throw new IOException("Incompatible encoding lengths: "
          + Arrays.toString(cacheEncodings) + " vs " + writerEncodings);
    }
    ColumnEncoding[] combinedEncodings = Arrays.copyOf(cacheEncodings, cacheEncodings.length);
    for (int colIx = 0; colIx < cacheEncodings.length; ++colIx) {
      ColumnEncoding newEncoding = writerEncodings.get(colIx);
      if (newEncoding == null) continue;
      if (combinedEncodings[colIx] != null && !newEncoding.equals(combinedEncodings[colIx])) {
        throw new IOException("Incompatible encodings at " + colIx + ": "
            + Arrays.toString(cacheEncodings) + " vs " + writerEncodings);
      }
      combinedEncodings[colIx] = newEncoding;
    }
    return Lists.newArrayList(combinedEncodings);
  }


  public void readSplitFromFile(FileSplit split, boolean[] splitIncludes, StripeData slice)
      throws IOException {
    boolean maySplitTheSplit = slice == null;
    ReaderWithOffsets offsetReader = null;
    @SuppressWarnings("rawtypes")
    RecordReader sourceReader = sourceInputFormat.getRecordReader(split, jobConf, reporter);
    try {
      offsetReader = createOffsetReader(sourceReader);
      maySplitTheSplit = maySplitTheSplit && offsetReader.hasOffsets();

      // writer writes to orcWriter which writes to cacheWriter
      // TODO: in due course, writer will also propagate row batches if it's capable
      StructObjectInspector originalOi = (StructObjectInspector)getOiFromSerDe();
      writer = VertorDeserializeOrcWriter.create(sourceInputFormat, sourceSerDe, parts,
          daemonConf, jobConf, split.getPath(), originalOi, columnIds);
      cacheWriter = new CacheWriter(
          bufferManager, allocSize, columnIds, splitIncludes, writer.hasIncludes());
      writer.init(OrcFile.createWriter(split.getPath(),
          createOrcWriterOptions(writer.getDestinationOi())));

      int rowsPerSlice = 0;
      long currentKnownTornStart = split.getStart();
      long lastStartOffset = Long.MIN_VALUE, firstStartOffset = Long.MIN_VALUE;
      boolean hasData = false;
      while (offsetReader.next()) {
        hasData = true;
        Writable value = offsetReader.getCurrentRow();
        lastStartOffset = offsetReader.getCurrentRowStartOffset();
        if (firstStartOffset == Long.MIN_VALUE) {
          firstStartOffset = lastStartOffset;
        }
        writer.writeOneRow(value);

        if (maySplitTheSplit && ++rowsPerSlice == targetSliceRowCount) {
          assert offsetReader.hasOffsets();
          writer.flushIntermediateData();
          long fileOffset = offsetReader.getCurrentRowEndOffset();
          // Must support offsets to be able to split.
          if (firstStartOffset < 0 || lastStartOffset < 0 || fileOffset < 0) {
            throw new AssertionError("Unable to get offsets from "
                + offsetReader.getClass().getSimpleName());
          }
          cacheWriter.setCurrentStripeOffsets(
              currentKnownTornStart, firstStartOffset, lastStartOffset, fileOffset);
          // Split starting at row start will not read that row.
          currentKnownTornStart = lastStartOffset;
          // Row offsets will be determined from the reader (we could set the first from last).
          lastStartOffset = Long.MIN_VALUE;
          firstStartOffset = Long.MIN_VALUE;
          rowsPerSlice = 0;
          writer.writeIntermediateFooter();
        }
      }
      if (rowsPerSlice > 0 || (!maySplitTheSplit && hasData)) {
        long fileOffset = -1;
        if (!offsetReader.hasOffsets()) {
          // The reader doesn't support offsets. We adjust offsets to match future splits.
          // If cached split was starting at row start, that row would be skipped, so +1
          firstStartOffset = split.getStart() + 1;
          // Last row starting at the end of the split would be read.
          lastStartOffset = split.getStart() + split.getLength();
          // However, it must end after the split end, otherwise the next one would have been read.
          fileOffset = lastStartOffset + 1;
          if (LlapIoImpl.CACHE_LOGGER.isDebugEnabled()) {
            LlapIoImpl.CACHE_LOGGER.debug("Cache offsets based on the split - 'first row' at "
              + firstStartOffset + "; 'last row' at " + lastStartOffset + ", " + fileOffset);
          }
        } else {
          fileOffset = offsetReader.getCurrentRowEndOffset();
          assert firstStartOffset >= 0 && lastStartOffset >= 0 && fileOffset >= 0;
        }
        cacheWriter.setCurrentStripeOffsets(
            currentKnownTornStart, firstStartOffset, lastStartOffset, fileOffset);
      }
      // Close the writer to finalize the metadata. No catch since we cannot go on if this throws.
      writer.close();
      writer = null;
    } finally {
      // We don't need the source reader anymore.
      if (offsetReader != null) {
        try {
          offsetReader.close();
        } catch (Exception ex) {
          LlapIoImpl.LOG.error("Failed to close source reader", ex);
        }
      } else {
        assert sourceReader != null;
        try {
          sourceReader.close();
        } catch (Exception ex) {
          LlapIoImpl.LOG.error("Failed to close source reader", ex);
        }
      }
    }
  }

  interface EncodingWriter {
    void writeOneRow(Writable row) throws IOException;
    StructObjectInspector getDestinationOi();
    void init(Writer orcWriter);
    boolean hasIncludes();
    void writeIntermediateFooter() throws IOException;
    void flushIntermediateData() throws IOException;
    void close() throws IOException;
  }

  static class DeserialerOrcWriter implements EncodingWriter {
    private Writer orcWriter;
    private final Deserializer sourceSerDe;
    private final StructObjectInspector sourceOi;

    public DeserialerOrcWriter(Deserializer sourceSerDe, StructObjectInspector sourceOi) {
      this.sourceSerDe = sourceSerDe;
      this.sourceOi = sourceOi;
    }

    @Override
    public void init(Writer orcWriter) {
      this.orcWriter = orcWriter;
    }

    @Override
    public void close() throws IOException {
      orcWriter.close();
    }

    @Override
    public void writeOneRow(Writable value) throws IOException {
      Object row = null;
      try {
        row = sourceSerDe.deserialize(value);
      } catch (SerDeException e) {
        throw new IOException(e);
      }
      orcWriter.addRow(row);
    }

    @Override
    public void flushIntermediateData() {
      // No-op.
    }

    @Override
    public void writeIntermediateFooter() throws IOException {
      orcWriter.writeIntermediateFooter();
    }

    @Override
    public boolean hasIncludes() {
      return false; // LazySimpleSerDe doesn't support projection.
    }

    @Override
    public StructObjectInspector getDestinationOi() {
      return sourceOi;
    }
  }

  private WriterOptions createOrcWriterOptions(ObjectInspector sourceOi) throws IOException {
    return OrcFile.writerOptions(daemonConf).stripeSize(Long.MAX_VALUE).blockSize(Long.MAX_VALUE)
        .rowIndexStride(Integer.MAX_VALUE) // For now, do not limit this - one RG per split
        .blockPadding(false).compress(CompressionKind.NONE).version(Version.CURRENT)
        .encodingStrategy(EncodingStrategy.SPEED).bloomFilterColumns(null).inspector(sourceOi)
        .physicalWriter(cacheWriter);
  }

  private ObjectInspector getOiFromSerDe() throws IOException {
    try {
      return sourceSerDe.getObjectInspector();
    } catch (SerDeException e) {
      throw new IOException(e);
    }
  }

  private ReaderWithOffsets createOffsetReader(RecordReader sourceReader) {
    if (LlapIoImpl.LOG.isDebugEnabled()) {
      LlapIoImpl.LOG.debug("Using " + sourceReader.getClass().getSimpleName() + " to read data");
    }
    // Handle the special cases here. Perhaps we could have a more general structure, or even
    // a configurable set (like storage handlers), but for now we only have one.
    if (isLrrEnabled && sourceReader instanceof LineRecordReader) {
      return LineRrOffsetReader.create((LineRecordReader)sourceReader);
    }
    return new PassThruOffsetReader(sourceReader);
  }

  private static String[] extractHosts(FileSplit split, boolean isInMemory) throws IOException {
    SplitLocationInfo[] locInfo = split.getLocationInfo();
    if (locInfo == null) return new String[0];
    List<String> hosts = null; // TODO: most of the time, there's no in-memory. Use an array?
    for (int i = 0; i < locInfo.length; i++) {
      if (locInfo[i].isInMemory() != isInMemory) continue;
      if (hosts == null) {
        hosts = new ArrayList<>();
      }
      hosts.add(locInfo[i].getLocation());
    }
    if (hosts == null) return new String[0];
    return hosts.toArray(new String[hosts.size()]);
  }

  private boolean sendEcbToConsumer(OrcEncodedColumnBatch ecb,
      boolean hasCachedSlice, CacheWriter.CacheStripeData writerData) {
    if (ecb == null) { // This basically means stop has been called.
      cleanupReaders();
      return false;
    }
    LlapIoImpl.LOG.trace("Sending a batch over to consumer");
    consumer.consumeData(ecb);
    if (hasCachedSlice) {
      cachedData.getData().remove(0); // See javadoc - no need to clean up the cache data anymore.
    }
    if (writerData != null) {
      writerData.colStreams.clear();
    }
    return true;
  }


  private void cleanupReaders() {
    if (writer != null) {
      try {
        writer.close();
        writer = null;
      } catch (Exception ex) {
        LlapIoImpl.LOG.error("Failed to close ORC writer", ex);
      }
    }
    if (cacheWriter != null) {
      try {
        cacheWriter.discardData();
        cacheWriter = null;
      } catch (Exception ex) {
        LlapIoImpl.LOG.error("Failed to close cache writer", ex);
      }
    }
  }

  private void recordReaderTime(long startTime) {
    counters.incrTimeCounter(LlapIOCounters.TOTAL_IO_TIME_NS, startTime);
  }

  private boolean processStop() {
    if (!isStopped) return false;
    LlapIoImpl.LOG.info("SerDe-based data reader is stopping");
    cleanupReaders();
    return true;
  }

  private static Object determineFileId(FileSystem fs, FileSplit split,
      boolean allowSynthetic) throws IOException {
    /* TODO: support this optionally? this is not OrcSplit, but we could add a custom split.
      Object fileKey = ((OrcSplit)split).getFileKey();
      if (fileKey != null) return fileKey; */
    LlapIoImpl.LOG.warn("Split for " + split.getPath() + " (" + split.getClass() + ") does not have file ID");
    return HdfsUtils.getFileId(fs, split.getPath(), allowSynthetic);
  }

  // TODO: move to a superclass?
  @Override
  public void returnData(OrcEncodedColumnBatch ecb) {
    for (int colIx = 0; colIx < ecb.getTotalColCount(); ++colIx) {
      if (!ecb.hasData(colIx)) continue;
      ColumnStreamData[] datas = ecb.getColumnData(colIx);
      for (ColumnStreamData data : datas) {
        if (data == null || data.decRef() != 0) continue;
        if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
          for (MemoryBuffer buf : data.getCacheBuffers()) {
            LlapIoImpl.LOCKING_LOGGER.trace("Unlocking {} at the end of processing", buf);
          }
        }
        bufferManager.decRefBuffers(data.getCacheBuffers());
        CSD_POOL.offer(data);
      }
    }
    // We can offer ECB even with some streams not discarded; reset() will clear the arrays.
    ECB_POOL.offer(ecb);
  }

  public TezCounters getTezCounters() {
    return counters.getTezCounters();
  }
}
