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

import org.apache.orc.impl.MemoryManager;

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
import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.Allocator.BufferObjectFactory;
import org.apache.hadoop.hive.common.io.DataCache.BooleanRef;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.DataCache.DiskRangeListFactory;
import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch.ColumnStreamData;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.ConsumerFeedback;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.cache.BufferUsageManager;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.cache.SerDeLowLevelCacheImpl;
import org.apache.hadoop.hive.llap.cache.SerDeLowLevelCacheImpl.FileData;
import org.apache.hadoop.hive.llap.cache.SerDeLowLevelCacheImpl.LlapSerDeDataBuffer;
import org.apache.hadoop.hive.llap.cache.SerDeLowLevelCacheImpl.StripeData;
import org.apache.hadoop.hive.llap.counters.LlapIOCounters;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.decode.GenericColumnVectorProducer.SerDeStripeMetadata;
import org.apache.hadoop.hive.llap.io.decode.OrcEncodedDataConsumer;
import org.apache.hadoop.hive.llap.io.encoded.VectorDeserializeOrcWriter.AsyncCallback;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
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
import org.apache.orc.PhysicalWriter;
import org.apache.orc.PhysicalWriter.OutputReceiver;
import org.apache.orc.impl.SchemaEvolution;
import org.apache.orc.impl.StreamName;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.counters.TezCounters;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class SerDeEncodedDataReader extends CallableWithNdc<Void>
    implements ConsumerFeedback<OrcEncodedColumnBatch>, TezCounterSource {

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
  private final static DiskRangeListFactory CC_FACTORY = new DiskRangeListFactory() {
    @Override
    public DiskRangeList createCacheChunk(MemoryBuffer buffer, long offset, long end) {
      return new CacheChunk(buffer, offset, end);
    }
  };

  private final SerDeLowLevelCacheImpl cache;
  private final BufferUsageManager bufferManager;
  private final BufferObjectFactory bufferFactory;
  private final Configuration daemonConf;
  private final FileSplit split;
  private List<Integer> columnIds;
  private final OrcEncodedDataConsumer consumer;
  private final QueryFragmentCounters counters;
  private final UserGroupInformation ugi;
  private final Map<Path, PartitionDesc> parts;

  private final Object fileKey;
  private final String cacheTag;
  private final FileSystem fs;

  private volatile boolean isStopped = false;
  private final Deserializer sourceSerDe;
  private final InputFormat<?, ?> sourceInputFormat;
  private final Reporter reporter;
  private final JobConf jobConf;
  private final TypeDescription schema;
  private final int allocSize;
  private final int targetSliceRowCount;
  private final boolean isLrrEnabled;
  private final boolean useObjectPools;

  private final boolean[] writerIncludes;
  private FileReaderYieldReturn currentFileRead = null;

  /**
   * Data from cache currently being processed. We store it here so that we could decref
   * it in case of failures. We remove each slice from the data after it has been sent to
   * the consumer, at which point the consumer is responsible for it.
   */
  private FileData cachedData;
  private List<VectorDeserializeOrcWriter> asyncWriters = new ArrayList<>();

  public SerDeEncodedDataReader(SerDeLowLevelCacheImpl cache,
      BufferUsageManager bufferManager, Configuration daemonConf, FileSplit split,
      List<Integer> columnIds, OrcEncodedDataConsumer consumer, JobConf jobConf, Reporter reporter,
      InputFormat<?, ?> sourceInputFormat, Deserializer sourceSerDe,
      QueryFragmentCounters counters, TypeDescription schema, Map<Path, PartitionDesc> parts)
          throws IOException {
    assert cache != null;
    this.cache = cache;
    this.bufferManager = bufferManager;
    this.bufferFactory = new BufferObjectFactory() {
      @Override
      public MemoryBuffer create() {
        return new SerDeLowLevelCacheImpl.LlapSerDeDataBuffer();
      }
    };
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
    this.useObjectPools = HiveConf.getBoolVar(sliceConf, ConfVars.LLAP_IO_SHARE_OBJECT_POOLS);
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
        HiveConf.getBoolVar(daemonConf, ConfVars.LLAP_CACHE_ALLOW_SYNTHETIC_FILEID),
        HiveConf.getBoolVar(daemonConf, ConfVars.LLAP_CACHE_DEFAULT_FS_FILE_ID));
    cacheTag = HiveConf.getBoolVar(daemonConf, ConfVars.LLAP_TRACK_CACHE_USAGE)
        ? LlapUtil.getDbAndTableNameForMetrics(split.getPath(), true) : null;
    this.sourceInputFormat = sourceInputFormat;
    this.sourceSerDe = sourceSerDe;
    this.reporter = reporter;
    this.jobConf = jobConf;
    this.schema = schema;
    this.writerIncludes = OrcInputFormat.genIncludedColumns(schema, columnIds);
    SchemaEvolution evolution = new SchemaEvolution(schema, null,
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

  // TODO: move to a base class?
  @Override
  protected Void callInternal() throws IOException, InterruptedException {
    return ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        return performDataRead();
      }
    });
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
          s += buffer + ", ";
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
    private final Allocator.BufferObjectFactory bufferFactory;
    /**
     * For !doesSourceHaveIncludes case, stores global column IDs to verify writer columns.
     * For doesSourceHaveIncludes case, stores source column IDs used to map things.
     */
    private final List<Integer> columnIds;
    private final boolean[] writerIncludes;
    // These are global since ORC reuses objects between stripes.
    private final Map<StreamName, OutputReceiver> streams = new HashMap<>();
    private final Map<Integer, List<CacheOutputReceiver>> colStreams = new HashMap<>();
    private final boolean doesSourceHaveIncludes;

    public CacheWriter(BufferUsageManager bufferManager, List<Integer> columnIds,
        boolean[] writerIncludes, boolean doesSourceHaveIncludes,
        Allocator.BufferObjectFactory bufferFactory) {
      this.bufferManager = bufferManager;
      assert writerIncludes != null; // Taken care of on higher level.
      this.writerIncludes = writerIncludes;
      this.doesSourceHaveIncludes = doesSourceHaveIncludes;
      this.columnIds = columnIds;
      this.bufferFactory = bufferFactory;
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
        CacheOutputReceiver cor = new CacheOutputReceiver(bufferManager, bufferFactory, name);
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
    public void writeIndex(StreamName name, OrcProto.RowIndex.Builder index,
        CompressionCodec codec) throws IOException {
      // TODO: right now we treat each slice as a stripe with a single RG and never bother
      //       with indexes. In phase 4, we need to add indexing and filtering.
    }

    @Override
    public void writeBloomFilter(StreamName name, OrcProto.BloomFilterIndex.Builder bloom,
        CompressionCodec codec) throws IOException {
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
    public void appendRawStripe(
        ByteBuffer stripe, OrcProto.StripeInformation.Builder dirEntry) throws IOException {
      throw new UnsupportedOperationException(); // Only used in ACID writer.
    }

    public void setCurrentStripeOffsets(long currentKnownTornStart,
        long firstStartOffset, long lastStartOffset, long currentFileOffset) {
      currentStripe.knownTornStart = currentKnownTornStart;
      currentStripe.firstRowStart = firstStartOffset;
      currentStripe.lastRowStart = lastStartOffset;
      currentStripe.lastRowEnd = currentFileOffset;
    }

    @Override
    public CompressionCodec getCompressionCodec() {
      return null;
    }

    @Override
    public long getFileBytes(int column) {
      long size = 0L;
      List<CacheOutputReceiver> l = this.colStreams.get(column);
      if (l == null) {
        return size;
      }
      for (CacheOutputReceiver c : l) {
        if (c.getData() != null && !c.suppressed && c.getName().getArea() != StreamName.Area.INDEX) {
          for (MemoryBuffer buffer : c.getData()) {
            size += buffer.getByteBufferRaw().limit();
          }
        }
      }
      return size;
    }
  }

  private interface CacheOutput {
    List<MemoryBuffer> getData();
    StreamName getName();
  }

  private static final class CacheOutputReceiver implements CacheOutput, OutputReceiver {
    private final BufferUsageManager bufferManager;
    private final BufferObjectFactory bufferFactory;
    private final StreamName name;
    private List<MemoryBuffer> buffers = null;
    private int lastBufferPos = -1;
    private boolean suppressed = false;

    public CacheOutputReceiver(BufferUsageManager bufferManager,
        BufferObjectFactory bufferFactory, StreamName name) {
      this.bufferManager = bufferManager;
      this.bufferFactory = bufferFactory;
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
        bufferManager.getAllocator().allocateMultiple(dest, size, bufferFactory);
        LlapSerDeDataBuffer newBuffer = (LlapSerDeDataBuffer)dest[0];
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
    @SuppressWarnings("unused")
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

  protected Void performDataRead() throws IOException, InterruptedException {
    boolean isOk = false;
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
          // Note that the code removes the data from the field as it's passed to the consumer,
          // so we expect to have stuff remaining in there only in case of errors.
          if (cachedData != null && cachedData.getData() != null) {
            for (StripeData sd : cachedData.getData()) {
              unlockAllBuffers(sd);
            }
            cachedData = null;
          }
        }
        if (isFromCache == null) return null; // Stop requested, and handled inside.
        if (!isFromCache) {
          if (!processOneFileSplit(split, startTime, Ref.from(0), null)) return null;
        }

        // Done with all the things.
        recordReaderTime(startTime);
        if (LlapIoImpl.LOG.isTraceEnabled()) {
          LlapIoImpl.LOG.trace("done processing {}", split);
        }
      } catch (Throwable e) {
        LlapIoImpl.LOG.error("Exception while processing", e);
        consumer.setError(e);
        throw e;
      }
      consumer.setDone();
      isOk = true;
      return null;
    } finally {
      cleanup(!isOk);
      // Do not clean up the writers - the callback should do it.
    }
  }


  private void unlockAllBuffers(StripeData si) {
    for (int i = 0; i < si.getData().length; ++i) {
      LlapSerDeDataBuffer[][] colData = si.getData()[i];
      if (colData == null) continue;
      for (int j = 0; j < colData.length; ++j) {
        LlapSerDeDataBuffer[] streamData = colData[j];
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
        } else if (encodings[i] == null) {
          throw new AssertionError("Caching data without an encoding at " + i + ": " + sd);
        }
      }
      FileData fd = new FileData(fileKey, encodings.length);
      fd.addStripe(sd);
      cache.putFileData(fd, Priority.NORMAL, counters, cacheTag);
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
      LlapSerDeDataBuffer[][] colData = sd.getData()[i];
      if (colData == null) continue;
      for (int j = 0; j < colData.length; ++j) {
        LlapSerDeDataBuffer[] streamData = colData[j];
        if (streamData == null) continue;
        for (int k = 0; k < streamData.length; ++k) {
          boolean canLock = bufferManager.incRefBuffer(streamData[k]);
          assert canLock;
        }
      }
    }
  }

  public Boolean readFileWithCache(long startTime) throws IOException, InterruptedException {
    if (fileKey == null) return false;
    BooleanRef gotAllData = new BooleanRef();
    long endOfSplit = split.getStart() + split.getLength();
    this.cachedData = cache.getFileData(fileKey, split.getStart(),
        endOfSplit, writerIncludes, CC_FACTORY, counters, gotAllData);
    if (cachedData == null) {
      if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
        LlapIoImpl.CACHE_LOGGER.trace("No data for the split found in cache");
      }
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
      // Note: we assume 0-length split is correct given now LRR interprets offsets (reading an
      // extra row). Should we instead assume 1+ chars and add 1 for isUnfortunate?
      FileSplit splitPart = new FileSplit(split.getPath(), uncachedSuffixStart,
          endOfSplit - uncachedSuffixStart, hosts, inMemoryHosts);
      if (!processOneFileSplit(splitPart, startTime, stripeIx, null)) return null;
    }
    return true;
  }

  public boolean processOneFileSplit(FileSplit split, long startTime,
      Ref<Integer> stripeIxRef, StripeData slice) throws IOException, InterruptedException {
    LlapIoImpl.LOG.info("Processing one split {" + split.getPath() + ", "
        + split.getStart() + ", " + split.getLength() + "}");
    if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
      LlapIoImpl.CACHE_LOGGER.trace("Cache data for the split is " + slice);
    }
    boolean[] splitIncludes = Arrays.copyOf(writerIncludes, writerIncludes.length);
    boolean hasAllData = slice != null
        && determineSplitIncludes(slice, splitIncludes, writerIncludes);

    // We have 3 cases here:
    // 1) All the data is in the cache. Always a single slice, no disk read, no cache puts.
    // 2) Some data is in the cache. Always a single slice, disk read and a single cache put.
    // 3) No data is in the cache. Multiple slices, disk read and multiple cache puts.
    if (hasAllData) {
      // Everything comes from cache.
      CacheWriter.CacheStripeData csd = null;
      boolean result = processOneSlice(csd, splitIncludes, stripeIxRef.value, slice, startTime);
      ++stripeIxRef.value;
      return result;
    }

    boolean result = false;
    // This initializes currentFileRead.
    startReadSplitFromFile(split, splitIncludes, slice);
    try {
      if (slice != null) {
        // If we had a cache range already, we expect a single matching disk slice.
        // Given that there's cached data we expect there to be some disk data.
        Vectors vectors = currentFileRead.readNextSlice();
        assert vectors != null;
        if (!vectors.isSupported()) {
          // Not in VRB mode - the new cache data is ready, we should use it.
          CacheWriter cacheWriter = currentFileRead.getCacheWriter();
          assert cacheWriter.stripes.size() == 1;
          result = processOneSlice(
              cacheWriter.stripes.get(0), splitIncludes, stripeIxRef.value, slice, startTime);
        } else {
          // VRB mode - process the VRBs with cache data; the new cache data is coming later.
          result = processOneSlice(
              vectors, splitIncludes, stripeIxRef.value, slice, startTime);
        }
        assert null == currentFileRead.readNextSlice();
        ++stripeIxRef.value;
      } else {
        // All the data comes from disk. The reader may have split it into multiple slices.
        // It is also possible there's no data in the file.
        Vectors vectors = currentFileRead.readNextSlice();
        if (vectors == null) return true;
        result = true;
        if (!vectors.isSupported()) {
          // Not in VRB mode - the new cache data is (partially) ready, we should use it.
          while (currentFileRead.readNextSlice() != null); // Force the rest of the data thru.
          CacheWriter cacheWriter = currentFileRead.getCacheWriter();
          for (CacheWriter.CacheStripeData csd : cacheWriter.stripes) {
            if (!processOneSlice(csd, splitIncludes, stripeIxRef.value, null, startTime)) {
              result = false;
              break;
            }
            ++stripeIxRef.value;
          }
        } else {
          // VRB mode - process the VRBs with cache data; the new cache data is coming later.
          do {
            assert vectors.isSupported();
            if (!processOneSlice(vectors, splitIncludes, stripeIxRef.value, null, startTime)) {
              result = false;
              break;
            }
            ++stripeIxRef.value;
          } while ((vectors = currentFileRead.readNextSlice()) != null);
        }
      }
    } finally {
      cleanUpCurrentRead();
    }
    return result;
  }

  private static boolean determineSplitIncludes(
      StripeData slice, boolean[] splitIncludes, boolean[] writerIncludes) {
    ColumnEncoding[] cacheEncodings = slice.getEncodings();
    assert cacheEncodings != null;
    boolean hasAllData = true;
    for (int colIx = 0; colIx < cacheEncodings.length; ++colIx) {
      if (!splitIncludes[colIx]) continue;
      if ((cacheEncodings[colIx] != null) != (slice.getData()[colIx] != null)) {
        throw new AssertionError("Inconsistent cache slice " + slice);
      }
      if (cacheEncodings[colIx] != null) {
        splitIncludes[colIx] = false;
      } else {
        hasAllData = false;
      }
    }
    if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
      LlapIoImpl.LOG.trace("Includes accounting for cached data: before " + DebugUtils.toString(
        writerIncludes) + ", after " + DebugUtils.toString(splitIncludes));
    }
    return hasAllData;
  }

  private boolean processOneSlice(CacheWriter.CacheStripeData diskData, boolean[] splitIncludes,
      int stripeIx, StripeData cacheData, long startTime) throws IOException, InterruptedException {
    logProcessOneSlice(stripeIx, diskData, cacheData);

    ColumnEncoding[] cacheEncodings = cacheData == null ? null : cacheData.getEncodings();
    LlapSerDeDataBuffer[][][] cacheBuffers = cacheData == null ? null : cacheData.getData();
    long cacheRowCount = cacheData == null ? -1L : cacheData.getRowCount();
    SerDeStripeMetadata metadata = new SerDeStripeMetadata(stripeIx);
    StripeData sliceToCache = null;
    boolean hasAllData = diskData == null;
    if (!hasAllData) {
      sliceToCache = createSliceToCache(diskData, cacheData);
      metadata.setEncodings(combineCacheAndWriterEncodings(cacheEncodings, diskData.encodings));
      metadata.setRowCount(diskData.rowCount);
    } else {
      metadata.setEncodings(Lists.newArrayList(cacheEncodings));
      metadata.setRowCount(cacheRowCount);
    }
    if (LlapIoImpl.LOG.isTraceEnabled()) {
      LlapIoImpl.LOG.trace("Derived stripe metadata for this split is " + metadata);
    }
    consumer.setStripeMetadata(metadata);

    OrcEncodedColumnBatch ecb = useObjectPools ? ECB_POOL.take() : new OrcEncodedColumnBatch();
    ecb.init(fileKey, metadata.getStripeIx(), OrcEncodedColumnBatch.ALL_RGS, writerIncludes.length);
    // Skip the 0th column that is the root structure.
    for (int colIx = 1; colIx < writerIncludes.length; ++colIx) {
      if (!writerIncludes[colIx]) continue;
      ecb.initColumn(colIx, OrcEncodedColumnBatch.MAX_DATA_STREAMS);
      if (!hasAllData && splitIncludes[colIx]) {
        // The column has been read from disk.
        List<CacheWriter.CacheStreamData> streams = diskData.colStreams.get(colIx);
        LlapSerDeDataBuffer[][] newCacheDataForCol = createArrayToCache(sliceToCache, colIx, streams);
        if (streams == null) continue; // Struct column, such as root?
        Iterator<CacheWriter.CacheStreamData> iter = streams.iterator();
        while (iter.hasNext()) {
          CacheWriter.CacheStreamData stream = iter.next();
          if (stream.isSuppressed) {
            if (LlapIoImpl.LOG.isTraceEnabled()) {
              LlapIoImpl.LOG.trace("Removing a suppressed stream " + stream.name);
            }
            iter.remove();
            discardUncachedBuffers(stream.data);
            continue;
          }
          int streamIx = setStreamDataToCache(newCacheDataForCol, stream);
          ColumnStreamData cb = useObjectPools ? CSD_POOL.take() : new ColumnStreamData();
          cb.incRef();
          cb.setCacheBuffers(stream.data);
          ecb.setStreamData(colIx, streamIx, cb);
        }
      } else {
        processColumnCacheData(cacheBuffers, ecb, colIx);
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
    return sendEcbToConsumer(ecb, cacheData != null, diskData);
  }

  private void validateCacheAndDisk(StripeData cacheData,
      long rowCount, long encodingCount, Object diskDataLog) throws IOException {
    if (rowCount != cacheData.getRowCount()) {
      throw new IOException("Row count mismatch; disk " + rowCount + ", cache "
          + cacheData.getRowCount() + " from " + diskDataLog + " and " + cacheData);
    }
    if (encodingCount > 0 && encodingCount != cacheData.getEncodings().length) {
      throw new IOException("Column count mismatch; disk " + encodingCount + ", cache "
          + cacheData.getEncodings().length + " from " + diskDataLog + " and " + cacheData);
    }
  }


  /** Unlike the other overload of processOneSlice, doesn't cache data. */
  private boolean processOneSlice(Vectors diskData, boolean[] splitIncludes, int stripeIx,
      StripeData cacheData, long startTime) throws IOException, InterruptedException {
    if (diskData == null) {
      throw new AssertionError(); // The other overload should have been used.
    }
    // LlapIoImpl.LOG.debug("diskData " + diskData);
    logProcessOneSlice(stripeIx, diskData, cacheData);

    if (cacheData == null && diskData.getRowCount() == 0) {
      return true; // Nothing to process.
    }
    ColumnEncoding[] cacheEncodings = cacheData == null ? null : cacheData.getEncodings();
    LlapSerDeDataBuffer[][][] cacheBuffers = cacheData == null ? null : cacheData.getData();
    if (cacheData != null) {
      // Don't validate column count - no encodings for vectors.
      validateCacheAndDisk(cacheData, diskData.getRowCount(), -1, diskData);
    }
    SerDeStripeMetadata metadata = new SerDeStripeMetadata(stripeIx);
    metadata.setEncodings(Arrays.asList(cacheEncodings == null
        ? new ColumnEncoding[splitIncludes.length] : cacheEncodings));
    metadata.setRowCount(diskData.getRowCount());
    if (LlapIoImpl.LOG.isTraceEnabled()) {
      LlapIoImpl.LOG.trace("Derived stripe metadata for this split is " + metadata);
    }
    consumer.setStripeMetadata(metadata);

    OrcEncodedColumnBatch ecb = useObjectPools ? ECB_POOL.take() : new OrcEncodedColumnBatch();
    ecb.init(fileKey, metadata.getStripeIx(), OrcEncodedColumnBatch.ALL_RGS, writerIncludes.length);
    int vectorsIx = 0;
    for (int colIx = 0; colIx < writerIncludes.length; ++colIx) {
      // Skip the 0-th column, since it won't have a vector after reading the text source.
      if (colIx == 0) continue;
      if (!writerIncludes[colIx]) continue;
      if (splitIncludes[colIx]) {
        List<ColumnVector> vectors = diskData.getVectors(vectorsIx++);
        if (LlapIoImpl.LOG.isTraceEnabled()) {
          LlapIoImpl.LOG.trace("Processing vectors for column " + colIx + ": " + vectors);
        }
        ecb.initColumnWithVectors(colIx, vectors);
      } else {
        ecb.initColumn(colIx, OrcEncodedColumnBatch.MAX_DATA_STREAMS);
        processColumnCacheData(cacheBuffers, ecb, colIx);
      }
    }
    if (processStop()) {
      recordReaderTime(startTime);
      return false;
    }
    return sendEcbToConsumer(ecb, cacheData != null, null);
  }


  private void processAsyncCacheData(CacheWriter.CacheStripeData diskData,
      boolean[] splitIncludes) throws IOException {
    StripeData sliceToCache = new StripeData(diskData.knownTornStart, diskData.firstRowStart,
        diskData.lastRowStart, diskData.lastRowEnd, diskData.rowCount,
        diskData.encodings.toArray(new ColumnEncoding[diskData.encodings.size()]));
    for (int colIx = 0; colIx < splitIncludes.length; ++colIx) {
      if (!splitIncludes[colIx]) continue;
      // The column has been read from disk.
      List<CacheWriter.CacheStreamData> streams = diskData.colStreams.get(colIx);
      LlapSerDeDataBuffer[][] newCacheDataForCol = createArrayToCache(sliceToCache, colIx, streams);
      if (streams == null) continue; // Struct column, such as root?
      Iterator<CacheWriter.CacheStreamData> iter = streams.iterator();
      while (iter.hasNext()) {
        CacheWriter.CacheStreamData stream = iter.next();
        if (stream.isSuppressed) {
          if (LlapIoImpl.LOG.isTraceEnabled()) {
            LlapIoImpl.LOG.trace("Removing a suppressed stream " + stream.name);
          }
          iter.remove();
          discardUncachedBuffers(stream.data);
          continue;
        }
        setStreamDataToCache(newCacheDataForCol, stream);
      }
    }
    if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
      LlapIoImpl.CACHE_LOGGER.trace("Data to cache from async read " + sliceToCache);
    }
    try {
      cacheFileData(sliceToCache);
    } finally {
      unlockAllBuffers(sliceToCache);
    }
  }

  private StripeData createSliceToCache(
      CacheWriter.CacheStripeData diskData, StripeData cacheData) throws IOException {
    assert diskData != null;
    if (cacheData == null) {
      return new StripeData(diskData.knownTornStart, diskData.firstRowStart,
          diskData.lastRowStart, diskData.lastRowEnd, diskData.rowCount,
          diskData.encodings.toArray(new ColumnEncoding[diskData.encodings.size()]));
    } else {
      long rowCount = diskData.rowCount, encodingCount = diskData.encodings.size();
      validateCacheAndDisk(cacheData, rowCount, encodingCount, diskData);
      if (LlapIoImpl.LOG.isDebugEnabled()) {
        LlapIoImpl.LOG.debug("Creating slice to cache in addition to an existing slice "
          + cacheData.toCoordinateString() + "; disk offsets were "
          + diskData.toCoordinateString());
      }
      // Note: we could just do what we already do above from disk data, except for the validation
      // that is not strictly necessary, and knownTornStart which is an optimization.
      StripeData sliceToCache = StripeData.duplicateStructure(cacheData);
      for (int i = 0; i < diskData.encodings.size(); ++i) {
        sliceToCache.getEncodings()[i] = diskData.encodings.get(i);
      }
      sliceToCache.setKnownTornStart(Math.min(
          diskData.knownTornStart, sliceToCache.getKnownTornStart()));
      return sliceToCache;
    }
  }


  private static LlapSerDeDataBuffer[][] createArrayToCache(
      StripeData sliceToCache, int colIx, List<CacheWriter.CacheStreamData> streams) {
    if (LlapIoImpl.LOG.isTraceEnabled()) {
      LlapIoImpl.LOG.trace("Processing streams for column " + colIx + ": " + streams);
    }
    LlapSerDeDataBuffer[][] newCacheDataForCol = sliceToCache.getData()[colIx]
        = new LlapSerDeDataBuffer[OrcEncodedColumnBatch.MAX_DATA_STREAMS][];
    return newCacheDataForCol;
  }

  private static int setStreamDataToCache(
      LlapSerDeDataBuffer[][] newCacheDataForCol, CacheWriter.CacheStreamData stream) {
    int streamIx = stream.name.getKind().getNumber();
    // This is kinda hacky - we "know" these are LlaSerDeDataBuffer-s.
    newCacheDataForCol[streamIx] = stream.data.toArray(new LlapSerDeDataBuffer[stream.data.size()]);
    return streamIx;
  }

  private void processColumnCacheData(LlapSerDeDataBuffer[][][] cacheBuffers,
      OrcEncodedColumnBatch ecb, int colIx) {
    // The column has been obtained from cache.
    LlapSerDeDataBuffer[][] colData = cacheBuffers[colIx];
    if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
      LlapIoImpl.CACHE_LOGGER.trace("Processing cache data for column " + colIx + ": "
          + SerDeLowLevelCacheImpl.toString(colData));
    }
    for (int streamIx = 0; streamIx < colData.length; ++streamIx) {
      if (colData[streamIx] == null) continue;
      ColumnStreamData cb = useObjectPools ? CSD_POOL.take() : new ColumnStreamData();
      cb.incRef();
      cb.setCacheBuffers(Lists.<MemoryBuffer>newArrayList(colData[streamIx]));
      ecb.setStreamData(colIx, streamIx, cb);
    }
  }

  private void logProcessOneSlice(int stripeIx, Object diskData, StripeData cacheData) {
    String sliceStr = cacheData == null ? "null" : cacheData.toCoordinateString();
    if (LlapIoImpl.LOG.isDebugEnabled()) {
      LlapIoImpl.LOG.debug("Processing slice #" + stripeIx + " " + sliceStr + "; has"
        + ((cacheData == null) ? " no" : "") + " cache data; has"
        + ((diskData == null) ? " no" : "") + " disk data");
    }
  }

  private void discardUncachedBuffers(List<MemoryBuffer> list) {
    for (MemoryBuffer buffer : list) {
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

  private static class Vectors {
    private final List<ColumnVector>[] data;
    private final boolean isSupported;
    private final long rowCount;

    @SuppressWarnings("unchecked")
    public Vectors(List<VectorizedRowBatch> vrbs) {
      if (vrbs == null) {
        isSupported = false;
        data = null;
        rowCount = 0;
        return;
      }
      isSupported = true;
      if (vrbs.isEmpty()) {
        data = null;
        rowCount = 0;
        return;
      }
      data = new List[vrbs.get(0).numCols];
      for (int i = 0; i < data.length; ++i) {
        data[i] = new ArrayList<>(vrbs.size());
      }
      int rowCount = 0;
      for (VectorizedRowBatch vrb : vrbs) {
        assert !vrb.selectedInUse;
        rowCount += vrb.size;
        for (int i = 0; i < vrb.cols.length; ++i) {
          data[i].add(vrb.cols[i]);
        }
      }
      this.rowCount = rowCount;
    }

    public List<ColumnVector> getVectors(int ix) {
      return data[ix];
    }

    public long getRowCount() {
      return rowCount;
    }

    public boolean isSupported() {
      return isSupported;
    }

    @Override
    public String toString() {
      return "Vectors {isSupported=" + isSupported + ", rowCount=" + rowCount
          + ", data=" + Arrays.toString(data) + "}";
    }
  }

  /**
   * This class only exists because Java doesn't have yield return. The original method
   * before this change only needed yield return-s sprinkled here and there; however,
   * Java developers are usually paid by class, so here we go.
   */
  private static class FileReaderYieldReturn {
    private ReaderWithOffsets offsetReader;
    private int rowsPerSlice = 0;
    private long currentKnownTornStart;
    private long lastStartOffset = Long.MIN_VALUE, firstStartOffset = Long.MIN_VALUE;
    private boolean hasAnyData = false;
    private final EncodingWriter writer;
    private final boolean maySplitTheSplit;
    private final int targetSliceRowCount;
    private final FileSplit split;

    public FileReaderYieldReturn(ReaderWithOffsets offsetReader, FileSplit split, EncodingWriter writer,
        boolean maySplitTheSplit, int targetSliceRowCount) {
      Preconditions.checkNotNull(offsetReader);
      this.offsetReader = offsetReader;
      currentKnownTornStart = split.getStart();
      this.writer = writer;
      this.maySplitTheSplit = maySplitTheSplit;
      this.targetSliceRowCount = targetSliceRowCount;
      this.split = split;
    }

    public CacheWriter getCacheWriter() throws IOException {
      return writer.getCacheWriter();
    }

    public Vectors readNextSlice() throws IOException {
      if (offsetReader == null) {
        return null; // This means the reader has already been closed.
      }
      try {
        while (offsetReader.next()) {
          hasAnyData = true;
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
            writer.setCurrentStripeOffsets(
                currentKnownTornStart, firstStartOffset, lastStartOffset, fileOffset);
            writer.writeIntermediateFooter();

            // Split starting at row start will not read that row.
            currentKnownTornStart = lastStartOffset;
            // Row offsets will be determined from the reader (we could set the first from last).
            lastStartOffset = Long.MIN_VALUE;
            firstStartOffset = Long.MIN_VALUE;
            rowsPerSlice = 0;
            return new Vectors(writer.extractCurrentVrbs());
          }
        }
        try {
          if (rowsPerSlice > 0 || (!maySplitTheSplit && hasAnyData)) {
            long fileOffset = -1;
            if (!offsetReader.hasOffsets()) {
              // The reader doesn't support offsets. We adjust offsets to match future splits.
              // If cached split was starting at row start, that row would be skipped, so +1 byte.
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
            writer.setCurrentStripeOffsets(
                currentKnownTornStart, firstStartOffset, lastStartOffset, fileOffset);
            // Close the writer to finalize the metadata.
            writer.close();
            return new Vectors(writer.extractCurrentVrbs());
          } else {
            writer.close();
            return null; // There's no more data.
          }
        } finally {
          closeOffsetReader();
        }
      } catch (Exception ex) {
        closeOffsetReader();
        throw (ex instanceof IOException) ? (IOException)ex : new IOException(ex);
      }
    }

    private void closeOffsetReader() {
      if (offsetReader == null) return;
      try {
        offsetReader.close();
      } catch (Exception ex) {
        LlapIoImpl.LOG.error("Failed to close source reader", ex);
      }
      offsetReader = null;
    }
  }

  public void startReadSplitFromFile(
      FileSplit split, boolean[] splitIncludes, StripeData slice) throws IOException {
    boolean maySplitTheSplit = slice == null;
    ReaderWithOffsets offsetReader = null;
    @SuppressWarnings("rawtypes")
    RecordReader sourceReader = sourceInputFormat.getRecordReader(split, jobConf, reporter);
    try {
      offsetReader = createOffsetReader(sourceReader);
      sourceReader = null;
    } finally {
      if (sourceReader != null) {
        try {
          sourceReader.close();
        } catch (Exception ex) {
          LlapIoImpl.LOG.error("Failed to close source reader", ex);
        }
      }
    }
    maySplitTheSplit = maySplitTheSplit && offsetReader.hasOffsets();

    try {
      StructObjectInspector originalOi = (StructObjectInspector)getOiFromSerDe();
      List<Integer> splitColumnIds = OrcInputFormat.genIncludedColumnsReverse(
          schema, splitIncludes, false);
      // fileread writes to the writer, which writes to orcWriter, which writes to cacheWriter
      EncodingWriter writer = VectorDeserializeOrcWriter.create(
          sourceInputFormat, sourceSerDe, parts, daemonConf, jobConf, split.getPath(), originalOi,
          splitColumnIds, splitIncludes, allocSize);
      // TODO: move this into ctor? EW would need to create CacheWriter then
      List<Integer> cwColIds = writer.isOnlyWritingIncludedColumns() ? splitColumnIds : columnIds;
      writer.init(new CacheWriter(bufferManager, cwColIds, splitIncludes,
          writer.isOnlyWritingIncludedColumns(), bufferFactory), daemonConf, split.getPath());
      if (writer instanceof VectorDeserializeOrcWriter) {
        VectorDeserializeOrcWriter asyncWriter = (VectorDeserializeOrcWriter)writer;
        asyncWriter.startAsync(new AsyncCacheDataCallback());
        this.asyncWriters.add(asyncWriter);
      }
      currentFileRead = new FileReaderYieldReturn(
          offsetReader, split, writer, maySplitTheSplit, targetSliceRowCount);
    } finally {
      // Assignment is the last thing in the try, so if it happen we assume success.
      if (currentFileRead != null) return;
      if (offsetReader == null) return;
      try {
        offsetReader.close();
      } catch (Exception ex) {
        LlapIoImpl.LOG.error("Failed to close source reader", ex);
      }
    }
  }

  private class AsyncCacheDataCallback implements AsyncCallback {
    @Override
    public void onComplete(VectorDeserializeOrcWriter writer) {
      CacheWriter cacheWriter = null;
      try {
        cacheWriter = writer.getCacheWriter();
        // What we were reading from disk originally.
        boolean[] cacheIncludes = writer.getOriginalCacheIncludes();
        Iterator<CacheWriter.CacheStripeData> iter = cacheWriter.stripes.iterator();
        while (iter.hasNext()) {
          processAsyncCacheData(iter.next(), cacheIncludes);
          iter.remove();
        }
      } catch (IOException e) {
        LlapIoImpl.LOG.error("Failed to cache async data", e);
      } finally {
        cacheWriter.discardData();
      }
    }
  }

  // TODO: this interface is ugly. The two implementations are so far apart feature-wise
  //       after all the perf changes that we might was well hardcode them separately.
  static abstract class EncodingWriter {
    protected Writer orcWriter;
    protected CacheWriter cacheWriter;
    protected final StructObjectInspector sourceOi;
    private final int allocSize;

    public EncodingWriter(StructObjectInspector sourceOi, int allocSize) {
      this.sourceOi = sourceOi;
      this.allocSize = allocSize;
    }


    public void init(CacheWriter cacheWriter, Configuration conf, Path path) throws IOException {
      this.orcWriter = createOrcWriter(cacheWriter, conf, path, sourceOi);
      this.cacheWriter = cacheWriter;
    }

    public CacheWriter getCacheWriter() {
      return cacheWriter;
    }
    public abstract boolean isOnlyWritingIncludedColumns();

    public abstract void writeOneRow(Writable row) throws IOException;
    public abstract void setCurrentStripeOffsets(long currentKnownTornStart,
        long firstStartOffset, long lastStartOffset, long fileOffset);
    public abstract void flushIntermediateData() throws IOException;
    public abstract void writeIntermediateFooter() throws IOException;
    public abstract List<VectorizedRowBatch> extractCurrentVrbs();
    public void close() throws IOException {
      if (orcWriter != null) {
        try {
          orcWriter.close();
          orcWriter = null;
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

    protected Writer createOrcWriter(CacheWriter cacheWriter, Configuration conf,
        Path path, StructObjectInspector oi) throws IOException {
      // TODO: this is currently broken. We need to set memory manager to a bogus implementation
      //       to avoid problems with memory manager actually tracking the usage.
      return OrcFile.createWriter(path, createOrcWriterOptions(oi, conf, cacheWriter, allocSize));
    }
  }

  static class DeserializerOrcWriter extends EncodingWriter {
    private final Deserializer sourceSerDe;

    public DeserializerOrcWriter(
        Deserializer sourceSerDe, StructObjectInspector sourceOi, int allocSize) {
      super(sourceOi, allocSize);
      this.sourceSerDe = sourceSerDe;
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
    public boolean isOnlyWritingIncludedColumns() {
      return false; // LazySimpleSerDe doesn't support projection.
    }

    @Override
    public void setCurrentStripeOffsets(long currentKnownTornStart,
        long firstStartOffset, long lastStartOffset, long fileOffset) {
      cacheWriter.setCurrentStripeOffsets(
          currentKnownTornStart, firstStartOffset, lastStartOffset, fileOffset);
    }

    @Override
    public List<VectorizedRowBatch> extractCurrentVrbs() {
      return null; // Doesn't support creating VRBs.
    }
  }

  private static final class NoopMemoryManager extends MemoryManager {
    public NoopMemoryManager() {
      super(null);
    }

    @Override
    public void addedRow(int rows) {}
    @Override
    public void addWriter(Path path, long requestedAllocation, Callback callback) {}
    @Override
    public void notifyWriters() {}
    @Override
    public void removeWriter(Path path) throws IOException {}
  }
  private static final NoopMemoryManager MEMORY_MANAGER = new NoopMemoryManager();

  static WriterOptions createOrcWriterOptions(ObjectInspector sourceOi,
      Configuration conf, CacheWriter cacheWriter, int allocSize) throws IOException {
    return OrcFile.writerOptions(conf).stripeSize(Long.MAX_VALUE).blockSize(Long.MAX_VALUE)
        .rowIndexStride(Integer.MAX_VALUE) // For now, do not limit this - one RG per split
        .blockPadding(false).compress(CompressionKind.NONE).version(Version.CURRENT)
        .encodingStrategy(EncodingStrategy.SPEED).bloomFilterColumns(null).inspector(sourceOi)
        .physicalWriter(cacheWriter).memory(MEMORY_MANAGER).bufferSize(allocSize);
  }

  private ObjectInspector getOiFromSerDe() throws IOException {
    try {
      return sourceSerDe.getObjectInspector();
    } catch (SerDeException e) {
      throw new IOException(e);
    }
  }

  private ReaderWithOffsets createOffsetReader(RecordReader<?, ?> sourceReader) {
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
      boolean hasCachedSlice, CacheWriter.CacheStripeData diskData) throws InterruptedException {
    if (ecb == null) { // This basically means stop has been called.
      cleanup(true);
      return false;
    }
    LlapIoImpl.LOG.trace("Sending a batch over to consumer");
    consumer.consumeData(ecb);
    if (hasCachedSlice) {
      cachedData.getData().remove(0); // See javadoc - no need to clean up the cache data anymore.
    }
    if (diskData != null) {
      diskData.colStreams.clear();
    }
    return true;
  }

  private void cleanup(boolean isError) {
    cleanUpCurrentRead();
    if (!isError) return;
    for (VectorDeserializeOrcWriter asyncWriter : asyncWriters) {
      try {
        asyncWriter.interrupt();
      } catch (Exception ex) {
        LlapIoImpl.LOG.warn("Failed to interrupt an async writer", ex);
      }
    }
    asyncWriters.clear();
  }

  private void cleanUpCurrentRead() {
    if (currentFileRead == null) return;
    try {
      currentFileRead.closeOffsetReader();
      currentFileRead = null;
    } catch (Exception ex) {
      LlapIoImpl.LOG.error("Failed to close current file reader", ex);
    }
  }

  private void recordReaderTime(long startTime) {
    counters.incrTimeCounter(LlapIOCounters.TOTAL_IO_TIME_NS, startTime);
  }

  private boolean processStop() {
    if (!isStopped) return false;
    LlapIoImpl.LOG.info("SerDe-based data reader is stopping");
    cleanup(true);
    return true;
  }

  private static Object determineFileId(FileSystem fs, FileSplit split,
      boolean allowSynthetic, boolean checkDefaultFs) throws IOException {
    /* TODO: support this optionally? this is not OrcSplit, but we could add a custom split.
      Object fileKey = ((OrcSplit)split).getFileKey();
      if (fileKey != null) return fileKey; */
    LlapIoImpl.LOG.warn("Split for " + split.getPath() + " (" + split.getClass() + ") does not have file ID");
    return HdfsUtils.getFileId(fs, split.getPath(), allowSynthetic, checkDefaultFs);
  }

  @Override
  public void returnData(OrcEncodedColumnBatch ecb) {
    for (int colIx = 0; colIx < ecb.getTotalColCount(); ++colIx) {
      if (!ecb.hasData(colIx)) continue;
      // TODO: reuse columnvector-s on hasBatch - save the array by column? take apart each list.
      ColumnStreamData[] datas = ecb.getColumnData(colIx);
      for (ColumnStreamData data : datas) {
        if (data == null || data.decRef() != 0) continue;
        if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
          for (MemoryBuffer buf : data.getCacheBuffers()) {
            LlapIoImpl.LOCKING_LOGGER.trace("Unlocking {} at the end of processing", buf);
          }
        }
        bufferManager.decRefBuffers(data.getCacheBuffers());
        if (useObjectPools) {
          CSD_POOL.offer(data);
        }
      }
    }
    // We can offer ECB even with some streams not discarded; reset() will clear the arrays.
    if (useObjectPools) {
      ECB_POOL.offer(ecb);
    }
  }

  @Override
  public TezCounters getTezCounters() {
    return counters.getTezCounters();
  }
}
