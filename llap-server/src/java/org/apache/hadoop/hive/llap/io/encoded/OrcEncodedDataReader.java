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
import java.util.List;

import org.apache.hadoop.hive.llap.counters.LlapIOCounters;
import org.apache.orc.CompressionCodec;
import org.apache.orc.OrcProto.BloomFilterIndex;
import org.apache.orc.OrcProto.FileTail;
import org.apache.orc.OrcProto.RowIndex;
import org.apache.orc.OrcProto.Stream;
import org.apache.orc.OrcProto.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.DataReaderProperties;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcCodecPool;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.impl.SchemaEvolution;
import org.apache.orc.impl.WriterImpl;
import org.apache.tez.common.counters.TezCounters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.Pool;
import org.apache.hadoop.hive.common.Pool.PoolObjectHelper;
import org.apache.hadoop.hive.common.io.Allocator.BufferObjectFactory;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch.ColumnStreamData;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.ConsumerFeedback;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.cache.BufferUsageManager;
import org.apache.hadoop.hive.llap.cache.LlapDataBuffer;
import org.apache.hadoop.hive.llap.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer.Includes;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer.SchemaEvolutionFactory;
import org.apache.hadoop.hive.llap.io.decode.OrcEncodedDataConsumer;
import org.apache.hadoop.hive.llap.io.metadata.OrcFileMetadata;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache.LlapBufferOrBuffers;
import org.apache.hadoop.hive.llap.io.metadata.OrcStripeMetadata;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HdfsUtils;
import org.apache.orc.CompressionKind;
import org.apache.orc.DataReader;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.ReaderOptions;
import org.apache.orc.OrcConf;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;
import org.apache.hadoop.hive.ql.io.orc.encoded.EncodedOrcFile;
import org.apache.hadoop.hive.ql.io.orc.encoded.EncodedReader;
import org.apache.hadoop.hive.ql.io.orc.encoded.IoTrace;
import org.apache.hadoop.hive.ql.io.orc.encoded.OrcBatchKey;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader.OrcEncodedColumnBatch;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader.PoolFactory;
import org.apache.orc.impl.RecordReaderUtils;
import org.apache.orc.StripeInformation;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.FixedSizedObjectPool;
import org.apache.orc.OrcProto;
import org.apache.tez.common.CallableWithNdc;

import com.google.common.collect.Lists;

/**
 * This produces EncodedColumnBatch via ORC EncodedDataImpl.
 * It serves as Consumer for EncodedColumnBatch too, for the high-level cache scenario where
 * it inserts itself into the pipeline to put the data in cache, before passing it to the real
 * consumer. It also serves as ConsumerFeedback that receives processed EncodedColumnBatch-es.
 */
public class OrcEncodedDataReader extends CallableWithNdc<Void>
    implements ConsumerFeedback<OrcEncodedColumnBatch>, TezCounterSource {
  private static final Logger LOG = LoggerFactory.getLogger(OrcEncodedDataReader.class);
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
  private static final PoolFactory POOL_FACTORY = new PoolFactory() {
    @Override
    public Pool<ColumnStreamData> createColumnStreamDataPool() {
      return CSD_POOL;
    }

    @Override
    public Pool<OrcEncodedColumnBatch> createEncodedColumnBatchPool() {
      return ECB_POOL;
    }
  };

  private final MetadataCache metadataCache;
  private final LowLevelCache lowLevelCache;
  private final BufferUsageManager bufferManager;
  private final Configuration daemonConf, jobConf;
  private final FileSplit split;
  private final SearchArgument sarg;
  private final OrcEncodedDataConsumer consumer;
  private final QueryFragmentCounters counters;
  private final UserGroupInformation ugi;
  private final SchemaEvolution evolution;
  private final boolean useCodecPool, useObjectPools;

  // Read state.
  private int stripeIxFrom;
  private OrcFileMetadata fileMetadata;
  private Path path;
  private Reader orcReader;
  private DataReader rawDataReader;
  private boolean isRawDataReaderOpen = false;
  private EncodedReader stripeReader;
  private CompressionCodec codec;
  private Object fileKey;
  private final String cacheTag;
  private FileSystem fs;

  /**
   * stripeRgs[stripeIx'] => boolean array (could be a bitmask) of rg-s that need to be read.
   * Contains only stripes that are read, and only columns included. null => read all RGs.
   */
  private boolean[][] stripeRgs;
  private volatile boolean isStopped = false;
  @SuppressWarnings("unused")
  private volatile boolean isPaused = false;

  boolean[] sargColumns = null, fileIncludes = null;
  private final IoTrace trace;
  private Pool<IoTrace> tracePool;

  public OrcEncodedDataReader(LowLevelCache lowLevelCache, BufferUsageManager bufferManager,
      MetadataCache metadataCache, Configuration daemonConf, Configuration jobConf,
      FileSplit split, Includes includes, SearchArgument sarg, OrcEncodedDataConsumer consumer,
      QueryFragmentCounters counters, SchemaEvolutionFactory sef, Pool<IoTrace> tracePool)
          throws IOException {
    this.lowLevelCache = lowLevelCache;
    this.metadataCache = metadataCache;
    this.bufferManager = bufferManager;
    this.daemonConf = daemonConf;
    this.split = split;
    this.sarg = sarg;
    this.consumer = consumer;
    this.counters = counters;
    this.trace = tracePool.take();
    this.tracePool = tracePool;
    try {
      this.ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.useCodecPool = HiveConf.getBoolVar(daemonConf, ConfVars.HIVE_ORC_CODEC_POOL);
    this.useObjectPools = HiveConf.getBoolVar(daemonConf, ConfVars.LLAP_IO_SHARE_OBJECT_POOLS);

    // LlapInputFormat needs to know the file schema to decide if schema evolution is supported.
    orcReader = null;
    cacheTag = HiveConf.getBoolVar(daemonConf, ConfVars.LLAP_TRACK_CACHE_USAGE)
        ? LlapUtil.getDbAndTableNameForMetrics(split.getPath(), true) : null;
    // 1. Get file metadata from cache, or create the reader and read it.
    // Don't cache the filesystem object for now; Tez closes it and FS cache will fix all that
    fs = split.getPath().getFileSystem(jobConf);
    fileKey = determineFileId(fs, split,
        HiveConf.getBoolVar(daemonConf, ConfVars.LLAP_CACHE_ALLOW_SYNTHETIC_FILEID),
        HiveConf.getBoolVar(daemonConf, ConfVars.LLAP_CACHE_DEFAULT_FS_FILE_ID));
    fileMetadata = getFileFooterFromCacheOrDisk();
    final TypeDescription fileSchema = fileMetadata.getSchema();

    fileIncludes = includes.generateFileIncludes(fileSchema);
    if (LOG.isDebugEnabled()) {
      LOG.debug("From {}, the file includes are {}", includes, DebugUtils.toString(fileIncludes));
    }

    // Do not allow users to override zero-copy setting. The rest can be taken from user config.
    boolean useZeroCopy = OrcConf.USE_ZEROCOPY.getBoolean(daemonConf);
    if (useZeroCopy != OrcConf.USE_ZEROCOPY.getBoolean(jobConf)) {
      jobConf = new Configuration(jobConf);
      jobConf.setBoolean(OrcConf.USE_ZEROCOPY.getAttribute(), useZeroCopy);
    }
    this.jobConf = jobConf;
    // TODO: setFileMetadata could just create schema. Called in two places; clean up later.
    this.evolution = sef.createSchemaEvolution(fileMetadata.getSchema());
    consumer.setFileMetadata(fileMetadata);
    consumer.setSchemaEvolution(evolution);
  }

  @Override
  public void stop() {
    LOG.debug("Encoded reader is being stopped");
    isStopped = true;
  }

  @Override
  public void pause() {
    isPaused = true;
    // TODO: pause fetching
  }

  @Override
  public void unpause() {
    isPaused = false;
    // TODO: unpause fetching
  }

  @Override
  protected Void callInternal() throws IOException, InterruptedException {
    return ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        return performDataRead();
      }
    });
  }

  protected Void performDataRead() throws IOException, InterruptedException {
    long startTime = counters.startTimeCounter();
    LlapIoImpl.LOG.info("Processing data for file {}: {}", fileKey, split.getPath());
    if (processStop()) {
      recordReaderTime(startTime);
      return null;
    }
    counters.setDesc(QueryFragmentCounters.Desc.TABLE,
        LlapUtil.getDbAndTableNameForMetrics(split.getPath(), false));
    counters.setDesc(QueryFragmentCounters.Desc.FILE, split.getPath()
        + (fileKey == null ? "" : " (" + fileKey + ")"));
    try {
      validateFileMetadata();

      // 2. Determine which stripes to read based on the split.
      determineStripesToRead();
    } catch (Throwable t) {
      handleReaderError(startTime, t);
      return null;
    }

    if (stripeRgs.length == 0) {
      consumer.setDone();
      recordReaderTime(startTime);
      tracePool.offer(trace);
      return null; // No data to read.
    }
    counters.setDesc(QueryFragmentCounters.Desc.STRIPES,
        stripeIxFrom + "," + stripeRgs.length);

    // 3. Apply SARG if needed, and otherwise determine what RGs to read.
    int stride = fileMetadata.getRowIndexStride();
    ArrayList<OrcStripeMetadata> stripeMetadatas = null;
    try {
      if (sarg != null && stride != 0) {
        // TODO: move this to a common method
        // Note: this gets IDs by name, so we assume indices don't need to be adjusted for ACID.
        int[] filterColumns = RecordReaderImpl.mapSargColumnsToOrcInternalColIdx(
          sarg.getLeaves(), evolution);
        // included will not be null, row options will fill the array with trues if null
        sargColumns = new boolean[evolution.getFileSchema().getMaximumId() + 1];
        for (int i : filterColumns) {
          // filter columns may have -1 as index which could be partition column in SARG.
          // TODO: should this then be >=?
          if (i > 0) {
            sargColumns[i] = true;
          }
        }

        // If SARG is present, get relevant stripe metadata from cache or readers.
        stripeMetadatas = readStripesMetadata(fileIncludes, sargColumns);
      }

      // Now, apply SARG if any; w/o sarg, this will just initialize stripeRgs.
      boolean hasData = determineRgsToRead(stride, stripeMetadatas);
      if (!hasData) {
        consumer.setDone();
        recordReaderTime(startTime);
        tracePool.offer(trace);
        return null; // No data to read.
      }
    } catch (Throwable t) {
      handleReaderError(startTime, t);
      return null;
    }

    if (processStop()) {
      recordReaderTime(startTime);
      return null;
    }

    // 4. Create encoded data reader.
    try {
      ensureDataReader();
    } catch (Throwable t) {
      handleReaderError(startTime, t);
      return null;
    }

    // 6. Read data.
    // TODO: I/O threadpool could be here - one thread per stripe; for now, linear.
    boolean hasFileId = this.fileKey != null;
    OrcBatchKey stripeKey = hasFileId ? new OrcBatchKey(fileKey, -1, 0) : null;
    for (int stripeIxMod = 0; stripeIxMod < stripeRgs.length; ++stripeIxMod) {
      if (processStop()) {
        recordReaderTime(startTime);
        return null;
      }
      int stripeIx = stripeIxFrom + stripeIxMod;
      boolean[] rgs = null;
      OrcStripeMetadata stripeMetadata = null;
      StripeInformation si;
      try {
        si = fileMetadata.getStripes().get(stripeIx);
        LlapIoImpl.ORC_LOGGER.trace("Reading stripe {}: {}, {}", stripeIx, si.getOffset(),
            si.getLength());
        trace.logReadingStripe(stripeIx, si.getOffset(), si.getLength());
        rgs = stripeRgs[stripeIxMod];
        if (LlapIoImpl.ORC_LOGGER.isTraceEnabled()) {
          LlapIoImpl.ORC_LOGGER.trace("stripeRgs[{}]: {}", stripeIxMod, Arrays.toString(rgs));
        }
        // We assume that NO_RGS value is only set from SARG filter and for all columns;
        // intermediate changes for individual columns will unset values in the array.
        // Skip this case for 0-column read. We could probably special-case it just like we do
        // in EncodedReaderImpl, but for now it's not that important.
        if (rgs == RecordReaderImpl.SargApplier.READ_NO_RGS) continue;

        // 6.2. Ensure we have stripe metadata. We might have read it before for RG filtering.
        if (stripeMetadatas != null) {
          stripeMetadata = stripeMetadatas.get(stripeIxMod);
        } else {
          stripeKey.stripeIx = stripeIx;
          OrcProto.StripeFooter footer = getStripeFooterFromCacheOrDisk(si, stripeKey);
          stripeMetadata = createOrcStripeMetadataObject(
              stripeIx, si, footer, fileIncludes, sargColumns);
          ensureDataReader();
          stripeReader.readIndexStreams(stripeMetadata.getIndex(),
              si, footer.getStreamsList(), fileIncludes, sargColumns);
          consumer.setStripeMetadata(stripeMetadata);
        }
      } catch (Throwable t) {
        handleReaderError(startTime, t);
        return null;
      }
      if (processStop()) {
        recordReaderTime(startTime);
        return null;
      }

      // 5.2. Finally, hand off to the stripe reader to produce the data.
      //      This is a sync call that will feed data to the consumer.
      try {
        // TODO: readEncodedColumns is not supposed to throw; errors should be propagated thru
        // consumer. It is potentially holding locked buffers, and must perform its own cleanup.
        // Also, currently readEncodedColumns is not stoppable. The consumer will discard the
        // data it receives for one stripe. We could probably interrupt it, if it checked that.
        stripeReader.readEncodedColumns(stripeIx, si, stripeMetadata.getRowIndexes(),
            stripeMetadata.getEncodings(), stripeMetadata.getStreams(), fileIncludes,
            rgs, consumer);
      } catch (Throwable t) {
        handleReaderError(startTime, t);
        return null;
      }
    }

    // Done with all the things.
    recordReaderTime(startTime);
    consumer.setDone();

    LlapIoImpl.LOG.trace("done processing {}", split);
    tracePool.offer(trace);
    // Close the stripe reader, we are done reading.
    cleanupReaders();
    return null;
  }

  private void handleReaderError(long startTime, Throwable t) throws InterruptedException {
    recordReaderTime(startTime);
    consumer.setError(t);
    trace.dumpLog(LOG);
    cleanupReaders();
    tracePool.offer(trace);
  }

  private void ensureDataReader() throws IOException {
    ensureOrcReader();
    // Reader creation updates HDFS counters, don't do it here.
    DataWrapperForOrc dw = new DataWrapperForOrc();
    stripeReader = orcReader.encodedReader(
        fileKey, dw, dw, useObjectPools ? POOL_FACTORY : null, trace, useCodecPool, cacheTag);
    stripeReader.setTracing(LlapIoImpl.ORC_LOGGER.isTraceEnabled());
  }

  private void recordReaderTime(long startTime) {
    counters.incrTimeCounter(LlapIOCounters.TOTAL_IO_TIME_NS, startTime);
  }

  private void validateFileMetadata() throws IOException {
    if (fileMetadata.getCompressionKind() == CompressionKind.NONE) return;
    int bufferSize = fileMetadata.getCompressionBufferSize();
    long minAllocSize = HiveConf.getSizeVar(daemonConf, ConfVars.LLAP_ALLOCATOR_MIN_ALLOC);
    if (bufferSize < minAllocSize) {
      LOG.warn("ORC compression buffer size (" + bufferSize + ") is smaller than LLAP low-level "
            + "cache minimum allocation size (" + minAllocSize + "). Decrease the value for "
            + HiveConf.ConfVars.LLAP_ALLOCATOR_MIN_ALLOC.toString() + " to avoid wasting memory");
    }
  }

  private boolean processStop() {
    if (!isStopped) return false;
    LOG.info("Encoded data reader is stopping");
    tracePool.offer(trace);
    cleanupReaders();
    return true;
  }

  private static Object determineFileId(FileSystem fs, FileSplit split,
      boolean allowSynthetic, boolean checkDefaultFs) throws IOException {
    if (split instanceof OrcSplit) {
      Object fileKey = ((OrcSplit)split).getFileKey();
      if (fileKey != null) {
        return fileKey;
      }
    }
    LOG.warn("Split for " + split.getPath() + " (" + split.getClass() + ") does not have file ID");
    return HdfsUtils.getFileId(fs, split.getPath(), allowSynthetic, checkDefaultFs);
  }

  /**
   * Closes the stripe readers (on error).
   */
  private void cleanupReaders() {
    if (stripeReader != null) {
      try {
        stripeReader.close();
      } catch (IOException ex) {
        // Ignore.
      }
    }
    if (rawDataReader != null && isRawDataReaderOpen) {
      try {
        rawDataReader.close();
        rawDataReader = null;
      } catch (IOException ex) {
        // Ignore.
      }
    }
  }

  /**
   * Ensures orcReader is initialized for the split.
   */
  private void ensureOrcReader() throws IOException {
    if (orcReader != null) return;
    path = split.getPath();
    if (fileKey instanceof Long && HiveConf.getBoolVar(
        daemonConf, ConfVars.LLAP_IO_USE_FILEID_PATH)) {
      path = HdfsUtils.getFileIdPath(fs, path, (long)fileKey);
    }
    LlapIoImpl.ORC_LOGGER.trace("Creating reader for {} ({})", path, split.getPath());
    long startTime = counters.startTimeCounter();
    ReaderOptions opts = OrcFile.readerOptions(jobConf).filesystem(fs).fileMetadata(fileMetadata);
    if (split instanceof OrcSplit) {
      OrcTail orcTail = ((OrcSplit) split).getOrcTail();
      if (orcTail != null) {
        LlapIoImpl.ORC_LOGGER.debug("Setting OrcTail. path={}", path);
        opts.orcTail(orcTail);
      }
    }
    orcReader = EncodedOrcFile.createReader(path, opts);
    counters.incrTimeCounter(LlapIOCounters.HDFS_TIME_NS, startTime);
  }

  /**
   * Ensure codec is created for the split, to decode values from cache. Can only be called
   * after initializing fileMetadata.
   */
  private void ensureCodecFromFileMetadata() {
    if (codec != null) return;
    codec = WriterImpl.createCodec(fileMetadata.getCompressionKind());
  }

  /**
   *  Gets file metadata for the split from cache, or reads it from the file.
   */
  private OrcFileMetadata getFileFooterFromCacheOrDisk() throws IOException {
    LlapBufferOrBuffers tailBuffers = null;
    List<StripeStatistics> stats = null;
    List<StripeInformation> stripes = null;
    boolean hasCache = fileKey != null && metadataCache != null;
    if (hasCache) {
      tailBuffers = metadataCache.getFileMetadata(fileKey);
      if (tailBuffers != null) {
        try {
          MemoryBuffer tailBuffer = tailBuffers.getSingleBuffer();
          ByteBuffer bb = null;
          if (tailBuffer != null) {
            bb = tailBuffer.getByteBufferDup();
            // TODO: remove the copy after ORC-158 and ORC-197
            // if (bb.isDirect()) {
              ByteBuffer dupBb = tailBuffer.getByteBufferDup(); // Don't mess with the cached object.
              bb = ByteBuffer.allocate(dupBb.remaining());
              bb.put(dupBb);
              bb.flip();
            // }
          } else {
            // TODO: add the ability to extractFileTail to read from multiple buffers?
            MemoryBuffer[] tailBufferArray = tailBuffers.getMultipleBuffers();
            int totalSize = 0;
            for (MemoryBuffer buf : tailBufferArray) {
              totalSize += buf.getByteBufferRaw().remaining();
            }
            bb = ByteBuffer.allocate(totalSize);
            for (MemoryBuffer buf : tailBufferArray) {
              bb.put(buf.getByteBufferDup());
            }
            bb.flip();
          }
          OrcTail orcTail = ReaderImpl.extractFileTail(bb);
          FileTail tail = orcTail.getFileTail();
          stats = orcTail.getStripeStatisticsProto();
          stripes = new ArrayList<>(tail.getFooter().getStripesCount());
          for (OrcProto.StripeInformation stripeProto : tail.getFooter().getStripesList()) {
            stripes.add(new ReaderImpl.StripeInformationImpl(stripeProto));
          }
          return new OrcFileMetadata(
              fileKey, tail.getFooter(), tail.getPostscript(), stats, stripes);
        } finally {
          // We don't need the buffer anymore.
          metadataCache.decRefBuffer(tailBuffers);
          counters.incrCounter(LlapIOCounters.METADATA_CACHE_HIT);
        }
      }
      counters.incrCounter(LlapIOCounters.METADATA_CACHE_MISS);
    }
    ensureOrcReader();
    ByteBuffer tailBufferBb = orcReader.getSerializedFileFooter();
    if (hasCache) {
      tailBuffers = metadataCache.putFileMetadata(fileKey, tailBufferBb, cacheTag);
      metadataCache.decRefBuffer(tailBuffers); // We don't use the cache's copy of the buffer.
    }
    FileTail ft = orcReader.getFileTail();
    return new OrcFileMetadata(fileKey, ft.getFooter(), ft.getPostscript(),
        orcReader.getOrcProtoStripeStatistics(), orcReader.getStripes());
  }

  private OrcProto.StripeFooter buildStripeFooter(
      List<DiskRange> bcs, int len, CompressionCodec codec, int bufferSize) throws IOException {
    return OrcProto.StripeFooter.parseFrom(InStream.createCodedInputStream(
        "footer", bcs, len, codec, bufferSize));
  }

  /**
   * Reads the metadata for all stripes in the file.
   */
  private ArrayList<OrcStripeMetadata> readStripesMetadata(
      boolean[] includes, boolean[] sargColumns) throws IOException {
    ArrayList<OrcStripeMetadata> result = new ArrayList<OrcStripeMetadata>(stripeRgs.length);
    boolean hasFileId = this.fileKey != null;
    OrcBatchKey stripeKey = hasFileId ? new OrcBatchKey(fileKey, 0, 0) : null;
    for (int stripeIxMod = 0; stripeIxMod < stripeRgs.length; ++stripeIxMod) {
      int stripeIx = stripeIxMod + stripeIxFrom;
      stripeKey.stripeIx = stripeIx;
      StripeInformation si = fileMetadata.getStripes().get(stripeIx);
      OrcProto.StripeFooter footer = getStripeFooterFromCacheOrDisk(si, stripeKey);
      OrcStripeMetadata osm = createOrcStripeMetadataObject(
          stripeIx, si, footer, includes, sargColumns);

      ensureDataReader();
      OrcIndex index = osm.getIndex();
      stripeReader.readIndexStreams(index, si, footer.getStreamsList(), includes, sargColumns);
      result.add(osm);
      consumer.setStripeMetadata(osm);
    }
    return result;
  }

  private OrcStripeMetadata createOrcStripeMetadataObject(int stripeIx, StripeInformation si,
      OrcProto.StripeFooter footer, boolean[] includes, boolean[] sargColumns) throws IOException {
    Stream.Kind[] bks = sargColumns == null ? null : new Stream.Kind[includes.length];
    BloomFilterIndex[] bis = sargColumns == null ? null : new BloomFilterIndex[includes.length];
    return new OrcStripeMetadata(new OrcBatchKey(fileKey, stripeIx, 0), footer,
        new OrcIndex(new RowIndex[includes.length], bks, bis), si);
  }

  private OrcProto.StripeFooter getStripeFooterFromCacheOrDisk(
      StripeInformation si, OrcBatchKey stripeKey) throws IOException {
    boolean hasCache = fileKey != null && metadataCache != null;
    if (hasCache) {
      LlapBufferOrBuffers footerBuffers = metadataCache.getStripeTail(stripeKey);
      if (footerBuffers != null) {
        try {
          counters.incrCounter(LlapIOCounters.METADATA_CACHE_HIT);
          ensureCodecFromFileMetadata();
          MemoryBuffer footerBuffer = footerBuffers.getSingleBuffer();
          if (footerBuffer != null) {
            ByteBuffer bb = footerBuffer.getByteBufferDup();
            return buildStripeFooter(Lists.<DiskRange>newArrayList(new BufferChunk(bb, 0)),
                bb.remaining(), codec, fileMetadata.getCompressionBufferSize());
          } else {
            MemoryBuffer[] footerBufferArray = footerBuffers.getMultipleBuffers();
            int pos = 0;
            List<DiskRange> bcs = new ArrayList<>(footerBufferArray.length);
            for (MemoryBuffer buf : footerBufferArray) {
              ByteBuffer bb = buf.getByteBufferDup();
              bcs.add(new BufferChunk(bb, pos));
              pos += bb.remaining();
            }
            return buildStripeFooter(bcs, pos, codec, fileMetadata.getCompressionBufferSize());
          }
        } finally {
          metadataCache.decRefBuffer(footerBuffers);
        }
      }
      counters.incrCounter(LlapIOCounters.METADATA_CACHE_MISS);
    }
    long offset = si.getOffset() + si.getIndexLength() + si.getDataLength();
    long startTime = counters.startTimeCounter();
    ensureRawDataReader(true);
    // TODO: add this to metadatareader in ORC - SI => metadata buffer, not just metadata.
    if (LOG.isTraceEnabled()) {
      LOG.trace("Reading [" + offset + ", "
          + (offset + si.getFooterLength()) + ") based on " + si);
    }
    DiskRangeList footerRange = rawDataReader.readFileData(
        new DiskRangeList(offset, offset + si.getFooterLength()), 0, false);
    // LOG.error("Got " + RecordReaderUtils.stringifyDiskRanges(footerRange));
    counters.incrTimeCounter(LlapIOCounters.HDFS_TIME_NS, startTime);
    assert footerRange.next == null; // Can only happens w/zcr for a single input buffer.
    if (hasCache) {
      LlapBufferOrBuffers cacheBuf = metadataCache.putStripeTail(
          stripeKey, footerRange.getData().duplicate(), cacheTag);
      metadataCache.decRefBuffer(cacheBuf); // We don't use this one.
    }
    ByteBuffer bb = footerRange.getData().duplicate();

    CompressionKind kind = orcReader.getCompressionKind();
    boolean isPool = useCodecPool;
    CompressionCodec codec = isPool ? OrcCodecPool.getCodec(kind) : WriterImpl.createCodec(kind);
    boolean isCodecError = true;
    try {
      OrcProto.StripeFooter result = buildStripeFooter(Lists.<DiskRange>newArrayList(
          new BufferChunk(bb, 0)), bb.remaining(), codec, orcReader.getCompressionSize());
      isCodecError = false;
      return result;
    } finally {
      try {
        if (codec != null) {
          if (isPool && !isCodecError) {
            OrcCodecPool.returnCodec(kind, codec);
          } else {
            codec.close();
          }
        }
      } catch (Exception ex) {
        LOG.error("Ignoring codec cleanup error", ex);
      }
    }
  }

  private void ensureRawDataReader(boolean isOpen) throws IOException {
    ensureOrcReader();
    if (rawDataReader != null) {
      if (!isRawDataReaderOpen && isOpen) {
        long startTime = counters.startTimeCounter();
        rawDataReader.open();
        counters.incrTimeCounter(LlapIOCounters.HDFS_TIME_NS, startTime);
      }
      return;
    }
    long startTime = counters.startTimeCounter();
    boolean useZeroCopy = (daemonConf != null) && OrcConf.USE_ZEROCOPY.getBoolean(daemonConf);
    rawDataReader = RecordReaderUtils.createDefaultDataReader(
        DataReaderProperties.builder().withBufferSize(orcReader.getCompressionSize())
        .withCompression(orcReader.getCompressionKind())
        .withFileSystem(fs).withPath(path)
        .withTypeCount(orcReader.getSchema().getMaximumId() + 1)
        .withZeroCopy(useZeroCopy)
        .build());

    if (isOpen) {
      rawDataReader.open();
      isRawDataReaderOpen = true;
    }
    counters.incrTimeCounter(LlapIOCounters.HDFS_TIME_NS, startTime);
  }

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

  /**
   * Determines which RGs need to be read, after stripes have been determined.
   * SARG is applied, and readState is populated for each stripe accordingly.
   */
  private boolean determineRgsToRead(int rowIndexStride,
      ArrayList<OrcStripeMetadata> metadata) throws IOException {
    RecordReaderImpl.SargApplier sargApp = null;
    if (sarg != null && rowIndexStride != 0) {
      sargApp = new RecordReaderImpl.SargApplier(sarg,
          rowIndexStride, evolution,
          OrcFile.WriterVersion.from(fileMetadata.getWriterVersionNum()));
    }
    boolean hasAnyData = false;
    // stripeRgs should have been initialized by this time with an empty array.
    for (int stripeIxMod = 0; stripeIxMod < stripeRgs.length; ++stripeIxMod) {
      int stripeIx = stripeIxMod + stripeIxFrom;
      StripeInformation stripe = fileMetadata.getStripes().get(stripeIx);
      int rgCount = getRgCount(stripe, rowIndexStride);
      boolean[] rgsToRead = null;
      if (sargApp != null) {
        OrcStripeMetadata stripeMetadata = metadata.get(stripeIxMod);
        rgsToRead = sargApp.pickRowGroups(stripe, stripeMetadata.getRowIndexes(),
            stripeMetadata.getBloomFilterKinds(),
            stripeMetadata.getEncodings(),
            stripeMetadata.getBloomFilterIndexes(), true);
      }
      boolean isNone = rgsToRead == RecordReaderImpl.SargApplier.READ_NO_RGS,
          isAll = rgsToRead == RecordReaderImpl.SargApplier.READ_ALL_RGS;
      hasAnyData = hasAnyData || !isNone;
      if (LlapIoImpl.ORC_LOGGER.isTraceEnabled()) {
        if (isNone) {
          LlapIoImpl.ORC_LOGGER.trace("SARG eliminated all RGs for stripe {}", stripeIx);
          trace.logSargResult(stripeIx, 0);
        } else if (!isAll) {
          LlapIoImpl.ORC_LOGGER.trace("SARG picked RGs for stripe {}: {}",
              stripeIx, DebugUtils.toString(rgsToRead));
          trace.logSargResult(stripeIx, rgsToRead);
        } else {
          LlapIoImpl.ORC_LOGGER.trace("Will read all {} RGs for stripe {}", rgCount, stripeIx);
          trace.logSargResult(stripeIx, rgCount);
        }
      }
      assert isAll || isNone || rgsToRead.length == rgCount;
      stripeRgs[stripeIxMod] = (isAll || isNone) ? rgsToRead :
          Arrays.copyOf(rgsToRead, rgsToRead.length);
      adjustRgMetric(rgCount, rgsToRead, isNone, isAll);
    }
    return hasAnyData;
  }

  private void adjustRgMetric(int rgCount, boolean[] rgsToRead, boolean isNone,
      boolean isAll) {
    int count = 0;
    if (!isAll) {
      for (boolean b : rgsToRead) {
        if (b)
          count++;
      }
    } else if (!isNone) {
      count = rgCount;
    }
    counters.incrCounter(LlapIOCounters.SELECTED_ROWGROUPS, count);
  }


  private int getRgCount(StripeInformation stripe, int rowIndexStride) {
    return (int)Math.ceil((double)stripe.getNumberOfRows() / rowIndexStride);
  }

  /**
   * Determine which stripes to read for a split. Populates stripeIxFrom and stripeRgs.
   */
  public void determineStripesToRead() {
    // The unit of caching for ORC is (rg x column) (see OrcBatchKey).
    List<StripeInformation> stripes = fileMetadata.getStripes();
    long offset = split.getStart(), maxOffset = offset + split.getLength();
    stripeIxFrom = -1;
    int stripeIxTo = -1;
    if (LlapIoImpl.ORC_LOGGER.isDebugEnabled()) {
      String tmp = "FileSplit {" + split.getStart() + ", " + split.getLength() + "}; stripes ";
      for (StripeInformation stripe : stripes) {
        tmp += "{" + stripe.getOffset() + ", " + stripe.getLength() + "}, ";
      }
      LlapIoImpl.ORC_LOGGER.debug(tmp);
    }

    int stripeIx = 0;
    for (StripeInformation stripe : stripes) {
      long stripeStart = stripe.getOffset();
      if (offset > stripeStart) {
        // We assume splits will never start in the middle of the stripe.
        ++stripeIx;
        continue;
      }
      if (stripeIxFrom == -1) {
        LlapIoImpl.ORC_LOGGER.trace("Including stripes from {} ({} >= {})",
            stripeIx, stripeStart, offset);
        stripeIxFrom = stripeIx;
      }
      if (stripeStart >= maxOffset) {
        stripeIxTo = stripeIx;
        LlapIoImpl.ORC_LOGGER.trace("Including stripes until {} ({} >= {}); {} stripes",
            stripeIxTo, stripeStart, maxOffset, (stripeIxTo - stripeIxFrom));
        break;
      }
      ++stripeIx;
    }
    if (stripeIxFrom == -1) {
      LlapIoImpl.LOG.info("Not including any stripes - empty split");
    }
    if (stripeIxTo == -1 && stripeIxFrom != -1) {
      stripeIxTo = stripeIx;
      LlapIoImpl.ORC_LOGGER.trace("Including stripes until {} (end of file); {} stripes",
          stripeIx, (stripeIxTo - stripeIxFrom));
    }
    stripeRgs = new boolean[stripeIxTo - stripeIxFrom][];
  }

  private class DataWrapperForOrc implements DataReader, DataCache, BufferObjectFactory {
    /** A reference to parent DataReader not owned by this object. */
    private final DataReader orcDataReaderRef;

    public DataWrapperForOrc() throws IOException {
      ensureRawDataReader(false);
      this.orcDataReaderRef = rawDataReader;
    }

    @Override
    public CompressionCodec getCompressionCodec() {
      return orcDataReaderRef.getCompressionCodec();
    }

    @Override
    public DiskRangeList getFileData(Object fileKey, DiskRangeList range,
        long baseOffset, DiskRangeListFactory factory, BooleanRef gotAllData) {
      DiskRangeList result = lowLevelCache.getFileData(
          fileKey, range, baseOffset, factory, counters, gotAllData);
      if (LlapIoImpl.ORC_LOGGER.isTraceEnabled()) {
        LlapIoImpl.ORC_LOGGER.trace("Disk ranges after data cache (file " + fileKey +
            ", base offset " + baseOffset + "): " + RecordReaderUtils.stringifyDiskRanges(result));
        // TODO: trace ranges here? Between data cache and incomplete cb cache
      }
      if (gotAllData.value) return result;
      return (metadataCache == null) ? result
          : metadataCache.getIncompleteCbs(fileKey, result, baseOffset, gotAllData);
    }

    @Override
    public long[] putFileData(Object fileKey, DiskRange[] ranges,
        MemoryBuffer[] data, long baseOffset) {
      return putFileData(fileKey, ranges, data, baseOffset, null);
    }

    @Override
    public long[] putFileData(Object fileKey, DiskRange[] ranges,
        MemoryBuffer[] data, long baseOffset, String tag) {
      if (data != null) {
        return lowLevelCache.putFileData(
            fileKey, ranges, data, baseOffset, Priority.NORMAL, counters, tag);
      } else if (metadataCache != null) {
        metadataCache.putIncompleteCbs(fileKey, ranges, baseOffset);
      }
      return null;
    }

    @Override
    public void releaseBuffer(MemoryBuffer buffer) {
      bufferManager.decRefBuffer(buffer);
    }

    @Override
    public void reuseBuffer(MemoryBuffer buffer) {
      boolean isReused = bufferManager.incRefBuffer(buffer);
      assert isReused;
    }

    @Override
    public Allocator getAllocator() {
      return bufferManager.getAllocator();
    }

    @Override
    public void close() throws IOException {
      // Noop: orcDataReaderRef is owned by the parent object
    }

    @Override
    public DiskRangeList readFileData(DiskRangeList range, long baseOffset,
        boolean doForceDirect) throws IOException {
      long startTime = counters.startTimeCounter();
      DiskRangeList result = orcDataReaderRef.readFileData(range, baseOffset, doForceDirect);
      counters.recordHdfsTime(startTime);
      if (LlapIoImpl.ORC_LOGGER.isTraceEnabled()) {
        LlapIoImpl.ORC_LOGGER.trace("Disk ranges after disk read (file {}, base offset {}): {}",
            fileKey, baseOffset, RecordReaderUtils.stringifyDiskRanges(result));
      }
      trace.logRanges(fileKey, baseOffset, result, IoTrace.RangesSrc.DISK);
      return result;
    }

    @Override
    public boolean isTrackingDiskRanges() {
      return orcDataReaderRef.isTrackingDiskRanges();
    }

    @Override
    public void releaseBuffer(ByteBuffer buffer) {
      orcDataReaderRef.releaseBuffer(buffer);
    }

    @Override
    public DataWrapperForOrc clone() {
      throw new AssertionError("Clone not supported");
    }

    @Override
    public void open() throws IOException {
      long startTime = counters.startTimeCounter();
      orcDataReaderRef.open();
      counters.recordHdfsTime(startTime);
    }

    @Override
    public OrcIndex readRowIndex(StripeInformation stripe,
                                 TypeDescription fileSchema,
                                 OrcProto.StripeFooter footer,
                                 boolean ignoreNonUtf8BloomFilter,
                                 boolean[] included,
                                 OrcProto.RowIndex[] indexes,
                                 boolean[] sargColumns,
                                 org.apache.orc.OrcFile.WriterVersion version,
                                 OrcProto.Stream.Kind[] bloomFilterKinds,
                                 OrcProto.BloomFilterIndex[] bloomFilterIndices
                                 ) throws IOException {
      return orcDataReaderRef.readRowIndex(stripe, fileSchema, footer,
          ignoreNonUtf8BloomFilter, included, indexes,
          sargColumns, version, bloomFilterKinds, bloomFilterIndices);
    }

    @Override
    public OrcProto.StripeFooter readStripeFooter(StripeInformation stripe) throws IOException {
      return orcDataReaderRef.readStripeFooter(stripe);
    }

    @Override
    public BufferObjectFactory getDataBufferFactory() {
      return this;
    }

    @Override
    public MemoryBuffer create() {
      return new LlapDataBuffer();
    }
  }

  @Override
  public TezCounters getTezCounters() {
    return counters.getTezCounters();
  }

  public IoTrace getTrace() {
    return trace;
  }
}
