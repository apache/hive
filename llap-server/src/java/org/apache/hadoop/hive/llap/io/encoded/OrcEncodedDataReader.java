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
import java.util.List;

import org.apache.hadoop.hive.llap.counters.LlapIOCounters;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.DataReaderProperties;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.SchemaEvolution;
import org.apache.tez.common.counters.TezCounters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.Pool;
import org.apache.hadoop.hive.common.Pool.PoolObjectHelper;
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
import org.apache.hadoop.hive.llap.cache.BufferUsageManager;
import org.apache.hadoop.hive.llap.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.decode.OrcEncodedDataConsumer;
import org.apache.hadoop.hive.llap.io.metadata.OrcFileMetadata;
import org.apache.hadoop.hive.llap.io.metadata.OrcMetadataCache;
import org.apache.hadoop.hive.llap.io.metadata.OrcStripeMetadata;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HdfsUtils;
import org.apache.orc.CompressionKind;
import org.apache.orc.DataReader;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.ReaderOptions;
import org.apache.orc.OrcConf;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;
import org.apache.hadoop.hive.ql.io.orc.encoded.EncodedOrcFile;
import org.apache.hadoop.hive.ql.io.orc.encoded.EncodedReader;
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
    public <T> Pool<T> createPool(int size, PoolObjectHelper<T> helper) {
      return new FixedSizedObjectPool<>(size, helper);
    }

    @Override
    public Pool<ColumnStreamData> createColumnStreamDataPool() {
      return CSD_POOL;
    }

    @Override
    public Pool<OrcEncodedColumnBatch> createEncodedColumnBatchPool() {
      return ECB_POOL;
    }
  };

  private final OrcMetadataCache metadataCache;
  private final LowLevelCache lowLevelCache;
  private final BufferUsageManager bufferManager;
  private final Configuration daemonConf, jobConf;
  private final FileSplit split;
  private List<Integer> includedColumnIds;
  private final SearchArgument sarg;
  private final String[] columnNames;
  private final OrcEncodedDataConsumer consumer;
  private final QueryFragmentCounters counters;
  private final UserGroupInformation ugi;
  private final SchemaEvolution evolution;

  // Read state.
  private int stripeIxFrom;
  private OrcFileMetadata fileMetadata;
  private Path path;
  private Reader orcReader;
  private DataReader metadataReader;
  private EncodedReader stripeReader;
  private Object fileKey;
  private FileSystem fs;
  /**
   * readState[stripeIx'][colIx'] => boolean array (could be a bitmask) of rg-s that need to be
   * read. Contains only stripes that are read, and only columns included. null => read all RGs.
   */
  private boolean[][][] readState;
  private volatile boolean isStopped = false;
  @SuppressWarnings("unused")
  private volatile boolean isPaused = false;

  boolean[] globalIncludes = null;

  public OrcEncodedDataReader(LowLevelCache lowLevelCache, BufferUsageManager bufferManager,
      OrcMetadataCache metadataCache, Configuration daemonConf, Configuration jobConf,
      FileSplit split, List<Integer> columnIds, SearchArgument sarg, String[] columnNames,
      OrcEncodedDataConsumer consumer, QueryFragmentCounters counters,
      TypeDescription readerSchema) throws IOException {
    this.lowLevelCache = lowLevelCache;
    this.metadataCache = metadataCache;
    this.bufferManager = bufferManager;
    this.daemonConf = daemonConf;
    this.split = split;
    this.includedColumnIds = columnIds;
    if (this.includedColumnIds != null) {
      Collections.sort(this.includedColumnIds);
    }
    this.sarg = sarg;
    this.columnNames = columnNames;
    this.consumer = consumer;
    this.counters = counters;
    try {
      this.ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // moved this part of code from performDataRead as LlapInputFormat need to know the file schema
    // to decide if schema evolution is supported or not
    orcReader = null;
    // 1. Get file metadata from cache, or create the reader and read it.
    // Don't cache the filesystem object for now; Tez closes it and FS cache will fix all that
    fs = split.getPath().getFileSystem(jobConf);
    fileKey = determineFileId(fs, split,
        HiveConf.getBoolVar(daemonConf, ConfVars.LLAP_CACHE_ALLOW_SYNTHETIC_FILEID));
    fileMetadata = getOrReadFileMetadata();
    if (readerSchema == null) {
      readerSchema = fileMetadata.getSchema();
    }
    globalIncludes = OrcInputFormat.genIncludedColumns(readerSchema, includedColumnIds);
    // Do not allow users to override zero-copy setting. The rest can be taken from user config.
    boolean useZeroCopy = OrcConf.USE_ZEROCOPY.getBoolean(daemonConf);
    if (useZeroCopy != OrcConf.USE_ZEROCOPY.getBoolean(jobConf)) {
      jobConf = new Configuration(jobConf);
      jobConf.setBoolean(OrcConf.USE_ZEROCOPY.getAttribute(), useZeroCopy);
    }
    this.jobConf = jobConf;
    Reader.Options options = new Reader.Options(jobConf).include(globalIncludes);
    evolution = new SchemaEvolution(fileMetadata.getSchema(), readerSchema, options);
    consumer.setFileMetadata(fileMetadata);
    consumer.setIncludedColumns(globalIncludes);
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

  protected Void performDataRead() throws IOException {
    long startTime = counters.startTimeCounter();
    LlapIoImpl.LOG.info("Processing data for {}", split.getPath());
    if (processStop()) {
      recordReaderTime(startTime);
      return null;
    }
    counters.setDesc(QueryFragmentCounters.Desc.TABLE, getDbAndTableName(split.getPath()));
    counters.setDesc(QueryFragmentCounters.Desc.FILE, split.getPath()
        + (fileKey == null ? "" : " (" + fileKey + ")"));
    try {
      validateFileMetadata();
      if (includedColumnIds == null) {
        includedColumnIds = getAllColumnIds(fileMetadata);
      }

      // 2. Determine which stripes to read based on the split.
      determineStripesToRead();
    } catch (Throwable t) {
      recordReaderTime(startTime);
      consumer.setError(t);
      return null;
    }

    if (readState.length == 0) {
      consumer.setDone();
      recordReaderTime(startTime);
      return null; // No data to read.
    }
    counters.setDesc(QueryFragmentCounters.Desc.STRIPES, stripeIxFrom + "," + readState.length);

    // 3. Apply SARG if needed, and otherwise determine what RGs to read.
    int stride = fileMetadata.getRowIndexStride();
    ArrayList<OrcStripeMetadata> stripeMetadatas = null;
    boolean[] sargColumns = null;
    try {
      if (sarg != null && stride != 0) {
        // TODO: move this to a common method
        int[] filterColumns = RecordReaderImpl.mapSargColumnsToOrcInternalColIdx(
          sarg.getLeaves(), evolution);
        // included will not be null, row options will fill the array with trues if null
        sargColumns = new boolean[globalIncludes.length];
        for (int i : filterColumns) {
          // filter columns may have -1 as index which could be partition column in SARG.
          if (i > 0) {
            sargColumns[i] = true;
          }
        }

        // If SARG is present, get relevant stripe metadata from cache or readers.
        stripeMetadatas = readStripesMetadata(globalIncludes, sargColumns);
      }

      // Now, apply SARG if any; w/o sarg, this will just initialize readState.
      boolean hasData = determineRgsToRead(globalIncludes, stride, stripeMetadatas);
      if (!hasData) {
        consumer.setDone();
        recordReaderTime(startTime);
        return null; // No data to read.
      }
    } catch (Throwable t) {
      cleanupReaders();
      consumer.setError(t);
      recordReaderTime(startTime);
      return null;
    }

    if (processStop()) {
      recordReaderTime(startTime);
      return null;
    }

    // 4. Create encoded data reader.
    try {
      ensureOrcReader();
      // Reader creating updates HDFS counters, don't do it here.
      DataWrapperForOrc dw = new DataWrapperForOrc();
      stripeReader = orcReader.encodedReader(fileKey, dw, dw, POOL_FACTORY);
      stripeReader.setTracing(LlapIoImpl.ORC_LOGGER.isTraceEnabled());
    } catch (Throwable t) {
      consumer.setError(t);
      recordReaderTime(startTime);
      cleanupReaders();
      return null;
    }

    // 6. Read data.
    // TODO: I/O threadpool could be here - one thread per stripe; for now, linear.
    boolean hasFileId = this.fileKey != null;
    OrcBatchKey stripeKey = hasFileId ? new OrcBatchKey(fileKey, -1, 0) : null;
    for (int stripeIxMod = 0; stripeIxMod < readState.length; ++stripeIxMod) {
      if (processStop()) {
        recordReaderTime(startTime);
        return null;
      }
      int stripeIx = stripeIxFrom + stripeIxMod;
      boolean[][] colRgs = null;
      OrcStripeMetadata stripeMetadata = null;
      StripeInformation stripe;
      try {
        stripe = fileMetadata.getStripes().get(stripeIx);

        LlapIoImpl.ORC_LOGGER.trace("Reading stripe {}: {}, {}", stripeIx, stripe.getOffset(),
            stripe.getLength());
        colRgs = readState[stripeIxMod];
        if (LlapIoImpl.ORC_LOGGER.isTraceEnabled()) {
          LlapIoImpl.ORC_LOGGER.trace("readState[{}]: {}", stripeIxMod, Arrays.toString(colRgs));
        }
        // We assume that NO_RGS value is only set from SARG filter and for all columns;
        // intermediate changes for individual columns will unset values in the array.
        // Skip this case for 0-column read. We could probably special-case it just like we do
        // in EncodedReaderImpl, but for now it's not that important.
        if (colRgs.length > 0 && colRgs[0] ==
            RecordReaderImpl.SargApplier.READ_NO_RGS) continue;

        // 6.2. Ensure we have stripe metadata. We might have read it before for RG filtering.
        boolean isFoundInCache = false;
        if (stripeMetadatas != null) {
          stripeMetadata = stripeMetadatas.get(stripeIxMod);
        } else {
          if (hasFileId && metadataCache != null) {
            stripeKey.stripeIx = stripeIx;
            stripeMetadata = metadataCache.getStripeMetadata(stripeKey);
          }
          isFoundInCache = (stripeMetadata != null);
          if (!isFoundInCache) {
            counters.incrCounter(LlapIOCounters.METADATA_CACHE_MISS);
            ensureMetadataReader();
            long startTimeHdfs = counters.startTimeCounter();
            stripeMetadata = new OrcStripeMetadata(new OrcBatchKey(fileKey, stripeIx, 0),
                metadataReader, stripe, globalIncludes, sargColumns,
                orcReader.getSchema(), orcReader.getWriterVersion());
            counters.incrTimeCounter(LlapIOCounters.HDFS_TIME_NS, startTimeHdfs);
            if (hasFileId && metadataCache != null) {
              stripeMetadata = metadataCache.putStripeMetadata(stripeMetadata);
              if (LlapIoImpl.ORC_LOGGER.isTraceEnabled()) {
                LlapIoImpl.ORC_LOGGER.trace("Caching stripe {} metadata with includes: {}",
                    stripeKey.stripeIx, DebugUtils.toString(globalIncludes));
              }
            }
          }
          consumer.setStripeMetadata(stripeMetadata);
        }
        if (!stripeMetadata.hasAllIndexes(globalIncludes)) {
          if (LlapIoImpl.ORC_LOGGER.isTraceEnabled()) {
            LlapIoImpl.ORC_LOGGER.trace("Updating indexes in stripe {} metadata for includes: {}",
                stripeKey.stripeIx, DebugUtils.toString(globalIncludes));
          }
          assert isFoundInCache;
          counters.incrCounter(LlapIOCounters.METADATA_CACHE_MISS);
          ensureMetadataReader();
          updateLoadedIndexes(stripeMetadata, stripe, globalIncludes, sargColumns);
        } else if (isFoundInCache) {
          counters.incrCounter(LlapIOCounters.METADATA_CACHE_HIT);
        }
      } catch (Throwable t) {
        consumer.setError(t);
        cleanupReaders();
        recordReaderTime(startTime);
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
        stripeReader.readEncodedColumns(stripeIx, stripe, stripeMetadata.getRowIndexes(),
            stripeMetadata.getEncodings(), stripeMetadata.getStreams(), globalIncludes,
            colRgs, consumer);
      } catch (Throwable t) {
        consumer.setError(t);
        cleanupReaders();
        recordReaderTime(startTime);
        return null;
      }
    }

    // Done with all the things.
    recordReaderTime(startTime);
    consumer.setDone();

    LlapIoImpl.LOG.trace("done processing {}", split);

    // Close the stripe reader, we are done reading.
    cleanupReaders();
    return null;
  }

  private void recordReaderTime(long startTime) {
    counters.incrTimeCounter(LlapIOCounters.TOTAL_IO_TIME_NS, startTime);
  }

  private static String getDbAndTableName(Path path) {
    // Ideally, we'd get this from split; however, split doesn't contain any such thing and it's
    // actually pretty hard to get cause even split generator only uses paths. We only need this
    // for metrics; therefore, brace for BLACK MAGIC!
    String[] parts = path.toUri().getPath().toString().split(Path.SEPARATOR);
    int dbIx = -1;
    // Try to find the default db postfix; don't check two last components - at least there
    // should be a table and file (we could also try to throw away partition/bucket/acid stuff).
    for (int i = 0; i < parts.length - 2; ++i) {
      if (!parts[i].endsWith(DDLTask.DATABASE_PATH_SUFFIX)) continue;
      if (dbIx >= 0) {
        dbIx = -1; // Let's not guess.
        break;
      }
      dbIx = i;
    }
    if (dbIx >= 0) {
      return parts[dbIx].substring(0, parts[dbIx].length() - 3) + "." + parts[dbIx + 1];
    }

    // Just go from the back and throw away everything we think is wrong; skip last item, the file.
    boolean isInPartFields = false;
    for (int i = parts.length - 2; i >= 0; --i) {
      String p = parts[i];
      boolean isPartField = p.contains("=");
      if ((isInPartFields && !isPartField) || (!isPartField && !p.startsWith(AcidUtils.BASE_PREFIX)
          && !p.startsWith(AcidUtils.DELTA_PREFIX) && !p.startsWith(AcidUtils.BUCKET_PREFIX))) {
        dbIx = i - 1;
        break;
      }
      isInPartFields = isPartField;
    }
    // If we found something before we ran out of components, use it.
    if (dbIx >= 0) {
      String dbName = parts[dbIx];
      if (dbName.endsWith(DDLTask.DATABASE_PATH_SUFFIX)) {
        dbName = dbName.substring(0, dbName.length() - 3);
      }
      return dbName + "." + parts[dbIx + 1];
    }
    return "unknown";
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
    cleanupReaders();
    return true;
  }

  private static Object determineFileId(FileSystem fs, FileSplit split,
      boolean allowSynthetic) throws IOException {
    if (split instanceof OrcSplit) {
      Object fileKey = ((OrcSplit)split).getFileKey();
      if (fileKey != null) {
        return fileKey;
      }
    }
    LOG.warn("Split for " + split.getPath() + " (" + split.getClass() + ") does not have file ID");
    return HdfsUtils.getFileId(fs, split.getPath(), allowSynthetic);
  }

  /**
   * Puts all column indexes from metadata to make a column list to read all column.
   */
  private static List<Integer> getAllColumnIds(OrcFileMetadata metadata) {
    int rootColumn = OrcInputFormat.getRootColumn(true);
    List<Integer> types = metadata.getTypes().get(rootColumn).getSubtypesList();
    List<Integer> columnIds = new ArrayList<Integer>(types.size());
    for (int i = 0; i < types.size(); ++i) {
      columnIds.add(i);
    }
    return columnIds;
  }

  /**
   * In case if stripe metadata in cache does not have all indexes for current query, load
   * the missing one. This is a temporary cludge until real metadata cache becomes available.
   */
  private void updateLoadedIndexes(OrcStripeMetadata stripeMetadata,
      StripeInformation stripe, boolean[] stripeIncludes, boolean[] sargColumns) throws IOException {
    // We only synchronize on write for now - design of metadata cache is very temporary;
    // we pre-allocate the array and never remove entries; so readers should be safe.
    synchronized (stripeMetadata) {
      if (stripeMetadata.hasAllIndexes(stripeIncludes)) return;
      long startTime = counters.startTimeCounter();
      stripeMetadata.loadMissingIndexes(metadataReader, stripe, stripeIncludes, sargColumns);
      counters.incrTimeCounter(LlapIOCounters.HDFS_TIME_NS, startTime);
    }
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
    if (metadataReader != null) {
      try {
        metadataReader.close();
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
   *  Gets file metadata for the split from cache, or reads it from the file.
   */
  private OrcFileMetadata getOrReadFileMetadata() throws IOException {
    OrcFileMetadata metadata = null;
    if (fileKey != null && metadataCache != null) {
      metadata = metadataCache.getFileMetadata(fileKey);
      if (metadata != null) {
        counters.incrCounter(LlapIOCounters.METADATA_CACHE_HIT);
        return metadata;
      } else {
        counters.incrCounter(LlapIOCounters.METADATA_CACHE_MISS);
      }
    }
    ensureOrcReader();
    // We assume this call doesn't touch HDFS because everything is already read; don't add time.
    metadata = new OrcFileMetadata(fileKey, orcReader);
    if (fileKey == null || metadataCache == null) return metadata;
    return metadataCache.putFileMetadata(metadata);
  }

  /**
   * Reads the metadata for all stripes in the file.
   */
  private ArrayList<OrcStripeMetadata> readStripesMetadata(
      boolean[] globalInc, boolean[] sargColumns) throws IOException {
    ArrayList<OrcStripeMetadata> result = new ArrayList<OrcStripeMetadata>(readState.length);
    boolean hasFileId = this.fileKey != null;
    OrcBatchKey stripeKey = hasFileId ? new OrcBatchKey(fileKey, 0, 0) : null;
    for (int stripeIxMod = 0; stripeIxMod < readState.length; ++stripeIxMod) {
      OrcStripeMetadata value = null;
      int stripeIx = stripeIxMod + stripeIxFrom;
      if (hasFileId && metadataCache != null) {
        stripeKey.stripeIx = stripeIx;
        value = metadataCache.getStripeMetadata(stripeKey);
      }
      if (value == null || !value.hasAllIndexes(globalInc)) {
        counters.incrCounter(LlapIOCounters.METADATA_CACHE_MISS);
        ensureMetadataReader();
        StripeInformation si = fileMetadata.getStripes().get(stripeIx);
        if (value == null) {
          long startTime = counters.startTimeCounter();
          value = new OrcStripeMetadata(new OrcBatchKey(fileKey, stripeIx, 0),
              metadataReader, si, globalInc, sargColumns, orcReader.getSchema(),
              orcReader.getWriterVersion());
          counters.incrTimeCounter(LlapIOCounters.HDFS_TIME_NS, startTime);
          if (hasFileId && metadataCache != null) {
            value = metadataCache.putStripeMetadata(value);
            if (LlapIoImpl.ORC_LOGGER.isTraceEnabled()) {
              LlapIoImpl.ORC_LOGGER.trace("Caching stripe {} metadata with includes: {}",
                  stripeKey.stripeIx, DebugUtils.toString(globalInc));
            }
          }
        }
        // We might have got an old value from cache; recheck it has indexes.
        if (!value.hasAllIndexes(globalInc)) {
          if (LlapIoImpl.ORC_LOGGER.isTraceEnabled()) {
            LlapIoImpl.ORC_LOGGER.trace("Updating indexes in stripe {} metadata for includes: {}",
                stripeKey.stripeIx, DebugUtils.toString(globalInc));
          }
          updateLoadedIndexes(value, si, globalInc, sargColumns);
        }
      } else {
        counters.incrCounter(LlapIOCounters.METADATA_CACHE_HIT);
      }
      result.add(value);
      consumer.setStripeMetadata(value);
    }
    return result;
  }

  private void ensureMetadataReader() throws IOException {
    ensureOrcReader();
    if (metadataReader != null) return;
    long startTime = counters.startTimeCounter();
    boolean useZeroCopy = (daemonConf != null) && OrcConf.USE_ZEROCOPY.getBoolean(daemonConf);
    metadataReader = RecordReaderUtils.createDefaultDataReader(
        DataReaderProperties.builder()
        .withBufferSize(orcReader.getCompressionSize())
        .withCompression(orcReader.getCompressionKind())
        .withFileSystem(fs)
        .withPath(path)
        .withTypeCount(orcReader.getSchema().getMaximumId() + 1)
        .withZeroCopy(useZeroCopy)
        .build());
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
        CSD_POOL.offer(data);
      }
    }
    // We can offer ECB even with some streams not discarded; reset() will clear the arrays.
    ECB_POOL.offer(ecb);
  }

  /**
   * Determines which RGs need to be read, after stripes have been determined.
   * SARG is applied, and readState is populated for each stripe accordingly.
   */
  private boolean determineRgsToRead(boolean[] globalIncludes, int rowIndexStride,
      ArrayList<OrcStripeMetadata> metadata) throws IOException {
    RecordReaderImpl.SargApplier sargApp = null;
    if (sarg != null && rowIndexStride != 0) {
      sargApp = new RecordReaderImpl.SargApplier(sarg,
          rowIndexStride, evolution,
          OrcFile.WriterVersion.from(fileMetadata.getWriterVersionNum()));
    }
    boolean hasAnyData = false;
    // readState should have been initialized by this time with an empty array.
    for (int stripeIxMod = 0; stripeIxMod < readState.length; ++stripeIxMod) {
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
        } else if (!isAll) {
          LlapIoImpl.ORC_LOGGER.trace("SARG picked RGs for stripe {}: {}",
              stripeIx, DebugUtils.toString(rgsToRead));
        } else {
          LlapIoImpl.ORC_LOGGER.trace("Will read all {} RGs for stripe {}", rgCount, stripeIx);
        }
      }
      assert isAll || isNone || rgsToRead.length == rgCount;
      int fileIncludesCount = 0;
      // TODO: hacky for now - skip the root 0-s column.
      //        We don't need separate readState w/o HL cache, should get rid of that instead.
      for (int includeIx = 1; includeIx < globalIncludes.length; ++includeIx) {
        fileIncludesCount += (globalIncludes[includeIx] ? 1 : 0);
      }
      readState[stripeIxMod] = new boolean[fileIncludesCount][];
      for (int includeIx = 0; includeIx < fileIncludesCount; ++includeIx) {
        readState[stripeIxMod][includeIx] = (isAll || isNone) ? rgsToRead :
          Arrays.copyOf(rgsToRead, rgsToRead.length);
      }
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
   * Determine which stripes to read for a split. Populates stripeIxFrom and readState.
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
    readState = new boolean[stripeIxTo - stripeIxFrom][][];
  }

  private class DataWrapperForOrc implements DataReader, DataCache {
    private final DataReader orcDataReader;

    private DataWrapperForOrc(DataWrapperForOrc other) {
      orcDataReader = other.orcDataReader.clone();
    }

    public DataWrapperForOrc() throws IOException {
      ensureMetadataReader();
      this.orcDataReader = metadataReader.clone();
    }

    @Override
    public DiskRangeList getFileData(Object fileKey, DiskRangeList range,
        long baseOffset, DiskRangeListFactory factory, BooleanRef gotAllData) {
      DiskRangeList result = lowLevelCache.getFileData(
          fileKey, range, baseOffset, factory, counters, gotAllData);
      if (LlapIoImpl.ORC_LOGGER.isTraceEnabled()) {
        LlapIoImpl.ORC_LOGGER.trace("Disk ranges after data cache (file " + fileKey +
            ", base offset " + baseOffset + "): " + RecordReaderUtils.stringifyDiskRanges(result));
      }
      if (gotAllData.value) return result;
      return (metadataCache == null) ? result
          : metadataCache.getIncompleteCbs(fileKey, result, baseOffset, factory, gotAllData);
    }

    @Override
    public long[] putFileData(Object fileKey, DiskRange[] ranges,
        MemoryBuffer[] data, long baseOffset) {
      if (data != null) {
        return lowLevelCache.putFileData(
            fileKey, ranges, data, baseOffset, Priority.NORMAL, counters);
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
      orcDataReader.close();
      if (metadataReader != null) {
        metadataReader.close();
      }
    }

    @Override
    public DiskRangeList readFileData(DiskRangeList range, long baseOffset,
        boolean doForceDirect) throws IOException {
      long startTime = counters.startTimeCounter();
      DiskRangeList result = orcDataReader.readFileData(range, baseOffset, doForceDirect);
      counters.recordHdfsTime(startTime);
      if (LlapIoImpl.ORC_LOGGER.isTraceEnabled()) {
        LlapIoImpl.ORC_LOGGER.trace("Disk ranges after disk read (file {}, base offset {}): {}",
            fileKey, baseOffset, RecordReaderUtils.stringifyDiskRanges(result));
      }
      return result;
    }

    @Override
    public boolean isTrackingDiskRanges() {
      return orcDataReader.isTrackingDiskRanges();
    }

    @Override
    public void releaseBuffer(ByteBuffer buffer) {
      orcDataReader.releaseBuffer(buffer);
    }

    @Override
    public DataWrapperForOrc clone() {
      return new DataWrapperForOrc(this);
    }

    @Override
    public void open() throws IOException {
      long startTime = counters.startTimeCounter();
      orcDataReader.open();
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
      return orcDataReader.readRowIndex(stripe, fileSchema, footer,
          ignoreNonUtf8BloomFilter, included, indexes,
          sargColumns, version, bloomFilterKinds, bloomFilterIndices);
    }

    @Override
    public OrcProto.StripeFooter readStripeFooter(StripeInformation stripe) throws IOException {
      return orcDataReader.readStripeFooter(stripe);
    }
  }

  @Override
  public TezCounters getTezCounters() {
    return counters.getTezCounters();
  }
}
