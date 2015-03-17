package org.apache.hadoop.hive.llap.io.encoded;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.common.CallableWithNdc;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.Consumer;
import org.apache.hadoop.hive.llap.ConsumerFeedback;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.cache.Cache;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch;
import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch.StreamBuffer;
import org.apache.hadoop.hive.llap.io.api.cache.LlapMemoryBuffer;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.api.orc.OrcBatchKey;
import org.apache.hadoop.hive.llap.io.api.orc.OrcCacheKey;
import org.apache.hadoop.hive.llap.io.decode.OrcEncodedDataConsumer;
import org.apache.hadoop.hive.llap.io.metadata.OrcFileMetadata;
import org.apache.hadoop.hive.llap.io.metadata.OrcMetadataCache;
import org.apache.hadoop.hive.llap.io.metadata.OrcStripeMetadata;
import org.apache.hadoop.hive.ql.io.orc.EncodedReader;
import org.apache.hadoop.hive.ql.io.orc.MetadataReader;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.ReaderOptions;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.SargApplier;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderUtils;
import org.apache.hadoop.hive.ql.io.orc.StripeInformation;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

public class OrcEncodedDataReader extends CallableWithNdc<Void>
    implements ConsumerFeedback<StreamBuffer>, Consumer<EncodedColumnBatch<OrcBatchKey>> {

  private final OrcMetadataCache metadataCache;
  private final LowLevelCache lowLevelCache;
  private final Configuration conf;
  private final Cache<OrcCacheKey> cache;
  private final FileSplit split;
  private List<Integer> columnIds;
  private final SearchArgument sarg;
  private final String[] columnNames;
  private final OrcEncodedDataConsumer consumer;
  private final QueryFragmentCounters counters;

  // Read state.
  private int stripeIxFrom;
  private OrcFileMetadata fileMetadata;
  private Reader orcReader;
  private MetadataReader metadataReader;
  private Long fileId;
  private FileSystem fs;
  /**
   * readState[stripeIx'][colIx'] => boolean array (could be a bitmask) of rg-s that need to be
   * read. Contains only stripes that are read, and only columns included. null => read all RGs.
   */
  private boolean[][][] readState;
  private boolean isStopped = false, isPaused = false;

  public OrcEncodedDataReader(LowLevelCache lowLevelCache, Cache<OrcCacheKey> cache,
      OrcMetadataCache metadataCache, Configuration conf, InputSplit split,
      List<Integer> columnIds, SearchArgument sarg, String[] columnNames,
      OrcEncodedDataConsumer consumer, QueryFragmentCounters counters) {
    this.lowLevelCache = lowLevelCache;
    this.metadataCache = metadataCache;
    this.cache = cache;
    this.conf = conf;
    this.split = (FileSplit)split;
    this.columnIds = columnIds;
    if (this.columnIds != null) {
      Collections.sort(this.columnIds);
    }
    this.sarg = sarg;
    this.columnNames = columnNames;
    this.consumer = consumer;
    this.counters = counters;
  }

  @Override
  public void stop() {
    isStopped = true;
    // TODO: stop fetching if still in progress
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
  protected Void callInternal() throws IOException {
    if (LlapIoImpl.LOGL.isInfoEnabled()) {
      LlapIoImpl.LOG.info("Processing split for " + split.getPath());
    }
    if (isStopped) return null;
    orcReader = null;
    // 1. Get file metadata from cache, or create the reader and read it.
    // Disable filesystem caching for now; Tez closes it and FS cache will fix all that
    fs = split.getPath().getFileSystem(conf);
    this.fileId = RecordReaderUtils.getFileId(fs, split.getPath());
    try {
      fileMetadata = getOrReadFileMetadata();
      consumer.setFileMetadata(fileMetadata);
      int bufferSize = fileMetadata.getCompressionBufferSize();
      int minAllocSize = HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_ORC_CACHE_MIN_ALLOC);
      if (bufferSize < minAllocSize) {
        throw new IOException("ORC compression buffer size (" + bufferSize + ") is smaller than" +
            " LLAP low-level cache minimum allocation size (" + minAllocSize + "). Decrease the" +
            " value for " + HiveConf.ConfVars.LLAP_ORC_CACHE_MIN_ALLOC.toString());
      }
      // TODO#: HERE 1
      if (columnIds == null) {
        columnIds = createColumnIds(fileMetadata);
      }

      // 2. Determine which stripes to read based on the split.
      determineStripesToRead();
    } catch (Throwable t) {
      consumer.setError(t);
      return null;
    }

    if (readState.length == 0) {
      consumer.setDone();
      return null; // No data to read.
    }

    // 3. Apply SARG if needed, and otherwise determine what RGs to read.
    int stride = fileMetadata.getRowIndexStride();
    ArrayList<OrcStripeMetadata> stripeMetadatas = null;
    boolean[] globalIncludes = null;
    boolean[] sargColumns = null;
    try {
      globalIncludes = OrcInputFormat.genIncludedColumns(fileMetadata.getTypes(), columnIds, true);
      if (sarg != null && stride != 0) {
        // TODO: move this to a common method
        int[] filterColumns = RecordReaderImpl.mapSargColumns(sarg.getLeaves(), columnNames, 0);
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
      determineRgsToRead(globalIncludes, stride, stripeMetadatas);
    } catch (Throwable t) {
      cleanupReaders(null);
      consumer.setError(t);
      return null;
    }

    if (isStopped) {
      cleanupReaders(null);
      return null;
    }

    // 4. Get data from high-level cache.
    //    If some cols are fully in cache, this will also give us the modified list of
    //    columns to read for every stripe (null means read all of them - the usual path).
    List<Integer>[] stripeColsToRead = null;
    if (cache != null) {
      try {
        stripeColsToRead = produceDataFromCache(stride);
      } catch (Throwable t) {
        // produceDataFromCache handles its own cleanup.
        consumer.setError(t);
        cleanupReaders(null);
        return null;
      }
    }
    // readState has been modified for column x rgs that were fetched from cache.

    // 5. Create encoded data reader.
    // In case if we have high-level cache, we will intercept the data and add it there;
    // otherwise just pass the data directly to the consumer.
    Consumer<EncodedColumnBatch<OrcBatchKey>> dataConsumer =
        (cache == null) ? this.consumer : this;
    EncodedReader stripeReader = null;
    try {
      ensureOrcReader();
      stripeReader = orcReader.encodedReader(lowLevelCache, dataConsumer);
    } catch (Throwable t) {
      consumer.setError(t);
      cleanupReaders(null);
      return null;
    }

    // 6. Read data.
    // TODO: I/O threadpool could be here - one thread per stripe; for now, linear.
    OrcBatchKey stripeKey = new OrcBatchKey(fileId, -1, 0);
    for (int stripeIxMod = 0; stripeIxMod < readState.length; ++stripeIxMod) {
      try {
        List<Integer> cols = stripeColsToRead == null ? null : stripeColsToRead[stripeIxMod];
        if (cols != null && cols.isEmpty()) continue; // No need to read this stripe.
        int stripeIx = stripeIxFrom + stripeIxMod;
        StripeInformation si = fileMetadata.getStripes().get(stripeIx);

        if (DebugUtils.isTraceOrcEnabled()) {
          LlapIoImpl.LOG.info("Reading stripe " + stripeIx + ": "
              + si.getOffset() + ", " + si.getLength());
        }
        boolean[] stripeIncludes = null;
        boolean[][] colRgs = readState[stripeIxMod];

        // 6.1. Determine the columns to read (usually the same as requested).
        if (cols == null || cols.size() == colRgs.length) {
          cols = columnIds;
          stripeIncludes = globalIncludes;
        } else {
          // We are reading subset of the original columns, remove unnecessary bitmasks/etc.
          // This will never happen w/o high-level cache.
          stripeIncludes = OrcInputFormat.genIncludedColumns(fileMetadata.getTypes(), cols, true);
          colRgs = genStripeColRgs(cols, colRgs);
        }

        // 6.2. Ensure we have stripe metadata. We might have read it before for RG filtering.
        OrcStripeMetadata stripeMetadata;
        if (stripeMetadatas != null) {
          stripeMetadata = stripeMetadatas.get(stripeIxMod);
        } else {
          stripeKey.stripeIx = stripeIx;
          stripeMetadata = metadataCache.getStripeMetadata(stripeKey);
          if (stripeMetadata == null) {
            ensureMetadataReader();
            stripeMetadata = metadataCache.putStripeMetadata(new OrcStripeMetadata(
                stripeKey, metadataReader, si, stripeIncludes, sargColumns));
            if (DebugUtils.isTraceOrcEnabled()) {
              LlapIoImpl.LOG.info("Caching stripe " + stripeKey.stripeIx
                  + " metadata with includes: " + DebugUtils.toString(stripeIncludes));
            }            
            stripeKey = new OrcBatchKey(fileId, -1, 0);
          }
          consumer.setStripeMetadata(stripeMetadata);
        }
        if (!stripeMetadata.hasAllIndexes(stripeIncludes)) {
          if (DebugUtils.isTraceOrcEnabled()) {
            LlapIoImpl.LOG.info("Updating indexes in stripe " + stripeKey.stripeIx
                + " metadata for includes: " + DebugUtils.toString(stripeIncludes));
          }
          ensureMetadataReader();
          updateLoadedIndexes(stripeMetadata, si, stripeIncludes, sargColumns);
        }
        // 6.3. Finally, hand off to the stripe reader to produce the data.
        //      This is a sync call that will feed data to the consumer.
        // TODO: readEncodedColumns is not supposed to throw; errors should be propagated thru
        // consumer. It is potentially holding locked buffers, and must perform its own cleanup.
        stripeReader.readEncodedColumns(stripeIx, si, stripeMetadata.getRowIndexes(),
            stripeMetadata.getEncodings(), stripeMetadata.getStreams(), stripeIncludes, colRgs);
      } catch (Throwable t) {
        consumer.setError(t);
        cleanupReaders(stripeReader);
        return null;
      }
    }

    // Done with all the things.
    dataConsumer.setDone();
    if (DebugUtils.isTraceMttEnabled()) {
      LlapIoImpl.LOG.info("done processing " + split);
    }

    // Close the stripe reader, we are done reading.
    stripeReader.close();
    return null;
  }

  private boolean[][] genStripeColRgs(List<Integer> stripeCols, boolean[][] globalColRgs) {
    boolean[][] stripeColRgs = new boolean[stripeCols.size()][];
    for (int i = 0, i2 = -1; i < globalColRgs.length; ++i) {
      if (globalColRgs[i] == null) continue;
      stripeColRgs[i2] = globalColRgs[i];
      ++i2;
    }
    return stripeColRgs;
  }

  /**
   * Puts all column indexes from metadata to make a column list to read all column.
   */
  private static List<Integer> createColumnIds(OrcFileMetadata metadata) {
    List<Integer> columnIds = new ArrayList<Integer>(metadata.getTypes().size());
    for (int i = 1; i < metadata.getTypes().size(); ++i) {
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
      stripeMetadata.loadMissingIndexes(metadataReader, stripe, stripeIncludes, sargColumns);
    }
  }

  /**
   * Closes the stripe readers (on error).
   */
  private void cleanupReaders(EncodedReader er) {
    if (metadataReader != null) {
      try {
        metadataReader.close();
      } catch (IOException ex) {
        // Ignore.
      }
    }
    if (er != null) {
      try {
        er.close();
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
    ReaderOptions opts = OrcFile.readerOptions(conf).filesystem(fs).fileMetadata(fileMetadata);
    orcReader = OrcFile.createReader(split.getPath(), opts);
  }

  /**
   *  Gets file metadata for the split from cache, or reads it from the file.
   */
  private OrcFileMetadata getOrReadFileMetadata() throws IOException {
    OrcFileMetadata metadata = metadataCache.getFileMetadata(fileId);
    if (metadata != null) return metadata;
    ensureOrcReader();
    metadata = new OrcFileMetadata(fileId, orcReader);
    return metadataCache.putFileMetadata(metadata);
  }

  /**
   * Reads the metadata for all stripes in the file.
   */
  private ArrayList<OrcStripeMetadata> readStripesMetadata(
      boolean[] globalInc, boolean[] sargColumns) throws IOException {
    ArrayList<OrcStripeMetadata> result = new ArrayList<OrcStripeMetadata>(readState.length);
    OrcBatchKey stripeKey = new OrcBatchKey(fileId, 0, 0);
    for (int stripeIxMod = 0; stripeIxMod < readState.length; ++stripeIxMod) {
      stripeKey.stripeIx = stripeIxMod + stripeIxFrom;
      OrcStripeMetadata value = metadataCache.getStripeMetadata(stripeKey);
      if (value == null || !value.hasAllIndexes(globalInc)) {
        ensureMetadataReader();
        StripeInformation si = fileMetadata.getStripes().get(stripeKey.stripeIx);
        if (value == null) {
          value = metadataCache.putStripeMetadata(
              new OrcStripeMetadata(stripeKey, metadataReader, si, globalInc, sargColumns));
          if (DebugUtils.isTraceOrcEnabled()) {
            LlapIoImpl.LOG.info("Caching stripe " + stripeKey.stripeIx
                + " metadata with includes: " + DebugUtils.toString(globalInc));
          }
          // Create new key object to reuse for gets; we've used the old one to put in cache.
          stripeKey = new OrcBatchKey(fileId, 0, 0);
        } else {
          if (DebugUtils.isTraceOrcEnabled()) {
            LlapIoImpl.LOG.info("Updating indexes in stripe " + stripeKey.stripeIx
                + " metadata for includes: " + DebugUtils.toString(globalInc));
          }
          updateLoadedIndexes(value, si, globalInc, sargColumns);
        }
      }
      result.add(value);
      consumer.setStripeMetadata(value);
    }
    return result;
  }

  private void ensureMetadataReader() throws IOException {
    ensureOrcReader();
    if (metadataReader != null) return;
    metadataReader = orcReader.metadata();
  }

  @Override
  public void returnData(StreamBuffer data) {
    if (data.decRef() != 0) return;
    if (DebugUtils.isTraceLockingEnabled()) {
      for (LlapMemoryBuffer buf : data.cacheBuffers) {
        LlapIoImpl.LOG.info("Unlocking " + buf + " at the end of processing");
      }
    }
    lowLevelCache.releaseBuffers(data.cacheBuffers);
  }

  /**
   * Determines which RGs need to be read, after stripes have been determined.
   * SARG is applied, and readState is populated for each stripe accordingly.
   * @param stripes All stripes in the file (field state is used to determine stripes to read).
   */
  private void determineRgsToRead(boolean[] globalIncludes, int rowIndexStride,
      ArrayList<OrcStripeMetadata> metadata) throws IOException {
    SargApplier sargApp = null;
    if (sarg != null && rowIndexStride != 0) {
      List<OrcProto.Type> types = fileMetadata.getTypes();
      String[] colNamesForSarg = OrcInputFormat.getSargColumnNames(
          columnNames, types, globalIncludes, fileMetadata.isOriginalFormat());
      sargApp = new SargApplier(sarg, colNamesForSarg, rowIndexStride, types);
    }
    // readState should have been initialized by this time with an empty array.
    for (int stripeIxMod = 0; stripeIxMod < readState.length; ++stripeIxMod) {
      int stripeIx = stripeIxMod + stripeIxFrom;
      StripeInformation stripe = fileMetadata.getStripes().get(stripeIx);
      int rgCount = getRgCount(stripe, rowIndexStride);
      boolean[] rgsToRead = null;
      if (sargApp != null) {
        rgsToRead = sargApp.pickRowGroups(stripe, metadata.get(stripeIxMod).getRowIndexes());
      }
      if (DebugUtils.isTraceOrcEnabled()) {
        if (rgsToRead != null ) {
          LlapIoImpl.LOG.info("SARG picked RGs for stripe " + stripeIx + ": "
              + DebugUtils.toString(rgsToRead));
        } else {
          LlapIoImpl.LOG.info("Will read all " + rgCount + " RGs for stripe " + stripeIx);
        }
      }
      assert rgsToRead == null || rgsToRead.length == rgCount;
      readState[stripeIxMod] = new boolean[columnIds.size()][];
      for (int j = 0; j < columnIds.size(); ++j) {
        readState[stripeIxMod][j] = (rgsToRead == null) ? null :
          Arrays.copyOf(rgsToRead, rgsToRead.length);
      }

      int count = 0;
      if (rgsToRead != null) {
        for (boolean b : rgsToRead) {
          if (b)
            count++;
        }
      } else {
        count = rgCount;
      }
      counters.setCounter(QueryFragmentCounters.Counter.SELECTED_ROWGROUPS, count);
    }
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
    if (LlapIoImpl.LOGL.isDebugEnabled()) {
      String tmp = "FileSplit {" + split.getStart() + ", " + split.getLength() + "}; stripes ";
      for (StripeInformation stripe : stripes) {
        tmp += "{" + stripe.getOffset() + ", " + stripe.getLength() + "}, ";
      }
      LlapIoImpl.LOG.debug(tmp);
    }

    int stripeIx = 0;
    for (StripeInformation stripe : stripes) {
      long stripeStart = stripe.getOffset();
      if (offset > stripeStart) continue;
      if (stripeIxFrom == -1) {
        if (DebugUtils.isTraceOrcEnabled()) {
          LlapIoImpl.LOG.info("Including stripes from " + stripeIx
              + " (" + stripeStart + " >= " + offset + ")");
        }
        stripeIxFrom = stripeIx;
      }
      if (stripeStart >= maxOffset) {
        if (DebugUtils.isTraceOrcEnabled()) {
          LlapIoImpl.LOG.info("Including stripes until " + stripeIxTo + " (" + stripeStart
              + " >= " + maxOffset + "); " + (stripeIxTo - stripeIxFrom) + " stripes");
        }
        stripeIxTo = stripeIx;
        break;
      }
      ++stripeIx;
    }
    if (stripeIxTo == -1) {
      stripeIxTo = stripeIx;
      if (DebugUtils.isTraceOrcEnabled()) {
        LlapIoImpl.LOG.info("Including stripes until " + stripeIx + " (end of file); "
            + (stripeIxTo - stripeIxFrom) + " stripes");
      }
    }
    readState = new boolean[stripeIxTo - stripeIxFrom][][];
  }

  // TODO: split by stripe? we do everything by stripe, and it might be faster
  /**
   * Takes the data from high-level cache for all stripes and returns to consumer.
   * @return List of columns to read per stripe, if any columns were fully eliminated by cache.
   */
  private List<Integer>[] produceDataFromCache(int rowIndexStride) throws IOException {
    OrcCacheKey key = new OrcCacheKey(fileId, -1, -1, -1);
    // For each stripe, keep a list of columns that are not fully in cache (null => all of them).
    @SuppressWarnings("unchecked")
    List<Integer>[] stripeColsNotInCache = new List[readState.length];
    for (int stripeIxMod = 0; stripeIxMod < readState.length; ++stripeIxMod) {
      key.stripeIx = stripeIxFrom + stripeIxMod;
      boolean[][] cols = readState[stripeIxMod];
      boolean[] isMissingAnyRgs = new boolean[cols.length];
      int totalRgCount = getRgCount(fileMetadata.getStripes().get(key.stripeIx), rowIndexStride);
      for (int rgIx = 0; rgIx < totalRgCount; ++rgIx) {
        EncodedColumnBatch<OrcBatchKey> col = new EncodedColumnBatch<OrcBatchKey>(
            new OrcBatchKey(fileId, key.stripeIx, rgIx), cols.length, cols.length);
        boolean hasAnyCached = false;
        try {
          key.rgIx = rgIx;
          for (int colIxMod = 0; colIxMod < cols.length; ++colIxMod) {
            boolean[] readMask = cols[colIxMod];
            // Check if RG is eliminated by SARG
            if (readMask != null && (readMask.length <= rgIx || !readMask[rgIx])) continue;
            key.colIx = columnIds.get(colIxMod);
            StreamBuffer[] cached = cache.get(key);
            if (cached == null) {
              isMissingAnyRgs[colIxMod] = true;
              continue;
            }
            col.setAllStreams(colIxMod, key.colIx, cached);
            hasAnyCached = true;
            if (readMask == null) {
              // We were going to read all RGs, but some were in cache, allocate the mask.
              cols[colIxMod] = readMask = new boolean[totalRgCount];
              Arrays.fill(readMask, true);
            }
            readMask[rgIx] = false; // Got from cache, don't read from disk.
          }
        } catch (Throwable t) {
          // TODO: Any cleanup needed to release data in col back to cache should be here.
          throw (t instanceof IOException) ? (IOException)t : new IOException(t);
        }
        if (hasAnyCached) {
          consumer.consumeData(col);
        }
      }
      boolean makeStripeColList = false; // By default assume we'll fetch all original columns.
      for (int colIxMod = 0; colIxMod < cols.length; ++colIxMod) {
        if (isMissingAnyRgs[colIxMod]) {
          if (makeStripeColList) {
            stripeColsNotInCache[stripeIxMod].add(columnIds.get(colIxMod));
          }
        } else if (!makeStripeColList) {
          // Some columns were fully in cache. Make a per-stripe col list, add previous columns.
          makeStripeColList = true;
          stripeColsNotInCache[stripeIxMod] = new ArrayList<Integer>(cols.length - 1);
          for (int i = 0; i < colIxMod; ++i) {
            stripeColsNotInCache[stripeIxMod].add(columnIds.get(i));
          }
        }
      }
    }
    return stripeColsNotInCache;
  }

  @Override
  public void setDone() {
    consumer.setDone();
  }

  @Override
  public void consumeData(EncodedColumnBatch<OrcBatchKey> data) {
    // Store object in cache; create new key object - cannot be reused.
    assert cache != null;
    for (int i = 0; i < data.columnData.length; ++i) {
      OrcCacheKey key = new OrcCacheKey(data.batchKey, data.columnIxs[i]);
      StreamBuffer[] toCache = data.columnData[i];
      StreamBuffer[] cached = cache.cacheOrGet(key, toCache);
      if (toCache != cached) {
        for (StreamBuffer sb : toCache) {
          if (sb.decRef() != 0) continue;
          lowLevelCache.releaseBuffers(sb.cacheBuffers);
        }
        data.columnData[i] = cached;
      }
    }
    consumer.consumeData(data);
  }

  @Override
  public void setError(Throwable t) {
    consumer.setError(t);
  }
}