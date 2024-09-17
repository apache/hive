/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.io.orc;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.IllegalCacheConfigurationException;
import org.apache.hadoop.hive.llap.LlapHiveUtils;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDeltaLight;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.txn.compactor.MRCompactor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hive.common.util.Ref;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.SchemaEvolution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg.sargToKryo;

/**
 * A fast vectorized batch reader class for ACID. Insert events are read directly
 * from the base files/insert_only deltas in vectorized row batches. The deleted
 * rows can then be easily indicated via the 'selected' field of the vectorized row batch.
 * Refer HIVE-14233 for more details.
 */
public class VectorizedOrcAcidRowBatchReader
    implements org.apache.hadoop.mapred.RecordReader<NullWritable,VectorizedRowBatch> {

  private static final Logger LOG = LoggerFactory.getLogger(VectorizedOrcAcidRowBatchReader.class);

  private org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch> baseReader;
  private final VectorizedRowBatchCtx rbCtx;
  private VectorizedRowBatch vectorizedRowBatchBase;
  private long offset;
  private long length;
  protected float progress = 0.0f;
  protected Object[] partitionValues;
  private boolean addPartitionCols = true;
  /**
   * true means there is no OrcRecordUpdater.ROW column
   * (i.e. the struct wrapping user columns) in {@link #vectorizedRowBatchBase}.
   */
  private final boolean isFlatPayload;
  private final ValidWriteIdList validWriteIdList;
  private final DeleteEventRegistry deleteEventRegistry;
  /**
   * {@link RecordIdentifier}/{@link VirtualColumn#ROWID} information
   */
  private final StructColumnVector recordIdColumnVector;
  private final RowIsDeletedColumnVector rowIsDeletedVector;
  private final Reader.Options readerOptions;
  private final boolean isOriginal;
  /**
   * something further in the data pipeline wants {@link VirtualColumn#ROWID}
   */
  private final boolean rowIdProjected;
  private final boolean rowIsDeletedProjected;
  private final boolean fetchDeletedRows;
  /**
   * if false, we don't need any acid medadata columns from the file because we
   * know all data in the split is valid (wrt to visible writeIDs/delete events)
   * and ROW_ID is not needed higher up in the operator pipeline
   */
  private final boolean includeAcidColumns;
  /**
   * partition/table root
   */
  private final Path rootPath;
  /**
   * for reading "original" files
   */
  private final OrcSplit.OffsetAndBucketProperty syntheticProps;
  /**
   * To have access to {@link RecordReader#getRowNumber()} in the underlying
   * file which we need to generate synthetic ROW_IDs for original files
   */
  private RecordReader innerReader;
  /**
   * min/max ROW__ID for the split (if available) so that we can limit the
   * number of delete events to load in memory
   */
  private final OrcRawRecordMerger.KeyInterval keyInterval;
  /**
   * {@link SearchArgument} pushed down to delete_deltaS
   */
  private SearchArgument deleteEventSarg = null;

  /**
   * Cachetag associated with the Split
   */
  private final CacheTag cacheTag;

  //OrcInputFormat c'tor
  VectorizedOrcAcidRowBatchReader(OrcSplit inputSplit, JobConf conf,
                                  Reporter reporter) throws IOException {
    this(inputSplit, conf,reporter, null);
  }
  @VisibleForTesting
  VectorizedOrcAcidRowBatchReader(OrcSplit inputSplit, JobConf conf,
        Reporter reporter, VectorizedRowBatchCtx rbCtx) throws IOException {
    this(conf, inputSplit, reporter,
        rbCtx == null ? Utilities.getVectorizedRowBatchCtx(conf) : rbCtx, false, null);

    final Reader reader = OrcInputFormat.createOrcReaderForSplit(conf, inputSplit);
    // Careful with the range here now, we do not want to read the whole base file like deltas.
    innerReader = reader.rowsOptions(readerOptions.range(offset, length), conf);
    baseReader = new org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch>() {

      @Override
      public boolean next(NullWritable key, VectorizedRowBatch value) throws IOException {
        return innerReader.nextBatch(value);
      }

      @Override
      public NullWritable createKey() {
        return NullWritable.get();
      }

      @Override
      public VectorizedRowBatch createValue() {
        return rbCtx.createVectorizedRowBatch();
      }

      @Override
      public long getPos() throws IOException {
        return 0;
      }

      @Override
      public void close() throws IOException {
        innerReader.close();
      }

      @Override
      public float getProgress() throws IOException {
        return innerReader.getProgress();
      }
    };
    final boolean useDecimal64ColumnVectors = HiveConf
      .getVar(conf, ConfVars.HIVE_VECTORIZED_INPUT_FORMAT_SUPPORTS_ENABLED).equalsIgnoreCase("decimal_64");
    if (useDecimal64ColumnVectors) {
      this.vectorizedRowBatchBase = ((RecordReaderImpl) innerReader).createRowBatch(true);
    } else {
      this.vectorizedRowBatchBase = ((RecordReaderImpl) innerReader).createRowBatch(false);
    }
  }

  /**
   * LLAP IO c'tor
   */
  public VectorizedOrcAcidRowBatchReader(OrcSplit inputSplit, JobConf conf, Reporter reporter,
    org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch> baseReader,
    VectorizedRowBatchCtx rbCtx, boolean isFlatPayload, MapWork mapWork) throws IOException {
    this(conf, inputSplit, reporter, rbCtx, isFlatPayload, mapWork);
    if (baseReader != null) {
      setBaseAndInnerReader(baseReader);
    }
  }

  private VectorizedOrcAcidRowBatchReader(JobConf conf, OrcSplit orcSplit, Reporter reporter,
      VectorizedRowBatchCtx rowBatchCtx, boolean isFlatPayload, MapWork mapWork) throws IOException {
    this.isFlatPayload = isFlatPayload;
    this.rbCtx = rowBatchCtx;
    final boolean isAcidRead = AcidUtils.isFullAcidScan(conf);
    final AcidUtils.AcidOperationalProperties acidOperationalProperties
            = AcidUtils.getAcidOperationalProperties(conf);

    // This type of VectorizedOrcAcidRowBatchReader can only be created when split-update is
    // enabled for an ACID case and the file format is ORC.
    boolean isReadNotAllowed = !isAcidRead || !acidOperationalProperties.isSplitUpdate();
    if (isReadNotAllowed) {
      OrcInputFormat.raiseAcidTablesMustBeReadWithAcidReaderException(conf);
    }

    reporter.setStatus(orcSplit.toString());
    readerOptions = OrcInputFormat.createOptionsForReader(conf);

    this.offset = orcSplit.getStart();
    this.length = orcSplit.getLength();

    int partitionColumnCount = (rbCtx != null) ? rbCtx.getPartitionColumnCount() : 0;
    if (partitionColumnCount > 0) {
      partitionValues = new Object[partitionColumnCount];
      VectorizedRowBatchCtx.getPartitionValues(rbCtx, conf, orcSplit, partitionValues);
    } else {
      partitionValues = null;
    }

    String txnString = conf.get(ValidWriteIdList.VALID_WRITEIDS_KEY);
    this.validWriteIdList = (txnString == null) ? new ValidReaderWriteIdList() : new ValidReaderWriteIdList(txnString);
    LOG.info("Read ValidWriteIdList: " + this.validWriteIdList.toString()
            + ":" + orcSplit);

    this.syntheticProps = orcSplit.getSyntheticAcidProps();

    if (LlapHiveUtils.isLlapMode(conf) && LlapProxy.isDaemon()
            && HiveConf.getBoolVar(conf, ConfVars.LLAP_TRACK_CACHE_USAGE))
    {
      if (mapWork == null) {
        mapWork = LlapHiveUtils.findMapWork(conf);
      }
      PartitionDesc partitionDesc =
          LlapHiveUtils.partitionDescForPath(orcSplit.getPath(), mapWork.getPathToPartitionInfo());
      cacheTag = LlapHiveUtils.getDbAndTableNameForMetrics(orcSplit.getPath(), true, partitionDesc);
    } else {
      cacheTag = null;
    }

    // Clone readerOptions for deleteEvents.
    Reader.Options deleteEventReaderOptions = readerOptions.clone();
    // Set the range on the deleteEventReaderOptions to 0 to INTEGER_MAX because
    // we always want to read all the delete delta files.
    deleteEventReaderOptions.range(0, Long.MAX_VALUE);
    deleteEventReaderOptions.searchArgument(null, null);
    keyInterval = findMinMaxKeys(orcSplit, conf, deleteEventReaderOptions);
    fetchDeletedRows = acidOperationalProperties.isFetchDeletedRows();
    DeleteEventRegistry der;
    try {
      // See if we can load all the relevant delete events from all the
      // delete deltas in memory...
      ColumnizedDeleteEventRegistry.OriginalWriteIdLoader writeIdLoader;
      if (fetchDeletedRows) {
        // Deleted rows requires both Current and Original writeId.
        // Original is for lookup
        // Current is for updating the writeId in the output record
        writeIdLoader = new ColumnizedDeleteEventRegistry.OriginalAndCurrentWriteIdLoader();
      } else {
        writeIdLoader = new ColumnizedDeleteEventRegistry.OriginalWriteIdLoader();
      }
      der = new ColumnizedDeleteEventRegistry(conf, orcSplit,
          deleteEventReaderOptions, keyInterval, cacheTag, writeIdLoader);
    } catch (DeleteEventsOverflowMemoryException e) {
      // If not, then create a set of hanging readers that do sort-merge to find the next smallest
      // delete event on-demand. Caps the memory consumption to (some_const * no. of readers).
      der = new SortMergedDeleteEventRegistry(conf, orcSplit, deleteEventReaderOptions, fetchDeletedRows);
    }
    this.deleteEventRegistry = der;
    isOriginal = orcSplit.isOriginal();
    if (isOriginal) {
      recordIdColumnVector = new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
        new LongColumnVector(), new LongColumnVector(), new LongColumnVector());
    } else {
      // Will swap in the Vectors from underlying row batch.
      recordIdColumnVector = new StructColumnVector(
          VectorizedRowBatch.DEFAULT_SIZE, null, null, null);
    }
    rowIdProjected = areRowIdsProjected(rbCtx);
    rowIsDeletedProjected = isRowIsDeletedProjected(rbCtx);
    if (rowIsDeletedProjected) {
      rowIsDeletedVector = new RowIsDeletedColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    } else {
      rowIsDeletedVector = null;
    }
    rootPath = orcSplit.getRootDir();

    /**
     * This could be optimized by moving dir type/write id based checks are
     * done during split generation (i.e. per file not per split) and the
     * DeleteEventRegistry is checked here since some splits from the same
     * file may have relevant deletes and other may not.
     */
    if(conf.getBoolean(ConfVars.OPTIMIZE_ACID_META_COLUMNS.varname, true)) {
      /*figure out if we can skip reading acid metadata columns:
       * isOriginal - don't have meta columns - nothing to skip
       * there no relevant delete events && ROW__ID is not needed higher up
       * (e.g. this is not a delete statement)*/
      if (deleteEventRegistry.isEmpty() && !rowIdProjected) {
        Path parent = orcSplit.getPath().getParent();
        while (parent != null && !rootPath.equals(parent)) {
          if (parent.getName().startsWith(AcidUtils.BASE_PREFIX)) {
            /**
             * The assumption here is that any base_x is filtered out by
             * {@link AcidUtils#getAcidState(Path, Configuration, ValidWriteIdList)}
             * so if we see it here it's valid.
             * {@link AcidUtils#isValidBase(long, ValidWriteIdList, Path, FileSystem)}
             * can check but it makes a {@link FileSystem} call.
             */
            readerOptions.includeAcidColumns(false);
            break;
          } else {
            ParsedDeltaLight pd = ParsedDeltaLight.parse(parent);
            if (validWriteIdList.isWriteIdRangeValid(pd.getMinWriteId(),
                pd.getMaxWriteId()) == ValidWriteIdList.RangeResponse.ALL) {
              //all write IDs in range are committed (and visible in current
              // snapshot)
              readerOptions.includeAcidColumns(false);
              break;
            }
          }
          parent = parent.getParent();
        }
      }
    }
    includeAcidColumns = readerOptions.getIncludeAcidColumns();//default is true
  }

  /**
   * Generates a SearchArgument to push down to delete_delta files.
   *
   *
   * Note that bucket is a bit packed int, so even thought all delete events
   * for a given split have the same bucket ID but not the same "bucket" value
   * {@link BucketCodec}
   */
  private void setSARG(OrcRawRecordMerger.KeyInterval keyInterval,
      Reader.Options deleteEventReaderOptions,
      long minBucketProp, long maxBucketProp, long minRowId, long maxRowId) {
    SearchArgument.Builder b = null;
    if(keyInterval.getMinKey() != null) {
      RecordIdentifier k = keyInterval.getMinKey();
      b = SearchArgumentFactory.newBuilder();
      b.startAnd()  //not(ot < 7) -> ot >=7
          .startNot().lessThan(OrcRecordUpdater.ORIGINAL_WRITEID_FIELD_NAME,
          PredicateLeaf.Type.LONG, k.getWriteId()).end();
      b.startNot().lessThan(
          OrcRecordUpdater.BUCKET_FIELD_NAME, PredicateLeaf.Type.LONG, minBucketProp).end();
      b.startNot().lessThan(OrcRecordUpdater.ROW_ID_FIELD_NAME,
          PredicateLeaf.Type.LONG, minRowId).end();
      b.end();
    }
    if(keyInterval.getMaxKey() != null) {
      RecordIdentifier k = keyInterval.getMaxKey();
      if(b == null) {
        b = SearchArgumentFactory.newBuilder();
      }
      b.startAnd().lessThanEquals(
          OrcRecordUpdater.ORIGINAL_WRITEID_FIELD_NAME, PredicateLeaf.Type.LONG, k.getWriteId());
      b.lessThanEquals(OrcRecordUpdater.BUCKET_FIELD_NAME, PredicateLeaf.Type.LONG, maxBucketProp);
      b.lessThanEquals(OrcRecordUpdater.ROW_ID_FIELD_NAME, PredicateLeaf.Type.LONG, maxRowId);
      b.end();
    }
    if(b != null) {
      deleteEventSarg = b.build();
      LOG.info("deleteReader SARG(" + deleteEventSarg + ") ");
      deleteEventReaderOptions.searchArgument(deleteEventSarg,
          new String[] {
              OrcRecordUpdater.ORIGINAL_WRITEID_FIELD_NAME,
              OrcRecordUpdater.BUCKET_FIELD_NAME,
              OrcRecordUpdater.ROW_ID_FIELD_NAME
          });
      return;
    }
    deleteEventReaderOptions.searchArgument(null, null);
  }

  public boolean includeAcidColumns() {
    return this.includeAcidColumns;
  }
  public void setBaseAndInnerReader(
    final org.apache.hadoop.mapred.RecordReader<NullWritable,
        VectorizedRowBatch> baseReader) {
    this.baseReader = baseReader;
    this.innerReader = null;
    this.vectorizedRowBatchBase = baseReader.createValue();
  }

  /**
   * A given ORC reader will always process one or more whole stripes but the
   * split boundaries may not line up with stripe boundaries if the InputFormat
   * doesn't understand ORC specifics. So first we need to figure out which
   * stripe(s) we are reading.
   *
   * Suppose txn1 writes 100K rows
   * and txn2 writes 100 rows so we have events
   * {1,0,0}....{1,0,100K},{2,0,0}...{2,0,100} in 2 files
   * After compaction we may have 2 stripes
   * {1,0,0}...{1,0,90K},{1,0,90001}...{2,0,100}
   *
   * Now suppose there is a delete stmt that deletes every row.  So when we load
   * the 2nd stripe, if we just look at stripe {@link ColumnStatistics},
   * minKey={1,0,100} and maxKey={2,0,90001}, all but the 1st 100 delete events
   * will get loaded.  But with {@link OrcRecordUpdater#ACID_KEY_INDEX_NAME},
   * minKey={1,0,90001} and maxKey={2,0,100} so we only load about 10K deletes.
   *
   * Also, even with Query Based compactor (once we have it), FileSinkOperator
   * uses OrcRecordWriter to write to file, so we should have the
   * hive.acid.index in place.
   *
   * If reading the 1st stripe, we don't have the start event, so we'll get it
   * from stats, which will strictly speaking be accurate only wrt writeId and
   * bucket but that is good enough.
   *
   * @return empty <code>KeyInterval</code> if KeyInterval could not be
   * determined
   */
  private OrcRawRecordMerger.KeyInterval findMinMaxKeys(
      OrcSplit orcSplit, Configuration conf,
      Reader.Options deleteEventReaderOptions) throws IOException {
    final boolean noDeleteDeltas = orcSplit.getDeltas().size() == 0;
    if(!HiveConf.getBoolVar(conf, ConfVars.FILTER_DELETE_EVENTS) || noDeleteDeltas) {
      LOG.debug("findMinMaxKeys() " + ConfVars.FILTER_DELETE_EVENTS + "=false");
      return new OrcRawRecordMerger.KeyInterval(null, null);
    }

    try (VectorizedOrcAcidRowBatchReader.ReaderData orcReaderData =
        getOrcReaderData(orcSplit.getPath(), conf, cacheTag, orcSplit.getFileKey())) {

      if(orcSplit.isOriginal()) {
        /**
         * Among originals we may have files with _copy_N suffix.  To properly
         * generate a synthetic ROW___ID for them we need
         * {@link OffsetAndBucketProperty} which could be an expensive computation
         * if there are lots of copy_N files for a given bucketId. But unless
         * there are delete events, we often don't need synthetic ROW__IDs at all.
         * Kind of chicken-and-egg - deal with this later.
         * See {@link OrcRawRecordMerger#discoverOriginalKeyBounds(Reader, int,
         * Reader.Options, Configuration, OrcRawRecordMerger.Options)}*/
        LOG.debug("findMinMaxKeys(original split)");

        return findOriginalMinMaxKeys(orcSplit, orcReaderData.orcTail, deleteEventReaderOptions);
      }

      List<StripeInformation> stripes = orcReaderData.orcTail.getStripes();
      final long splitStart = orcSplit.getStart();
      final long splitEnd = splitStart + orcSplit.getLength();
      int firstStripeIndex = -1;
      int lastStripeIndex = -1;
      for(int i = 0; i < stripes.size(); i++) {
        StripeInformation stripe = stripes.get(i);
        long stripeEnd = stripe.getOffset() + stripe.getLength();
        if(firstStripeIndex == -1 && stripe.getOffset() >= splitStart) {
          firstStripeIndex = i;
        }
        if(lastStripeIndex == -1 && splitEnd <= stripeEnd) {
          lastStripeIndex = i;
        }
      }
      if(lastStripeIndex == -1) {
        //split goes to the EOF which is > end of stripe since file has a footer
        assert stripes.get(stripes.size() - 1).getOffset() +
            stripes.get(stripes.size() - 1).getLength() < splitEnd;
        lastStripeIndex = stripes.size() - 1;
      }

      if (firstStripeIndex > lastStripeIndex || firstStripeIndex == -1) {
        /**
         * If the firstStripeIndex was set after the lastStripeIndex the split lies entirely within a single stripe.
         * In case the split lies entirely within the last stripe, the firstStripeIndex will never be found, hence the
         * second condition.
         * In this case, the reader for this split will not read any data.
         * See {@link org.apache.orc.impl.RecordReaderImpl#RecordReaderImpl
         * Create a KeyInterval such that no delete delta records are loaded into memory in the deleteEventRegistry.
         */

        long minRowId = 1;
        long maxRowId = 0;
        int minBucketProp = 1;
        int maxBucketProp = 0;

        OrcRawRecordMerger.KeyInterval keyIntervalTmp =
            new OrcRawRecordMerger.KeyInterval(new RecordIdentifier(1, minBucketProp, minRowId),
            new RecordIdentifier(0, maxBucketProp, maxRowId));

        setSARG(keyIntervalTmp, deleteEventReaderOptions, minBucketProp, maxBucketProp,
            minRowId, maxRowId);
        LOG.info("findMinMaxKeys(): " + keyIntervalTmp +
            " stripes(" + firstStripeIndex + "," + lastStripeIndex + ")");

        return keyIntervalTmp;
      }

      if(firstStripeIndex == -1 || lastStripeIndex == -1) {
        //this should not happen but... if we don't know which stripe(s) are
        //involved we can't figure out min/max bounds
        LOG.warn("Could not find stripe (" + firstStripeIndex + "," +
            lastStripeIndex + ")");
        return new OrcRawRecordMerger.KeyInterval(null, null);
      }
      RecordIdentifier[] keyIndex = OrcRecordUpdater.parseKeyIndex(orcReaderData.orcTail);

      if(keyIndex == null) {
        LOG.warn("Could not find keyIndex (" + firstStripeIndex + "," +
            lastStripeIndex + "," + stripes.size() + ")");
      }

      if(keyIndex != null && keyIndex.length != stripes.size()) {
        LOG.warn("keyIndex length doesn't match (" +
            firstStripeIndex + "," + lastStripeIndex + "," + stripes.size() +
            "," + keyIndex.length + ")");
        return new OrcRawRecordMerger.KeyInterval(null, null);
      }
      /**
       * If {@link OrcConf.ROW_INDEX_STRIDE} is set to 0 all column stats on
       * ORC file are disabled though objects for them exist but and have
       * min/max set to MIN_LONG/MAX_LONG so we only use column stats if they
       * are actually computed.  Streaming ingest used to set it 0 and Minor
       * compaction so there are lots of legacy files with no (rather, bad)
       * column stats*/
      boolean columnStatsPresent = orcReaderData.orcTail.getFooter().getRowIndexStride() > 0;
      if(!columnStatsPresent) {
        LOG.debug("findMinMaxKeys() No ORC column stats");
      }

      List<StripeStatistics> stats = orcReaderData.reader.getVariantStripeStatistics(null);
      assert stripes.size() == stats.size() : "str.s=" + stripes.size() +
          " sta.s=" + stats.size();

      RecordIdentifier minKey = null;
      if(firstStripeIndex > 0 && keyIndex != null) {
        //valid keys are strictly > than this key
        minKey = keyIndex[firstStripeIndex - 1];
        //add 1 to make comparison >= to match the case of 0th stripe
        minKey.setRowId(minKey.getRowId() + 1);
      }
      else {
        if(columnStatsPresent) {
          minKey = getKeyInterval(stats.get(firstStripeIndex).getColumnStatistics()).getMinKey();
        }
      }

      RecordIdentifier maxKey = null;

      if (keyIndex != null) {
        maxKey = keyIndex[lastStripeIndex];
      } else {
        if(columnStatsPresent) {
          maxKey = getKeyInterval(stats.get(lastStripeIndex).getColumnStatistics()).getMaxKey();
        }
      }
      OrcRawRecordMerger.KeyInterval keyInterval =
          new OrcRawRecordMerger.KeyInterval(minKey, maxKey);
      LOG.info("findMinMaxKeys(): " + keyInterval +
          " stripes(" + firstStripeIndex + "," + lastStripeIndex + ")");

      long minBucketProp = Long.MAX_VALUE, maxBucketProp = Long.MIN_VALUE;
      long minRowId = Long.MAX_VALUE, maxRowId = Long.MIN_VALUE;
      if(columnStatsPresent) {
        /**
         * figure out min/max bucket, rowid for push down.  This is different from
         * min/max ROW__ID because ROW__ID comparison uses dictionary order on two
         * tuples (a,b,c), but PPD can only do
         * (a between (x,y) and b between(x1,y1) and c between(x2,y2))
         * Consider:
         * (0,536936448,0), (0,536936448,2), (10000001,536936448,0)
         * 1st is min ROW_ID, 3r is max ROW_ID
         * and Delete events (0,536936448,2),....,(10000001,536936448,1000000)
         * So PPD based on min/max ROW_ID would have 0<= rowId <=0 which will
         * miss this delete event.  But we still want PPD to filter out data if
         * possible.
         *
         * So use stripe stats to find proper min/max for bucketProp and rowId
         * writeId is the same in both cases
         */
        for(int i = firstStripeIndex; i <= lastStripeIndex; i++) {
          OrcRawRecordMerger.KeyInterval key = getKeyInterval(stats.get(i).getColumnStatistics());
          if(key.getMinKey().getBucketProperty() < minBucketProp) {
            minBucketProp = key.getMinKey().getBucketProperty();
          }
          if(key.getMaxKey().getBucketProperty() > maxBucketProp) {
            maxBucketProp = key.getMaxKey().getBucketProperty();
          }
          if(key.getMinKey().getRowId() < minRowId) {
            minRowId = key.getMinKey().getRowId();
          }
          if(key.getMaxKey().getRowId() > maxRowId) {
            maxRowId = key.getMaxKey().getRowId();
          }
        }
      }
      if(minBucketProp == Long.MAX_VALUE) minBucketProp = Long.MIN_VALUE;
      if(maxBucketProp == Long.MIN_VALUE) maxBucketProp = Long.MAX_VALUE;
      if(minRowId == Long.MAX_VALUE) minRowId = Long.MIN_VALUE;
      if(maxRowId == Long.MIN_VALUE) maxRowId = Long.MAX_VALUE;

      setSARG(keyInterval, deleteEventReaderOptions, minBucketProp, maxBucketProp,
          minRowId, maxRowId);

      return keyInterval;
    }
  }

  private OrcRawRecordMerger.KeyInterval findOriginalMinMaxKeys(OrcSplit orcSplit, OrcTail orcTail,
      Reader.Options deleteEventReaderOptions) {

    // This method returns the minimum and maximum synthetic row ids that are present in this split
    // because min and max keys are both inclusive when filtering out the delete delta records.

    if (syntheticProps == null) {
      // syntheticProps containing the synthetic rowid offset is computed if there are delete delta files.
      // If there aren't any delete delta files, then we don't need this anyway.
      return new OrcRawRecordMerger.KeyInterval(null, null);
    }

    long splitStart = orcSplit.getStart();
    long splitEnd = orcSplit.getStart() + orcSplit.getLength();

    long minRowId = syntheticProps.getRowIdOffset();
    long maxRowId = syntheticProps.getRowIdOffset();

    for(StripeInformation stripe: orcTail.getStripes()) {
      if (splitStart > stripe.getOffset()) {
        // This stripe starts before the current split starts. This stripe is not included in this split.
        minRowId += stripe.getNumberOfRows();
      }

      if (splitEnd > stripe.getOffset()) {
        // This stripe starts before the current split ends.
        maxRowId += stripe.getNumberOfRows();
      } else {
        // The split ends before (or exactly where) this stripe starts.
        // Remaining stripes are not included in this split.
        break;
      }
    }

    RecordIdentifier minKey = new RecordIdentifier(syntheticProps.getSyntheticWriteId(),
        syntheticProps.getBucketProperty(), minRowId);

    RecordIdentifier maxKey = new RecordIdentifier(syntheticProps.getSyntheticWriteId(),
        syntheticProps.getBucketProperty(), maxRowId > 0? maxRowId - 1: 0);

    OrcRawRecordMerger.KeyInterval keyIntervalTmp = new OrcRawRecordMerger.KeyInterval(minKey, maxKey);

    if (minRowId >= maxRowId) {
      /**
       * The split lies entirely within a single stripe. In this case, the reader for this split will not read any data.
       * See {@link org.apache.orc.impl.RecordReaderImpl#RecordReaderImpl
       * We can return the min max key interval as is (it will not read any of the delete delta records into mem)
       */

      LOG.info("findOriginalMinMaxKeys(): This split starts and ends in the same stripe.");
    }

    LOG.info("findOriginalMinMaxKeys(): " + keyIntervalTmp);

    // Using min/max ROW__ID from original will work for ppd to the delete deltas because the writeid is the same in
    // the min and the max ROW__ID
    setSARG(keyIntervalTmp, deleteEventReaderOptions, minKey.getBucketProperty(), maxKey.getBucketProperty(),
        minKey.getRowId(), maxKey.getRowId());

    return keyIntervalTmp;
  }

  private static class ReaderData implements Closeable {
    OrcTail orcTail;
    Reader reader;

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }

  /**
   * Gets the OrcTail from cache if LLAP IO is enabled, otherwise creates the reader to get the tail.
   * Always store the Reader along with the Tail as part of ReaderData so we can reuse it.
   * @param path The Orc file path we want to get the OrcTail for
   * @param conf The Configuration to access LLAP
   * @param cacheTag The cacheTag needed to get OrcTail from LLAP IO cache
   * @param fileKey fileId of the Orc file (either the Long fileId of HDFS or the SyntheticFileId).
   *                Optional, if it is not provided, it will be generated, see:
   *                {@link org.apache.hadoop.hive.ql.io.HdfsUtils.getFileId()}
   * @return ReaderData object where the orcTail is not null. Reader can be null, but if we had to create
   * one we return that as well for further reuse.
   */
  private static ReaderData getOrcReaderData(Path path, Configuration conf, CacheTag cacheTag, Object fileKey) throws IOException {
    ReaderData readerData = new ReaderData();
    if (shouldReadDeleteDeltasWithLlap(conf, true)) {
      try {
        readerData.orcTail = LlapProxy.getIo().getOrcTailFromCache(path, conf, cacheTag, fileKey);
        readerData.reader = OrcFile.createReader(path, OrcFile.readerOptions(conf).orcTail(readerData.orcTail));
      } catch (IllegalCacheConfigurationException icce) {
        throw new IOException("LLAP cache is not configured properly while delete delta caching is turned on", icce);
      }
    }
    readerData.reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    readerData.orcTail = new OrcTail(readerData.reader.getFileTail(), readerData.reader.getSerializedFileFooter());
    return readerData;
  }

  /**
   * Checks whether delete delta files should be read through LLAP IO by verifying that:
   * - delete delta caching feature is turned on in configuration
   * - this execution is inside an LLAP daemon, and LLAP IO is enabled
   * @param conf job conf / session conf
   * @param metaDataLevelSufficient if true: 'metadata' level delete delta caching is sufficient to return true
   *                                if false: full delete delta caching ('all') is required for this to return true
   * @return
   */
  private static boolean shouldReadDeleteDeltasWithLlap(Configuration conf, boolean metaDataLevelSufficient) {
    String ddCacheLevel = HiveConf.getVar(conf, ConfVars.LLAP_IO_CACHE_DELETEDELTAS);
    return ("all".equals(ddCacheLevel) || (metaDataLevelSufficient && ddCacheLevel.equals("metadata")))
        && LlapHiveUtils.isLlapMode(conf) && LlapProxy.isDaemon() && LlapProxy.getIo() != null;
  }

  /**
   * See {@link #next(NullWritable, VectorizedRowBatch)} first and
   * {@link OrcRawRecordMerger.OriginalReaderPair}.
   * When reading a split of an "original" file and we need to decorate data with ROW__ID.
   * This requires treating multiple files that are part of the same bucket (tranche for unbucketed
   * tables) as a single logical file to number rowids consistently.
   */
  static OrcSplit.OffsetAndBucketProperty computeOffsetAndBucket(
          FileStatus file, Path rootDir, boolean isOriginal, boolean hasDeletes,
          Configuration conf) throws IOException {

    VectorizedRowBatchCtx vrbCtx = Utilities.getVectorizedRowBatchCtx(conf);

    if (!needSyntheticRowIds(isOriginal, hasDeletes, areRowIdsProjected(vrbCtx))) {
      if(isOriginal) {
        /**
         * Even if we don't need to project ROW_IDs, we still need to check the write ID that
         * created the file to see if it's committed.  See more in
         * {@link #next(NullWritable, VectorizedRowBatch)}.  (In practice getAcidState() should
         * filter out base/delta files but this makes fewer dependencies)
         */
        OrcRawRecordMerger.TransactionMetaData syntheticTxnInfo =
            OrcRawRecordMerger.TransactionMetaData.findWriteIDForSynthetcRowIDs(file.getPath(),
                    rootDir, conf);
        return new OrcSplit.OffsetAndBucketProperty(-1, -1, syntheticTxnInfo.syntheticWriteId);
      }
      return null;
    }

    String txnString = conf.get(ValidWriteIdList.VALID_WRITEIDS_KEY);
    ValidWriteIdList validWriteIdList = (txnString == null) ? new ValidReaderWriteIdList() :
        new ValidReaderWriteIdList(txnString);

    long rowIdOffset = 0;
    OrcRawRecordMerger.TransactionMetaData syntheticTxnInfo =
        OrcRawRecordMerger.TransactionMetaData.findWriteIDForSynthetcRowIDs(file.getPath(), rootDir, conf);
    int bucketId = AcidUtils.parseBucketId(file.getPath());
    int bucketProperty = BucketCodec.V1.encode(new AcidOutputFormat.Options(conf)
        //statementId is from directory name (or 0 if there is none)
      .statementId(syntheticTxnInfo.statementId).bucket(bucketId));
    AcidDirectory directoryState = AcidUtils.getAcidState(null, syntheticTxnInfo.folder, conf,
        validWriteIdList, Ref.from(false), true);
    for (HadoopShims.HdfsFileStatusWithId f : directoryState.getOriginalFiles()) {
      int bucketIdFromPath = AcidUtils.parseBucketId(f.getFileStatus().getPath());
      if (bucketIdFromPath != bucketId) {
        continue;//HIVE-16952
      }
      if (f.getFileStatus().getPath().equals(file.getPath())) {
        //'f' is the file whence this split is
        break;
      }
      Reader reader = OrcFile.createReader(f.getFileStatus().getPath(),
        OrcFile.readerOptions(conf));
      rowIdOffset += reader.getNumberOfRows();
    }
    return new OrcSplit.OffsetAndBucketProperty(rowIdOffset, bucketProperty,
      syntheticTxnInfo.syntheticWriteId);
  }
  /**
   * {@link VectorizedOrcAcidRowBatchReader} is always used for vectorized reads of acid tables.
   * In some cases this cannot be used from LLAP IO elevator because
   * {@link RecordReader#getRowNumber()} is not (currently) available there but is required to
   * generate ROW__IDs for "original" files
   * @param hasDeletes - if there are any deletes that apply to this split
   * todo: HIVE-17944
   */
  static boolean canUseLlapIoForAcid(OrcSplit split, boolean hasDeletes, Configuration conf) {
    if(!split.isOriginal()) {
      return true;
    }
    VectorizedRowBatchCtx rbCtx = Utilities.getVectorizedRowBatchCtx(conf);
    if(rbCtx == null) {
      throw new IllegalStateException("Could not create VectorizedRowBatchCtx for " + split.getPath());
    }
    return !needSyntheticRowIds(split.isOriginal(), hasDeletes, areRowIdsProjected(rbCtx));
  }

  /**
   * Does this reader need to decorate rows with ROW__IDs (for "original" reads).
   * Even if ROW__ID is not projected you still need to decorate the rows with them to see if
   * any of the delete events apply.
   */
  private static boolean needSyntheticRowIds(boolean isOriginal, boolean hasDeletes, boolean rowIdProjected) {
    return isOriginal && (hasDeletes || rowIdProjected);
  }

  private static boolean areRowIdsProjected(VectorizedRowBatchCtx rbCtx) {
    //The query needs ROW__ID: maybe explicitly asked, maybe it's part of
    // Update/Delete statement.
    //Either way, we need to decorate "original" rows with row__id
    return isVirtualColumnProjected(rbCtx, VirtualColumn.ROWID);
  }

  private static boolean isRowIsDeletedProjected(VectorizedRowBatchCtx rbCtx) {
    return isVirtualColumnProjected(rbCtx, VirtualColumn.ROWISDELETED);
  }

  private static boolean isVirtualColumnProjected(VectorizedRowBatchCtx rbCtx, VirtualColumn virtualColumn) {
    if (rbCtx.getVirtualColumnCount() == 0) {
      return false;
    }
    for(VirtualColumn vc : rbCtx.getNeededVirtualColumns()) {
      if(vc == virtualColumn) {
        return true;
      }
    }
    return false;
  }

  @Deprecated
  static Path[] getDeleteDeltaDirsFromSplit(OrcSplit orcSplit) {
    return getDeleteDeltaDirsFromSplit(orcSplit, null);
  }

  static Path[] getDeleteDeltaDirsFromSplit(OrcSplit orcSplit,
      Map<String, AcidInputFormat.DeltaMetaData> pathToDeltaMetaData) {
    Path path = orcSplit.getPath();
    Path root;
    if (orcSplit.hasBase()) {
      if (orcSplit.isOriginal()) {
        root = orcSplit.getRootDir();
      } else {
        root = path.getParent().getParent();//todo: why not just use getRootDir()?
        assert root.equals(orcSplit.getRootDir()) : "root mismatch: baseDir=" + orcSplit.getRootDir() +
          " path.p.p=" + root;
      }
    } else {
      throw new IllegalStateException("Split w/o base w/Acid 2.0??: " + path);
    }
    return AcidUtils.deserializeDeleteDeltas(root, orcSplit.getDeltas(), pathToDeltaMetaData);
  }

  /**
   * There are 2 types of schema from the {@link #baseReader} that this handles.  In the case
   * the data was written to a transactional table from the start, every row is decorated with
   * transaction related info and looks like &lt;op, owid, writerId, rowid, cwid, &lt;f1, ... fn&gt;&gt;.
   *
   * The other case is when data was written to non-transactional table and thus only has the user
   * data: &lt;f1, ... fn&gt;.  Then this table was then converted to a transactional table but the data
   * files are not changed until major compaction.  These are the "original" files.
   *
   * In this case we may need to decorate the outgoing data with transactional column values at
   * read time.  (It's done somewhat out of band via VectorizedRowBatchCtx - ask Teddy Choi).
   * The "owid, writerId, rowid" columns represent {@link RecordIdentifier}.  They are assigned
   * each time the table is read in a way that needs to project {@link VirtualColumn#ROWID}.
   * Major compaction will attach these values to each row permanently.
   * It's critical that these generated column values are assigned exactly the same way by each
   * read of the same row and by the Compactor.
   * See {@link MRCompactor} and
   * {@link OrcRawRecordMerger.OriginalReaderPairToCompact} for the Compactor read path.
   * (Longer term should make compactor use this class)
   *
   * This only decorates original rows with metadata if something above is requesting these values
   * or if there are Delete events to apply.
   *
   * @return false where there is no more data, i.e. {@code value} is empty
   */
  @Override
  public boolean next(NullWritable key, VectorizedRowBatch value) throws IOException {
    try {
      // Check and update partition cols if necessary. Ideally, this should be done
      // in CreateValue as the partition is constant per split. But since Hive uses
      // CombineHiveRecordReader and
      // as this does not call CreateValue for each new RecordReader it creates, this check is
      // required in next()
      if (addPartitionCols) {
        if (partitionValues != null) {
          rbCtx.addPartitionColsToBatch(value, partitionValues);
        }
        addPartitionCols = false;
      }
      if (!baseReader.next(null, vectorizedRowBatchBase)) {
        return false;
      }
    } catch (Exception e) {
      throw new IOException("error iterating", e);
    }
    if(!includeAcidColumns) {
      //if here, we don't need to filter anything wrt acid metadata columns
      //in fact, they are not even read from file/llap
      value.size = vectorizedRowBatchBase.size;
      value.selected = vectorizedRowBatchBase.selected;
      value.selectedInUse = vectorizedRowBatchBase.selectedInUse;
      copyFromBase(value);

      if (rowIsDeletedProjected) {
        rowIsDeletedVector.fill(false);
        int ix = rbCtx.findVirtualColumnNum(VirtualColumn.ROWISDELETED);
        value.cols[ix] = rowIsDeletedVector;
      }

      progress = baseReader.getProgress();
      return true;
    }
    // Once we have read the VectorizedRowBatchBase from the file, there are two kinds of cases
    // for which we might have to discard rows from the batch:
    // Case 1- when the row is created by a transaction that is not valid, or
    // Case 2- when the row has been deleted.
    // We will go through the batch to discover rows which match any of the cases and specifically
    // remove them from the selected vector. Of course, selectedInUse should also be set.

    BitSet selectedBitSet = new BitSet(vectorizedRowBatchBase.size);
    if (vectorizedRowBatchBase.selectedInUse) {
      // When selectedInUse is true, start with every bit set to false and selectively set
      // certain bits to true based on the selected[] vector.
      selectedBitSet.set(0, vectorizedRowBatchBase.size, false);
      for (int j = 0; j < vectorizedRowBatchBase.size; ++j) {
        int i = vectorizedRowBatchBase.selected[j];
        selectedBitSet.set(i);
      }
    } else {
      // When selectedInUse is set to false, everything in the batch is selected.
      selectedBitSet.set(0, vectorizedRowBatchBase.size, true);
    }
    ColumnVector[] innerRecordIdColumnVector = vectorizedRowBatchBase.cols;
    if (isOriginal) {
      // Handle synthetic row IDs for the original files.
      innerRecordIdColumnVector = handleOriginalFile(selectedBitSet, innerRecordIdColumnVector);
    } else {
      // Case 1- find rows which belong to write Ids that are not valid.
      findRecordsWithInvalidWriteIds(vectorizedRowBatchBase, selectedBitSet);
    }

    // Case 2- find rows which have been deleted.
    // if deleted rows should be fetched we clone the selectedBitSet to notDeletedBitSet.
    // Records marked by selectedBitSet should be filtered out from the result but notDeletedBitSet
    // should be appear with ROW__IS__DELETED = false
    BitSet notDeletedBitSet = fetchDeletedRows ? (BitSet) selectedBitSet.clone() : selectedBitSet;

    this.deleteEventRegistry.findDeletedRecords(innerRecordIdColumnVector,
        vectorizedRowBatchBase.size, notDeletedBitSet);

    if (selectedBitSet.cardinality() == vectorizedRowBatchBase.size) {
      // None of the cases above matched and everything is selected. Hence, we will use the
      // same values for the selected and selectedInUse.
      value.size = vectorizedRowBatchBase.size;
      value.selected = vectorizedRowBatchBase.selected;
      value.selectedInUse = vectorizedRowBatchBase.selectedInUse;
    } else {
      value.size = selectedBitSet.cardinality();
      value.selectedInUse = true;
      value.selected = new int[selectedBitSet.cardinality()];
      // This loop fills up the selected[] vector with all the index positions that are selected.
      for (int setBitIndex = selectedBitSet.nextSetBit(0), selectedItr = 0;
           setBitIndex >= 0;
           setBitIndex = selectedBitSet.nextSetBit(setBitIndex+1), ++selectedItr) {
        value.selected[selectedItr] = setBitIndex;
      }
    }

    copyFromBase(value);

    if (rowIdProjected) {
      int ix = rbCtx.findVirtualColumnNum(VirtualColumn.ROWID);
      value.cols[ix] = recordIdColumnVector;
    }
    if (rowIsDeletedProjected) {
      if (!fetchDeletedRows || notDeletedBitSet.cardinality() == vectorizedRowBatchBase.size) {
        rowIsDeletedVector.fill(false);
      } else {
        rowIsDeletedVector.set(notDeletedBitSet);
      }

      int ix = rbCtx.findVirtualColumnNum(VirtualColumn.ROWISDELETED);
      value.cols[ix] = rowIsDeletedVector;
    }
    progress = baseReader.getProgress();
    return true;
  }
  //get the 'data' cols and set in value as individual ColumnVector, then get
  //ColumnVectors for acid meta cols to create a single ColumnVector
  //representing RecordIdentifier and (optionally) set it in 'value'
  private void copyFromBase(VectorizedRowBatch value) {
    if (isOriginal) {
      /* Just copy the payload.  {@link recordIdColumnVector} has already been populated if needed */
      System.arraycopy(vectorizedRowBatchBase.cols, 0, value.cols, 0, value.getDataColumnCount());
      return;
    }
    if (isFlatPayload) {
      int payloadCol = includeAcidColumns ? OrcRecordUpdater.ROW : 0;
        // Ignore the struct column and just copy all the following data columns.
        System.arraycopy(vectorizedRowBatchBase.cols, payloadCol + 1, value.cols, 0,
            vectorizedRowBatchBase.cols.length - payloadCol - 1);
    } else {
      StructColumnVector payloadStruct =
          (StructColumnVector) vectorizedRowBatchBase.cols[OrcRecordUpdater.ROW];
      // Transfer columnVector objects from base batch to outgoing batch.
      System.arraycopy(payloadStruct.fields, 0, value.cols, 0, value.getDataColumnCount());
    }
    if (rowIdProjected) {
      // If deleted rows should be fetched the writeId belongs to the deleted record should be the one which actually did
      // the delete operation. Current and Original writeId of inserted records are equals.
      recordIdColumnVector.fields[0] = vectorizedRowBatchBase.cols[fetchDeletedRows ? OrcRecordUpdater.CURRENT_WRITEID : OrcRecordUpdater.ORIGINAL_WRITEID];
      recordIdColumnVector.fields[1] = vectorizedRowBatchBase.cols[OrcRecordUpdater.BUCKET];
      recordIdColumnVector.fields[2] = vectorizedRowBatchBase.cols[OrcRecordUpdater.ROW_ID];
    }
  }
  private ColumnVector[] handleOriginalFile(
      BitSet selectedBitSet, ColumnVector[] innerRecordIdColumnVector) throws IOException {
    /*
     * If there are deletes and reading original file, we must produce synthetic ROW_IDs in order
     * to see if any deletes apply
     */
    boolean needSyntheticRowId =
        needSyntheticRowIds(true, !deleteEventRegistry.isEmpty(), rowIdProjected);
    if(needSyntheticRowId) {
      assert syntheticProps != null : "" + syntheticProps;
      assert syntheticProps.getRowIdOffset() >= 0 : "" + syntheticProps;
      assert syntheticProps.getBucketProperty() >= 0 : "" + syntheticProps;
      if(innerReader == null) {
        throw new IllegalStateException(getClass().getName() + " requires " +
          org.apache.orc.RecordReader.class +
          " to handle original files that require ROW__IDs: " + rootPath);
      }
      /**
       * {@link RecordIdentifier#getWriteId()}
       */
      recordIdColumnVector.fields[0].noNulls = true;
      recordIdColumnVector.fields[0].isRepeating = true;
      ((LongColumnVector)recordIdColumnVector.fields[0]).vector[0] = syntheticProps.getSyntheticWriteId();
      /**
       * This is {@link RecordIdentifier#getBucketProperty()}
       * Also see {@link BucketCodec}
       */
      recordIdColumnVector.fields[1].noNulls = true;
      recordIdColumnVector.fields[1].isRepeating = true;
      ((LongColumnVector)recordIdColumnVector.fields[1]).vector[0] = syntheticProps.getBucketProperty();
      /**
       * {@link RecordIdentifier#getRowId()}
       */
      recordIdColumnVector.fields[2].noNulls = true;
      recordIdColumnVector.fields[2].isRepeating = false;
      long[] rowIdVector = ((LongColumnVector)recordIdColumnVector.fields[2]).vector;
      for(int i = 0; i < vectorizedRowBatchBase.size; i++) {
        //baseReader.getRowNumber() seems to point at the start of the batch todo: validate
        rowIdVector[i] = syntheticProps.getRowIdOffset() + innerReader.getRowNumber() + i;
      }
      //Now populate a structure to use to apply delete events
      innerRecordIdColumnVector = new ColumnVector[OrcRecordUpdater.FIELDS];
      innerRecordIdColumnVector[OrcRecordUpdater.ORIGINAL_WRITEID] = recordIdColumnVector.fields[0];
      innerRecordIdColumnVector[OrcRecordUpdater.BUCKET] = recordIdColumnVector.fields[1];
      innerRecordIdColumnVector[OrcRecordUpdater.ROW_ID] = recordIdColumnVector.fields[2];
      //these are insert events so (original txn == current) txn for all rows
      innerRecordIdColumnVector[OrcRecordUpdater.CURRENT_WRITEID] = recordIdColumnVector.fields[0];
    }
    if(syntheticProps.getSyntheticWriteId() > 0) {
      //"originals" (written before table was converted to acid) is considered written by
      // writeid:0 which is always committed so there is no need to check wrt invalid write Ids
      //But originals written by Load Data for example can be in base_x or delta_x_x so we must
      //check if 'x' is committed or not evn if ROW_ID is not needed in the Operator pipeline.
      if (needSyntheticRowId) {
        findRecordsWithInvalidWriteIds(innerRecordIdColumnVector,
            vectorizedRowBatchBase.size, selectedBitSet);
      } else {
        /*since ROW_IDs are not needed we didn't create the ColumnVectors to hold them but we
        * still have to check if the data being read is committed as far as current
        * reader (transactions) is concerned.  Since here we are reading 'original' schema file,
        * all rows in it have been created by the same txn, namely 'syntheticProps.syntheticWriteId'
        */
        if (!validWriteIdList.isWriteIdValid(syntheticProps.getSyntheticWriteId())) {
          selectedBitSet.clear(0, vectorizedRowBatchBase.size);
        }
      }
    }
    return innerRecordIdColumnVector;
  }

  private void findRecordsWithInvalidWriteIds(VectorizedRowBatch batch, BitSet selectedBitSet) {
    findRecordsWithInvalidWriteIds(batch.cols, batch.size, selectedBitSet);
  }

  private void findRecordsWithInvalidWriteIds(ColumnVector[] cols, int size, BitSet selectedBitSet) {
    if (cols[OrcRecordUpdater.CURRENT_WRITEID].isRepeating) {
      // When we have repeating values, we can unset the whole bitset at once
      // if the repeating value is not a valid write id.
      long currentWriteIdForBatch = ((LongColumnVector)
          cols[OrcRecordUpdater.CURRENT_WRITEID]).vector[0];
      if (!validWriteIdList.isWriteIdValid(currentWriteIdForBatch)) {
        selectedBitSet.clear(0, size);
      }
      return;
    }
    long[] currentWriteIdVector =
        ((LongColumnVector) cols[OrcRecordUpdater.CURRENT_WRITEID]).vector;
    // Loop through the bits that are set to true and mark those rows as false, if their
    // current write ids are not valid.
    for (int setBitIndex = selectedBitSet.nextSetBit(0);
        setBitIndex >= 0;
        setBitIndex = selectedBitSet.nextSetBit(setBitIndex+1)) {
      if (!validWriteIdList.isWriteIdValid(currentWriteIdVector[setBitIndex])) {
        selectedBitSet.clear(setBitIndex);
      }
   }
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public VectorizedRowBatch createValue() {
    return rbCtx.createVectorizedRowBatch();
  }

  @Override
  public long getPos() throws IOException {
    return offset + (long) (progress * length);
  }

  @Override
  public void close() throws IOException {
    try {
      this.baseReader.close();
    } finally {
      this.deleteEventRegistry.close();
    }
  }

  @Override
  public float getProgress() throws IOException {
    return progress;
  }

  @VisibleForTesting
  DeleteEventRegistry getDeleteEventRegistry() {
    return deleteEventRegistry;
  }

  /**
   * An interface that can determine which rows have been deleted
   * from a given vectorized row batch. Implementations of this interface
   * will read the delete delta files and will create their own internal
   * data structures to maintain record ids of the records that got deleted.
   */
  protected interface DeleteEventRegistry {
    /**
     * Modifies the passed bitset to indicate which of the rows in the batch
     * have been deleted. Assumes that the batch.size is equal to bitset size.
     * @param cols
     * @param size
     * @param selectedBitSet
     * @throws IOException
     */
    public void findDeletedRecords(ColumnVector[] cols, int size, BitSet selectedBitSet) throws IOException;

    /**
     * The close() method can be called externally to signal the implementing classes
     * to free up resources.
     * @throws IOException
     */
    public void close() throws IOException;

    /**
     * @return {@code true} if no delete events were found
     */
    boolean isEmpty();
  }

  /**
   * An implementation for DeleteEventRegistry that opens the delete delta files all
   * at once, and then uses the sort-merge algorithm to maintain a sorted list of
   * delete events. This internally uses the OrcRawRecordMerger and maintains a constant
   * amount of memory usage, given the number of delete delta files. Therefore, this
   * implementation will be picked up when the memory pressure is high.
   *
   * Don't bother to use KeyInterval from split here because since this doesn't
   * buffer delete events in memory.
   */
  static class SortMergedDeleteEventRegistry implements DeleteEventRegistry {
    private OrcRawRecordMerger deleteRecords;
    private OrcRawRecordMerger.ReaderKey deleteRecordKey;
    private OrcStruct deleteRecordValue;
    private Boolean isDeleteRecordAvailable = null;
    private ValidWriteIdList validWriteIdList;
    private final boolean fetchDeletedRows;

    SortMergedDeleteEventRegistry(JobConf conf, OrcSplit orcSplit,
                                  Reader.Options readerOptions, boolean fetchDeletedRows) throws IOException {
      this.fetchDeletedRows = fetchDeletedRows;
      Map<String, AcidInputFormat.DeltaMetaData> pathToDeltaMetaData = new HashMap<>();
      final Path[] deleteDeltas = getDeleteDeltaDirsFromSplit(orcSplit, pathToDeltaMetaData);
      if (deleteDeltas.length > 0) {
        int bucket = AcidUtils.parseBucketId(orcSplit.getPath());
        String txnString = conf.get(ValidWriteIdList.VALID_WRITEIDS_KEY);
        this.validWriteIdList
                = (txnString == null) ? new ValidReaderWriteIdList() : new ValidReaderWriteIdList(txnString);
        LOG.debug("Using SortMergedDeleteEventRegistry");
        Map<String, Integer> deltaToAttemptId = AcidUtils.getDeltaToAttemptIdMap(pathToDeltaMetaData, deleteDeltas, bucket);
        OrcRawRecordMerger.Options mergerOptions = new OrcRawRecordMerger.Options().isDeleteReader(true);
        assert !orcSplit.isOriginal() : "If this now supports Original splits, set up mergeOptions properly";
        this.deleteRecords = new OrcRawRecordMerger(conf, true, null, false, bucket, validWriteIdList, readerOptions,
            deleteDeltas, mergerOptions, deltaToAttemptId);
        this.deleteRecordKey = new OrcRawRecordMerger.ReaderKey();
        this.deleteRecordValue = this.deleteRecords.createValue();
        // Initialize the first value in the delete reader.
        this.isDeleteRecordAvailable = this.deleteRecords.next(deleteRecordKey, deleteRecordValue);
      } else {
        this.isDeleteRecordAvailable = false;
        this.deleteRecordKey = null;
        this.deleteRecordValue = null;
        this.deleteRecords = null;
      }
    }

    @Override
    public boolean isEmpty() {
      if(isDeleteRecordAvailable == null) {
        throw new IllegalStateException("Not yet initialized");
      }
      return !isDeleteRecordAvailable;
    }
    @Override
    public void findDeletedRecords(ColumnVector[] cols, int size, BitSet selectedBitSet)
        throws IOException {
      if (!isDeleteRecordAvailable) {
        return;
      }

      long[] originalWriteId =
          cols[OrcRecordUpdater.ORIGINAL_WRITEID].isRepeating ? null
              : ((LongColumnVector) cols[OrcRecordUpdater.ORIGINAL_WRITEID]).vector;
      long[] bucket =
          cols[OrcRecordUpdater.BUCKET].isRepeating ? null
              : ((LongColumnVector) cols[OrcRecordUpdater.BUCKET]).vector;
      long[] rowId =
          cols[OrcRecordUpdater.ROW_ID].isRepeating ? null
              : ((LongColumnVector) cols[OrcRecordUpdater.ROW_ID]).vector;

      // The following repeatedX values will be set, if any of the columns are repeating.
      long repeatedOriginalWriteId = (originalWriteId != null) ? -1
          : ((LongColumnVector) cols[OrcRecordUpdater.ORIGINAL_WRITEID]).vector[0];
      long repeatedBucket = (bucket != null) ? -1
          : ((LongColumnVector) cols[OrcRecordUpdater.BUCKET]).vector[0];
      long repeatedRowId = (rowId != null) ? -1
          : ((LongColumnVector) cols[OrcRecordUpdater.ROW_ID]).vector[0];
      LongColumnVector currentWriteIdVector = (LongColumnVector) cols[OrcRecordUpdater.CURRENT_WRITEID];

      // Get the first valid row in the batch still available.
      int firstValidIndex = selectedBitSet.nextSetBit(0);
      if (firstValidIndex == -1) {
        return; // Everything in the batch has already been filtered out.
      }
      RecordIdentifier firstRecordIdInBatch =
          new RecordIdentifier(
              originalWriteId != null ? originalWriteId[firstValidIndex] : repeatedOriginalWriteId,
              bucket != null ? (int) bucket[firstValidIndex] : (int) repeatedBucket,
              rowId != null ? (int)  rowId[firstValidIndex] : repeatedRowId);

      // Get the last valid row in the batch still available.
      int lastValidIndex = selectedBitSet.previousSetBit(size - 1);
      RecordIdentifier lastRecordIdInBatch =
          new RecordIdentifier(
              originalWriteId != null ? originalWriteId[lastValidIndex] : repeatedOriginalWriteId,
              bucket != null ? (int) bucket[lastValidIndex] : (int) repeatedBucket,
              rowId != null ? (int)  rowId[lastValidIndex] : repeatedRowId);

      // We must iterate over all the delete records, until we find one record with
      // deleteRecord >= firstRecordInBatch or until we exhaust all the delete records.
      while (deleteRecordKey.compareRow(firstRecordIdInBatch) == -1) {
        isDeleteRecordAvailable = deleteRecords.next(deleteRecordKey, deleteRecordValue);
        if (!isDeleteRecordAvailable) return; // exhausted all delete records, return.
      }

      // If we are here, then we have established that firstRecordInBatch <= deleteRecord.
      // Now continue marking records which have been deleted until we reach the end of the batch
      // or we exhaust all the delete records.

      int currIndex = firstValidIndex;
      RecordIdentifier currRecordIdInBatch = new RecordIdentifier();
      while (isDeleteRecordAvailable && currIndex != -1 && currIndex <= lastValidIndex) {
        currRecordIdInBatch.setValues(
            (originalWriteId != null) ? originalWriteId[currIndex] : repeatedOriginalWriteId,
            (bucket != null) ? (int) bucket[currIndex] : (int) repeatedBucket,
            (rowId != null) ? rowId[currIndex] : repeatedRowId);

        if (deleteRecordKey.compareRow(currRecordIdInBatch) == 0) {
          // When deleteRecordId == currRecordIdInBatch, this record in the batch has been deleted.
          selectedBitSet.clear(currIndex);
          if (fetchDeletedRows) {
            currentWriteIdVector.vector[currIndex] = deleteRecordKey.getCurrentWriteId();
            cols[OrcRecordUpdater.CURRENT_WRITEID].isRepeating = false;
          }
          currIndex = selectedBitSet.nextSetBit(currIndex + 1); // Move to next valid index.
        } else if (deleteRecordKey.compareRow(currRecordIdInBatch) == 1) {
          // When deleteRecordId > currRecordIdInBatch, we have to move on to look at the
          // next record in the batch.
          // But before that, can we short-circuit and skip the entire batch itself
          // by checking if the deleteRecordId > lastRecordInBatch?
          if (deleteRecordKey.compareRow(lastRecordIdInBatch) == 1) {
            return; // Yay! We short-circuited, skip everything remaining in the batch and return.
          }
          currIndex = selectedBitSet.nextSetBit(currIndex + 1); // Move to next valid index.
        } else {
          // We have deleteRecordId < currRecordIdInBatch, we must now move on to find
          // next the larger deleteRecordId that can possibly match anything in the batch.
          isDeleteRecordAvailable = deleteRecords.next(deleteRecordKey, deleteRecordValue);
        }
      }
    }

    @Override
    public void close() throws IOException {
      if (this.deleteRecords != null) {
        this.deleteRecords.close();
      }
    }
  }

  /**
   * An implementation for DeleteEventRegistry that optimizes for performance by loading
   * all the delete events into memory at once from all the delete delta files.
   * It starts by reading all the delete events through a regular sort merge logic
   * into 3 vectors- one for original Write id (owid), one for bucket property and one for
   * row id.  See {@link BucketCodec} for more about bucket property.
   * The owids are likely to be repeated very often, as a single transaction
   * often deletes thousands of rows. Hence, the owid vector is compressed to only store the
   * toIndex and fromIndex ranges in the larger row id vector. Now, querying whether a
   * record id is deleted or not, is done by performing a binary search on the
   * compressed owid range. If a match is found, then a binary search is then performed on
   * the larger rowId vector between the given toIndex and fromIndex. Of course, there is rough
   * heuristic that prevents creation of an instance of this class if the memory pressure is high.
   * The SortMergedDeleteEventRegistry is then the fallback method for such scenarios.
   */
   static class ColumnizedDeleteEventRegistry implements DeleteEventRegistry {
    /**
     * A simple wrapper class to hold the (owid, bucketProperty, rowId) pair.
     */
    static class DeleteRecordKey implements Comparable<DeleteRecordKey> {
      private long originalWriteId;
      private long currentWriteId;
      /**
       * see {@link BucketCodec}
       */
      private int bucketProperty;
      private long rowId;
      DeleteRecordKey() {
        this.originalWriteId = -1;
        this.currentWriteId = -1;
        this.rowId = -1;
      }
      public void set(long owid, int bucketProperty, long rowId, long cwid) {
        this.originalWriteId = owid;
        this.bucketProperty = bucketProperty;
        this.rowId = rowId;
        this.currentWriteId = cwid;
      }

      @Override
      public int compareTo(DeleteRecordKey other) {
        if (other == null) {
          return -1;
        }
        return compareTo(other.originalWriteId, other.bucketProperty, other.rowId);
      }

      private int compareTo(RecordIdentifier other) {
        if (other == null) {
          return -1;
        }
        return compareTo(other.getWriteId(), other.getBucketProperty(), other.getRowId());
      }

      private int compareTo(long oOriginalWriteId, int oBucketProperty, long oRowId) {
        if (originalWriteId != oOriginalWriteId) {
          return originalWriteId < oOriginalWriteId ? -1 : 1;
        }
        if(bucketProperty != oBucketProperty) {
          return bucketProperty < oBucketProperty ? -1 : 1;
        }
        if (rowId != oRowId) {
          return rowId < oRowId ? -1 : 1;
        }
        return 0;
      }

      @Override
      public String toString() {
        return "DeleteRecordKey(" + originalWriteId + "," +
            RecordIdentifier.bucketToString(bucketProperty) + "," + rowId +")";
      }
    }

    /**
     * This class actually reads the delete delta files in vectorized row batches.
     * For every call to next(), it returns the next smallest record id in the file if available.
     * Internally, the next() buffers a row batch and maintains an index pointer, reading the
     * next batch when the previous batch is exhausted.
     *
     * For unbucketed tables this will currently return all delete events.  Once we trust that
     * the N in bucketN for "base" spit is reliable, all delete events not matching N can be skipped.
     */
    static class DeleteReaderValue {

      // Produces column indices for all ACID columns, except for the last one, which is the ROW struct.
      private static final List<Integer> DELETE_DELTA_INCLUDE_COLUMNS =
          IntStream.range(0, OrcInputFormat.getRootColumn(false) - 1).boxed().collect(toList());

      private static final TypeDescription DELETE_DELTA_EMPTY_STRUCT =
          new TypeDescription(TypeDescription.Category.STRUCT);

      private VectorizedRowBatch batch;
      private RecordReader recordReader;
      private int indexPtrInBatch;
      private final int bucketForSplit; // The bucket value should be same for all the records.
      private final ValidWriteIdList validWriteIdList;
      private boolean isBucketPropertyRepeating;
      private final boolean isBucketedTable;
      private final Path deleteDeltaFile;
      private final OrcRawRecordMerger.KeyInterval keyInterval;
      private final OrcSplit orcSplit;
      /**
       * total number in the file
       */
      private final long numEvents;
      /**
       * number of events lifted from disk
       * some may be skipped due to PPD
       */
      private long numEventsFromDisk = 0;
      /**
       * number of events actually loaded in memory
       */
      private long numEventsLoaded = 0;

      DeleteReaderValue(Reader deleteDeltaReader, Path deleteDeltaFile, Reader.Options readerOptions, int bucket,
          ValidWriteIdList validWriteIdList, boolean isBucketedTable, final JobConf conf,
          OrcRawRecordMerger.KeyInterval keyInterval, OrcSplit orcSplit, long numRows, CacheTag cacheTag, Object fileId)
          throws IOException {
        this.deleteDeltaFile = deleteDeltaFile;

        // ACID schema with empty row struct will be used for reading delete deltas
        TypeDescription acidEmptyStructSchema = SchemaEvolution.createEventSchema(DELETE_DELTA_EMPTY_STRUCT);

        // If configured try with LLAP reader. This may still return null if LLAP record reader can't be created
        // e.g. due to unsupported schema evolution
        if (shouldReadDeleteDeltasWithLlap(conf, false)) {
          this.recordReader = getLlapRecordReader(deleteDeltaFile, readerOptions, conf, cacheTag, fileId);
        }
        if (this.recordReader == null) {
          if (deleteDeltaReader == null) {
            // The only case this happens is if using LLAP caching is turned on for delete deltas, and OrcTail of
            // delete delta files were served out by LLAP, but a record reader for DD content could not be created.
            deleteDeltaReader = OrcFile.createReader(deleteDeltaFile, OrcFile.readerOptions(conf));
          }
          // To prevent the record reader from allocating empty vectors for the actual table schema in its batch, we
          // specify the original schema to be an empty struct, thus only ACID columns will be read.
          // Note: clone() here makes sure not to alter the original options object, which is also used by
          // SortMergeDeleteEventRegistry should a DeleteEventsOverflowMemoryException be thrown...
          readerOptions = readerOptions.clone().schema(DELETE_DELTA_EMPTY_STRUCT).include(new boolean[] { true });
          this.recordReader = deleteDeltaReader.rowsOptions(readerOptions, conf);
        }

        this.bucketForSplit = bucket;

        final boolean useDecimal64ColumnVector = HiveConf.getVar(conf, ConfVars
          .HIVE_VECTORIZED_INPUT_FORMAT_SUPPORTS_ENABLED).equalsIgnoreCase("decimal_64");
        if (useDecimal64ColumnVector) {
          this.batch = acidEmptyStructSchema.createRowBatchV2();
        } else {
          this.batch = acidEmptyStructSchema.createRowBatch();
        }

        if (!recordReader.nextBatch(batch)) { // Read the first batch.
          this.batch = null; // Oh! the first batch itself was null. Close the reader.
        }
        this.indexPtrInBatch = 0;
        this.validWriteIdList = validWriteIdList;
        this.isBucketedTable = isBucketedTable;
        if(batch != null) {
          checkBucketId();//check 1st batch
        }
        this.keyInterval = keyInterval;
        this.orcSplit = orcSplit;
        this.numEvents = numRows;
        LOG.debug("Num events stats({},x,x)", numEvents);
      }

      public boolean next(DeleteRecordKey deleteRecordKey) throws IOException {
        if (batch == null) {
          return false;
        }
        boolean isValidNext = false;
        while (!isValidNext) {
          if (indexPtrInBatch >= batch.size) {
            // We have exhausted our current batch, read the next batch.
            if (recordReader.nextBatch(batch)) {
              checkBucketId();
              indexPtrInBatch = 0; // After reading the batch, reset the pointer to beginning.
            } else {
              return false; // no more batches to read, exhausted the reader.
            }
          }
          long currentWriteId = setCurrentDeleteKey(deleteRecordKey);
          if(!isBucketPropertyRepeating) {
            checkBucketId(deleteRecordKey.bucketProperty);
          }
          ++indexPtrInBatch;
          numEventsFromDisk++;
          if(!isDeleteEventInRange(keyInterval, deleteRecordKey)) {
            continue;
          }
          if (validWriteIdList.isWriteIdValid(currentWriteId)) {
            isValidNext = true;
          }
        }
        numEventsLoaded++;
        return true;
      }
      static boolean isDeleteEventInRange(
          OrcRawRecordMerger.KeyInterval keyInterval,
          DeleteRecordKey deleteRecordKey) {
        if(keyInterval.getMinKey() != null &&
            deleteRecordKey.compareTo(keyInterval.getMinKey()) < 0) {
          //current deleteEvent is < than minKey
          return false;
        }
        if(keyInterval.getMaxKey() != null &&
            deleteRecordKey.compareTo(keyInterval.getMaxKey()) > 0) {
          //current deleteEvent is > than maxKey
          return false;
        }
        return true;
      }
      public void close() throws IOException {
        this.recordReader.close();
        LOG.debug("Num events stats({},{},{})",
            numEvents, numEventsFromDisk, numEventsLoaded);
      }
      private long setCurrentDeleteKey(DeleteRecordKey deleteRecordKey) {
        int originalWriteIdIndex =
          batch.cols[OrcRecordUpdater.ORIGINAL_WRITEID].isRepeating ? 0 : indexPtrInBatch;
        long originalWriteId
                = ((LongColumnVector) batch.cols[OrcRecordUpdater.ORIGINAL_WRITEID]).vector[originalWriteIdIndex];
        int bucketPropertyIndex =
          batch.cols[OrcRecordUpdater.BUCKET].isRepeating ? 0 : indexPtrInBatch;
        int bucketProperty = (int)((LongColumnVector)batch.cols[OrcRecordUpdater.BUCKET]).vector[bucketPropertyIndex];
        long rowId = ((LongColumnVector) batch.cols[OrcRecordUpdater.ROW_ID]).vector[indexPtrInBatch];
        int currentWriteIdIndex
                = batch.cols[OrcRecordUpdater.CURRENT_WRITEID].isRepeating ? 0 : indexPtrInBatch;
        long currentWriteId
                = ((LongColumnVector) batch.cols[OrcRecordUpdater.CURRENT_WRITEID]).vector[currentWriteIdIndex];
        deleteRecordKey.set(originalWriteId, bucketProperty, rowId, currentWriteId);
        return currentWriteId;
      }
      private void checkBucketId() throws IOException {
        isBucketPropertyRepeating = batch.cols[OrcRecordUpdater.BUCKET].isRepeating;
        if(isBucketPropertyRepeating) {
          int bucketPropertyFromRecord = (int)((LongColumnVector)
            batch.cols[OrcRecordUpdater.BUCKET]).vector[0];
          checkBucketId(bucketPropertyFromRecord);
        }
      }
      /**
       * Whenever we are reading a batch, we must ensure that all the records in the batch
       * have the same bucket id as the bucket id of the split. If not, throw exception.
       */
      private void checkBucketId(int bucketPropertyFromRecord) throws IOException {
        int bucketIdFromRecord = BucketCodec.determineVersion(bucketPropertyFromRecord)
          .decodeWriterId(bucketPropertyFromRecord);
        if(bucketIdFromRecord != bucketForSplit) {
          DeleteRecordKey dummy = new DeleteRecordKey();
          setCurrentDeleteKey(dummy);
          throw new IOException("Corrupted records with different bucket ids "
              + "from the containing bucket file found! Expected bucket id "
              + bucketForSplit + ", however found " + dummy
              + ".  (" + orcSplit + "," + deleteDeltaFile + ")");
        }
      }

      @Override
      public String toString() {
        return "{recordReader=" + recordReader + ", isBucketPropertyRepeating=" + isBucketPropertyRepeating +
            ", bucketForSplit=" + bucketForSplit + ", isBucketedTable=" + isBucketedTable + "}";
      }

      private static RecordReader getLlapRecordReader(Path deleteDeltaFile, org.apache.orc.Reader.Options readerOptions, JobConf conf, CacheTag tag, Object fileId)
          throws IOException {
        JobConf c = new JobConf(conf);
        // DeleteReaderValue will actually read the ACID parts itself, thus as far as LLAP concerned this delete
        // delta should be deemed as an 'original' ORC file.
        HiveConf.setBoolVar(c, ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN, false);

        // Jobconf may contain predicate pushdown for the original table, this is not applicable for delete delta
        c.unset(TableScanDesc.FILTER_EXPR_CONF_STR);
        c.unset(ConvertAstToSearchArg.SARG_PUSHDOWN);

        // Schema info in this job conf relates to the table schema without ACID cols. Unsetting them forces LLAP to
        // use the file schema instead which is the correct way to handle delete delta files, as there's no such
        // thing as logical schema for them.
        HiveConf.setBoolVar(c, ConfVars.HIVE_SCHEMA_EVOLUTION, false);
        c.unset(serdeConstants.LIST_COLUMNS);
        c.unset(serdeConstants.LIST_COLUMN_TYPES);

        // Apply delete delta SARG if any
        SearchArgument deleteDeltaSarg = readerOptions.getSearchArgument();
        if (deleteDeltaSarg != null) {
          c.set(ConvertAstToSearchArg.SARG_PUSHDOWN, sargToKryo(deleteDeltaSarg));
        }
        return wrapLlapVectorizedRecordReader(
            LlapProxy.getIo().llapVectorizedOrcReaderForPath(fileId, deleteDeltaFile, tag,
            DELETE_DELTA_INCLUDE_COLUMNS, c, 0L, Long.MAX_VALUE, null)
        );
      }

      /**
       * Wraps an LLAP vectorized record reader to a simple ORC record reader for our delete delta reading effort here.
       * Assumes VectorizedRowBatch type is used in upstream record reader, will not iterate row-by-row.
       * @param rr
       * @return
       */
      private static RecordReader wrapLlapVectorizedRecordReader(
          org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch> rr) {
        if (rr == null) {
          return null;
        }
        return new RecordReader() {
          @Override
          public boolean hasNext() throws IOException {
            throw new UnsupportedOperationException();
          }

          @Override
          public Object next(Object previous) throws IOException {
            if (previous instanceof VectorizedRowBatch) {
              return nextBatch((VectorizedRowBatch) previous);
            } else {
              throw new UnsupportedOperationException();
            }
          }

          @Override
          public boolean nextBatch(VectorizedRowBatch vectorizedRowBatch) throws IOException {
            return rr.next(null ,vectorizedRowBatch);
          }

          @Override
          public long getRowNumber() throws IOException {
            throw new UnsupportedOperationException();
          }

          @Override
          public float getProgress() throws IOException {
            return rr.getProgress();
          }

          @Override
          public void close() throws IOException {
            rr.close();
          }

          @Override
          public void seekToRow(long l) throws IOException {
            throw new UnsupportedOperationException();
          }
        };
      }
    }
    /**
     * A CompressedOwid class stores a compressed representation of the original
     * write ids (owids) read from the delete delta files. Since the record ids
     * are sorted by (owid, rowId) and owids are highly likely to be repetitive, it is
     * efficient to compress them as a CompressedOwid that stores the fromIndex and
     * the toIndex. These fromIndex and toIndex reference the larger vector formed by
     * concatenating the correspondingly ordered rowIds.
     */
    private static final class CompressedOwid implements Comparable<CompressedOwid> {
      final long originalWriteId;
      final int bucketProperty;
      final int fromIndex; // inclusive
      int toIndex; // exclusive

      CompressedOwid(long owid, int bucketProperty, int fromIndex, int toIndex) {
        this.originalWriteId = owid;
        this.bucketProperty = bucketProperty;
        this.fromIndex = fromIndex;
        this.toIndex = toIndex;
      }

      @Override
      public int compareTo(CompressedOwid other) {
        // When comparing the CompressedOwid, the one with the lesser value is smaller.
        if (originalWriteId != other.originalWriteId) {
          return originalWriteId < other.originalWriteId ? -1 : 1;
        }
        if(bucketProperty != other.bucketProperty) {
          return bucketProperty < other.bucketProperty ? -1 : 1;
        }
        return 0;
      }
    }

    /**
     * Food for thought:
     * this is a bit problematic - in order to load ColumnizedDeleteEventRegistry we still open
     * all delete deltas at once - possibly causing OOM same as for {@link SortMergedDeleteEventRegistry}
     * which uses {@link OrcRawRecordMerger}.  Why not load all delete_delta sequentially.  Each
     * dd is sorted by {@link RecordIdentifier} so we could create a BTree like structure where the
     * 1st level is an array of originalWriteId where each entry points at an array
     * of bucketIds where each entry points at an array of rowIds.  We could probably use ArrayList
     * to manage insertion as the structure is built (LinkedList?).  This should reduce memory
     * footprint (as far as OrcReader to a single reader) - probably bad for LLAP IO
     * Or much simpler, make compaction of delete deltas very aggressive so that
     * we never have move than a few delete files to read.
     */
    private final TreeMap<DeleteRecordKey, DeleteReaderValue> sortMerger;
    private long[] rowIds;
    private OriginalWriteIds writeIds;
    private final Boolean isEmpty;
    private final int maxEventsInMemory;
    private final OrcSplit orcSplit;
    private final boolean testMode;

    static class OriginalWriteIds {
      private final CompressedOwid[] compressedOwids;

      OriginalWriteIds(CompressedOwid[] compressedOwids) {
        this.compressedOwids = compressedOwids;
      }

      boolean isEmpty() {
        return compressedOwids == null;
      }

      protected int indexOfRowId(long owid, int bucketProperty, long rowId, long[] rowIds) {
        if (isEmpty()) {
          return -1;
        }
        // To find if a given (owid, rowId) pair is deleted or not, we perform
        // two binary searches at most. The first binary search is on the
        // compressed owids. If a match is found, only then we do the next
        // binary search in the larger rowId vector between the given toIndex & fromIndex.

        // Check if owid is outside the range of all owids present.
        if (owid < compressedOwids[0].originalWriteId
                || owid > compressedOwids[compressedOwids.length - 1].originalWriteId) {
          return -1;
        }
        // Create a dummy key for searching the owid/bucket in the compressed owid ranges.
        CompressedOwid key = new CompressedOwid(owid, bucketProperty, -1, -1);
        int pos = Arrays.binarySearch(compressedOwids, key);
        if (pos >= 0) {
          // Owid with the given value found! Searching now for rowId...
          key = compressedOwids[pos]; // Retrieve the actual CompressedOwid that matched.
          // Check if rowId is outside the range of all rowIds present for this owid.
          if (rowId < rowIds[key.fromIndex]
                  || rowId > rowIds[key.toIndex - 1]) {
            return -1;
          }
          return Arrays.binarySearch(rowIds, key.fromIndex, key.toIndex, rowId);
        }
        return -1;
      }

      public void markAsDeleted(long owid, int bucketProperty, long rowId, long[] rowIds, int setBitIndex,
                                BitSet selectedBitSet, LongColumnVector currentWriteIdVector) {
        long currentWriteId = indexOfRowId(owid, bucketProperty, rowId, rowIds);
        if (currentWriteId >= 0) {
          selectedBitSet.clear(setBitIndex);
        }
      }
    }

    static class BothWriteIds extends OriginalWriteIds {
      private final TreeMap<Integer, Long> compressedCwids;

      BothWriteIds(CompressedOwid[] compressedOwids, TreeMap<Integer, Long> compressedCwids) {
        super(compressedOwids);
        this.compressedCwids = compressedCwids;
      }

      private long findCurrentWriteId(long owid, int bucketProperty, long rowId, long[] rowIds) {
        if (isEmpty()) {
          return -1;
        }

        int rowIdx = indexOfRowId(owid, bucketProperty, rowId, rowIds);
        if (rowIdx < 0) {
          return -1;
        }

        // rowId also found!
        Map.Entry<Integer, Long> cEntry = compressedCwids.floorEntry(rowIdx);
        return cEntry == null ? -1 : cEntry.getValue();
      }

      @Override
      public void markAsDeleted(long owid, int bucketProperty, long rowId, long[] rowIds, int setBitIndex,
                                BitSet selectedBitSet, LongColumnVector currentWriteIdVector) {
        long currentWriteId = findCurrentWriteId(owid, bucketProperty, rowId, rowIds);
        if (currentWriteId >= 0) {
          selectedBitSet.clear(setBitIndex);
          currentWriteIdVector.vector[setBitIndex] = currentWriteId;
          currentWriteIdVector.isRepeating = false;
        }
      }
    }

    static class OriginalWriteIdLoader {
      private final List<CompressedOwid> compressedOwids;
      private CompressedOwid lastCo;

      OriginalWriteIdLoader() {
        this.compressedOwids = new ArrayList<>();
        this.lastCo = null;
      }

      public void add(int index, DeleteRecordKey deleteRecordKey) {
        long owid = deleteRecordKey.originalWriteId;
        int bp = deleteRecordKey.bucketProperty;

        if (lastCo == null || lastCo.originalWriteId != owid || lastCo.bucketProperty != bp) {
          if (lastCo != null) {
            lastCo.toIndex = index; // Finalize the previous record.
          }
          lastCo = new CompressedOwid(owid, bp, index, -1);
          compressedOwids.add(lastCo);
        }
      }

      public OriginalWriteIds done(int index) {
        if (lastCo != null) {
          lastCo.toIndex = index; // Finalize the last record.
          lastCo = null;
        }

        return new OriginalWriteIds(compressedOwids.toArray(new CompressedOwid[0]));
      }
    }

    static class OriginalAndCurrentWriteIdLoader extends OriginalWriteIdLoader {
      private final TreeMap<Integer, Long> compressedCwids;
      private long lastCwid;
      private int lastBucketProperty;

      OriginalAndCurrentWriteIdLoader() {
        this.compressedCwids = new TreeMap<>();
        this.lastCwid = -1;
        this.lastBucketProperty = -1;
      }

      @Override
      public void add(int index, DeleteRecordKey deleteRecordKey) {
        super.add(index, deleteRecordKey);
        long cwid = deleteRecordKey.currentWriteId;
        int bp = deleteRecordKey.bucketProperty;

        if (lastCwid != cwid || lastBucketProperty != bp) {
          compressedCwids.put(index, cwid);
          lastCwid = cwid;
        }
      }

      public BothWriteIds done(int index) {
        compressedCwids.put(index, -1L);
        return new BothWriteIds(super.done(index).compressedOwids, compressedCwids);
      }
    }


    ColumnizedDeleteEventRegistry(JobConf conf, OrcSplit orcSplit,
                                  Reader.Options readerOptions,
                                  OrcRawRecordMerger.KeyInterval keyInterval,
                                  CacheTag cacheTag,
                                  OriginalWriteIdLoader writeIdLoader)
        throws IOException, DeleteEventsOverflowMemoryException {
      this.testMode = conf.getBoolean(ConfVars.HIVE_IN_TEST.varname, false);
      int bucket = AcidUtils.parseBucketId(orcSplit.getPath());
      String txnString = conf.get(ValidWriteIdList.VALID_WRITEIDS_KEY);
      ValidWriteIdList validWriteIdList = (txnString == null) ? new ValidReaderWriteIdList() : new ValidReaderWriteIdList(txnString);
      LOG.debug("Using ColumnizedDeleteEventRegistry");
      this.sortMerger = new TreeMap<>();
      this.rowIds = null;
      this.writeIds = null;
      maxEventsInMemory = HiveConf
          .getIntVar(conf, ConfVars.HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY);
      final boolean isBucketedTable  = conf.getInt(hive_metastoreConstants.BUCKET_COUNT, 0) > 0;
      this.orcSplit = orcSplit;

      try {
        if (orcSplit.getDeltas().size() > 0) {
          AcidOutputFormat.Options orcSplitMinMaxWriteIds =
              AcidUtils.parseBaseOrDeltaBucketFilename(orcSplit.getPath(), conf);
          int totalDeleteEventCount = 0;
          for (AcidInputFormat.DeltaMetaData deltaMetaData : orcSplit.getDeltas()) {
            // We got one path for each statement in a multiStmt transaction
            for (Pair<Path, Integer> deleteDeltaDir : deltaMetaData.getPaths(orcSplit.getRootDir())) {
              Integer stmtId = deleteDeltaDir.getRight();
              if (!isQualifiedDeleteDeltaForSplit(orcSplitMinMaxWriteIds, deltaMetaData, stmtId)) {
                LOG.debug("Skipping delete delta dir {}", deleteDeltaDir);
                continue;
              }
              Path deleteDeltaPath = deleteDeltaDir.getLeft();
              for (AcidInputFormat.DeltaFileMetaData fileMetaData : deltaMetaData.getDeltaFilesForStmtId(stmtId)) {
                Path deleteDeltaFile = fileMetaData.getPath(deleteDeltaPath, bucket);
                Object fileId = fileMetaData.getFileId(deleteDeltaFile, bucket, conf);
                try (ReaderData readerData = getOrcReaderData(deleteDeltaFile, conf, cacheTag, fileId)) {
                  OrcTail orcTail = readerData.orcTail;
                  long numRows = orcTail.getFooter().getNumberOfRows();
                  if (numRows <= 0) {
                    continue; // just a safe check to ensure that we are not reading empty delete files.
                  }
                  OrcRawRecordMerger.KeyInterval deleteKeyInterval = findDeleteMinMaxKeys(orcTail, deleteDeltaFile);
                  if (!deleteKeyInterval.isIntersects(keyInterval)) {
                    // If there is no intersection between data and delete delta, do not read delete file
                    continue;
                  }

                  totalDeleteEventCount += numRows;

                  DeleteReaderValue deleteReaderValue = null;

                  // If reader is set, then it got set while retrieving the ORC tail, because reading was not possible
                  // with LLAP. In this case we continue with this reader. In other cases we rely on LLAP to read and
                  // cache delete delta files for us, so we won't create a reader instance ourselves here.
                  if (readerData.reader == null) {
                    assert shouldReadDeleteDeltasWithLlap(conf, true);
                  }
                  deleteReaderValue = new DeleteReaderValue(readerData.reader, deleteDeltaFile, readerOptions, bucket,
                      validWriteIdList, isBucketedTable, conf, keyInterval, orcSplit, numRows, cacheTag, fileId);

                  DeleteRecordKey deleteRecordKey = new DeleteRecordKey();
                  if (deleteReaderValue.next(deleteRecordKey)) {
                    sortMerger.put(deleteRecordKey, deleteReaderValue);
                  } else {
                    deleteReaderValue.close();
                  }
                }
              }
            }
          }
          readAllDeleteEventsFromDeleteDeltas(writeIdLoader);
          LOG.debug("Number of delete events(limit, actual)=({},{})",
              totalDeleteEventCount, size());
        }
        isEmpty = writeIds == null || writeIds.isEmpty() || rowIds == null;
      } catch(IOException|DeleteEventsOverflowMemoryException e) {
        close(); // close any open readers, if there was some exception during initialization.
        throw e; // rethrow the exception so that the caller can handle.
      }
    }

    private static OrcRawRecordMerger.KeyInterval findDeleteMinMaxKeys(OrcTail orcTail, Path path) {
      boolean columnStatsPresent = orcTail.getFooter().getRowIndexStride() > 0;
      if (!columnStatsPresent) {
        LOG.debug("findMinMaxKeys() No ORC column stats");
        return new OrcRawRecordMerger.KeyInterval(null, null);
      }

      return getKeyInterval(orcTail.getFooter().getStatisticsList());
    }

    /**
     * Check if the delete delta folder needs to be scanned for a given split's min/max write ids.
     *
     * @param orcSplitMinMaxWriteIds
     * @param deleteDelta
     * @param stmtId statementId of the deleteDelta if present
     * @return true when  delete delta dir has to be scanned.
     */
    @VisibleForTesting
    protected static boolean isQualifiedDeleteDeltaForSplit(AcidOutputFormat.Options orcSplitMinMaxWriteIds,
        AcidInputFormat.DeltaMetaData deleteDelta, Integer stmtId) {
      // We allow equal writeIds so we are prepared for multi statement transactions.
      // In this case we have to check the stmt id.
      if (orcSplitMinMaxWriteIds.getMinimumWriteId() == deleteDelta.getMaxWriteId()) {
        int orcSplitStmtId = orcSplitMinMaxWriteIds.getStatementId();
        // StatementId -1 and 0 is also used as the default one if it is not provided.
        // Not brave enough to fix generally, so just fix here.
        if (orcSplitStmtId == -1) {
          orcSplitStmtId = 0;
        }
        if (stmtId == null || stmtId == -1) {
          stmtId = 0;
        }
        return orcSplitStmtId < stmtId;
      }
      // For delta_0000012_0000014_0000, no need to read delete delta folders < 12.
      return orcSplitMinMaxWriteIds.getMinimumWriteId() < deleteDelta.getMaxWriteId();
    }

    private void checkSize(int index) throws DeleteEventsOverflowMemoryException {
      if(index > maxEventsInMemory) {
        //check to prevent OOM errors
        LOG.info("Total number of delete events exceeds the maximum number of "
            + "delete events that can be loaded into memory for " + orcSplit
            + ". The max limit is currently set at " + maxEventsInMemory
            + " and can be changed by setting the Hive config variable "
            + ConfVars.HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY.varname);
        throw new DeleteEventsOverflowMemoryException();
      }
      if(index < rowIds.length) {
        return;
      }
      int newLength = rowIds.length + 1000000;
      if(rowIds.length <= 1000000) {
        //double small arrays; increase by 1M large arrays
        newLength = rowIds.length * 2;
      }
      rowIds = Arrays.copyOf(rowIds, newLength);
    }
    /**
     * This is not done quite right.  The intent of {@link CompressedOwid} is a hedge against
     * "delete from T" that generates a huge number of delete events possibly even 2G - max array
     * size.  (assuming no one txn inserts > 2G rows (in a bucket)).  As implemented, the algorithm
     * first loads all data into one array owid[] and rowIds[] which defeats the purpose.
     * In practice we should be filtering delete evens by min/max ROW_ID from the split.  The later
     * is also not yet implemented: HIVE-16812.
     */
    private void readAllDeleteEventsFromDeleteDeltas(OriginalWriteIdLoader writeIdLoader)
        throws IOException, DeleteEventsOverflowMemoryException {
      if (sortMerger == null || sortMerger.isEmpty()) {
        return; // trivial case, nothing to read.
      }

      // Initialize the rowId array when we have some delete events.
      rowIds = new long[testMode ? 1 : 10000];

      int index = 0;
      // We compress the owids into CompressedOwid data structure that records
      // the fromIndex(inclusive) and toIndex(exclusive) for each unique owid.
      while (!sortMerger.isEmpty()) {
        // The sortMerger is a heap data structure that stores a pair of
        // (deleteRecordKey, deleteReaderValue) at each node and is ordered by deleteRecordKey.
        // The deleteReaderValue is the actual wrapper class that has the reference to the
        // underlying delta file that is being read, and its corresponding deleteRecordKey
        // is the smallest record id for that file. In each iteration of this loop, we extract(poll)
        // the minimum deleteRecordKey pair. Once we have processed that deleteRecordKey, we
        // advance the pointer for the corresponding deleteReaderValue. If the underlying file
        // itself has no more records, then we remove that pair from the heap, or else we
        // add the updated pair back to the heap.
        Entry<DeleteRecordKey, DeleteReaderValue> entry = sortMerger.pollFirstEntry();
        DeleteRecordKey deleteRecordKey = entry.getKey();
        DeleteReaderValue deleteReaderValue = entry.getValue();
        checkSize(index);
        rowIds[index] = deleteRecordKey.rowId;
        writeIdLoader.add(index, deleteRecordKey);
        ++index;
        if (deleteReaderValue.next(deleteRecordKey)) {
          sortMerger.put(deleteRecordKey, deleteReaderValue);
        } else {
          deleteReaderValue.close(); // Exhausted reading all records, close the reader.
        }
      }
      writeIds = writeIdLoader.done(index);
      if (rowIds.length > index) {
        rowIds = Arrays.copyOf(rowIds, index);
      }
    }

    /**
     * @return how many delete events are actually loaded
     */
    int size() {
      return rowIds == null ? 0 : rowIds.length;
    }
    @Override
    public boolean isEmpty() {
      if(isEmpty == null) {
        throw new IllegalStateException("Not yet initialized");
      }
      return isEmpty;
    }
    @Override
    public void findDeletedRecords(ColumnVector[] cols, int size, BitSet selectedBitSet) {
      if (rowIds == null || writeIds == null || writeIds.isEmpty()) {
        return;
      }
      // Iterate through the batch and for each (owid, rowid) in the batch
      // check if it is deleted or not.

      long[] originalWriteIdVector =
          cols[OrcRecordUpdater.ORIGINAL_WRITEID].isRepeating ? null
              : ((LongColumnVector) cols[OrcRecordUpdater.ORIGINAL_WRITEID]).vector;
      long repeatedOriginalWriteId = (originalWriteIdVector != null) ? -1
          : ((LongColumnVector) cols[OrcRecordUpdater.ORIGINAL_WRITEID]).vector[0];

      long[] bucketProperties =
        cols[OrcRecordUpdater.BUCKET].isRepeating ? null
          : ((LongColumnVector) cols[OrcRecordUpdater.BUCKET]).vector;
      int repeatedBucketProperty = (bucketProperties != null) ? -1
        : (int)((LongColumnVector) cols[OrcRecordUpdater.BUCKET]).vector[0];

      long[] rowIdVector =
          ((LongColumnVector) cols[OrcRecordUpdater.ROW_ID]).vector;

      LongColumnVector currentWriteIdVector = (LongColumnVector) cols[OrcRecordUpdater.CURRENT_WRITEID];
      for (int setBitIndex = selectedBitSet.nextSetBit(0);
          setBitIndex >= 0;
          setBitIndex = selectedBitSet.nextSetBit(setBitIndex+1)) {
        long owid = originalWriteIdVector != null ? originalWriteIdVector[setBitIndex]
                                                    : repeatedOriginalWriteId ;
        int bucketProperty = bucketProperties != null ? (int)bucketProperties[setBitIndex]
          : repeatedBucketProperty;
        long rowId = rowIdVector[setBitIndex];
        writeIds.markAsDeleted(owid, bucketProperty, rowId, rowIds, setBitIndex, selectedBitSet, currentWriteIdVector);
      }
    }

    @Override
    public void close() throws IOException {
      // ColumnizedDeleteEventRegistry reads all the delete events into memory during initialization
      // and it closes the delete event readers after it. If an exception gets thrown during
      // initialization, we may have to close any readers that are still left open.
      while (!sortMerger.isEmpty()) {
        Entry<DeleteRecordKey, DeleteReaderValue> entry = sortMerger.pollFirstEntry();
        entry.getValue().close(); // close the reader for this entry
      }
    }
  }

  static class DeleteEventsOverflowMemoryException extends Exception {
    private static final long serialVersionUID = 1L;
  }
  @VisibleForTesting
  OrcRawRecordMerger.KeyInterval getKeyInterval() {
    return keyInterval;
  }
  @VisibleForTesting
  SearchArgument getDeleteEventSarg() {
     return deleteEventSarg;
  }

  private static IntegerColumnStatistics deserializeIntColumnStatistics(List<OrcProto.ColumnStatistics> colStats, int id) {
    return (IntegerColumnStatistics) ColumnStatisticsImpl.deserialize(null, colStats.get(id));
  }

  /**
   * Calculates the min/max record key.
   * Structure in data is like this:
   * <op, owid, writerId, rowid, cwid, <f1, ... fn>>
   * The +1 is to account for the top level struct which has a
   * ColumnStatistics object in colsStats.  Top level struct is normally
   * dropped by the Reader (I guess because of orc.impl.SchemaEvolution)
   * @param colStats The statistics array
   * @return The min record key
   */
  private static OrcRawRecordMerger.KeyInterval getKeyInterval(ColumnStatistics[] colStats) {
    IntegerColumnStatistics origWriteId = (IntegerColumnStatistics) colStats[OrcRecordUpdater.ORIGINAL_WRITEID + 1];
    IntegerColumnStatistics bucketProperty = (IntegerColumnStatistics) colStats[OrcRecordUpdater.BUCKET + 1];
    IntegerColumnStatistics rowId = (IntegerColumnStatistics) colStats[OrcRecordUpdater.ROW_ID + 1];

    // We may want to change bucketProperty from int to long in the future(across the stack) this protects
    // the following cast to int
    assert bucketProperty.getMaximum() <= Integer.MAX_VALUE :
        "was bucketProperty (max) changed to a long (" + bucketProperty.getMaximum() + ")?!";
    assert bucketProperty.getMinimum() <= Integer.MAX_VALUE :
        "was bucketProperty (min) changed to a long (" + bucketProperty.getMaximum() + ")?!";
    RecordIdentifier maxKey = new RecordIdentifier(origWriteId.getMaximum(), (int) bucketProperty.getMaximum(), rowId.getMaximum());
    RecordIdentifier minKey = new RecordIdentifier(origWriteId.getMinimum(), (int) bucketProperty.getMinimum(), rowId.getMinimum());
    return new OrcRawRecordMerger.KeyInterval(minKey, maxKey);
  }

  private static OrcRawRecordMerger.KeyInterval getKeyInterval(List<OrcProto.ColumnStatistics> colStats) {
    ColumnStatistics[] columnStatsArray = new ColumnStatistics[colStats.size()];
    columnStatsArray[OrcRecordUpdater.ORIGINAL_WRITEID + 1] =
        deserializeIntColumnStatistics(colStats, OrcRecordUpdater.ORIGINAL_WRITEID + 1);
    columnStatsArray[OrcRecordUpdater.BUCKET + 1]  =
        deserializeIntColumnStatistics(colStats, OrcRecordUpdater.BUCKET + 1);
    columnStatsArray[OrcRecordUpdater.ROW_ID + 1]  =
        deserializeIntColumnStatistics(colStats, OrcRecordUpdater.ROW_ID + 1);
    return getKeyInterval(columnStatsArray);
  }

  private static class RowIsDeletedColumnVector extends LongColumnVector {

    public RowIsDeletedColumnVector(int defaultSize) {
      super(defaultSize);
    }

    void fill(boolean deleted) {
      fill(deleted ? 1 : 0);
    }

    void set(BitSet notDeletedBitSet) {
      if (notDeletedBitSet.cardinality() == 0) {
        fill(true);
      } else {
        Arrays.fill(vector, 1);
        isRepeating = false;
        for (int setBitIndex = notDeletedBitSet.nextSetBit(0);
             setBitIndex >= 0;
             setBitIndex = notDeletedBitSet.nextSetBit(setBitIndex + 1)) {
          vector[setBitIndex] = 0;
        }
      }
    }
  }
}
