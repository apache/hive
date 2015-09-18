/**
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidInputFormat.DeltaMetaData;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils.Directory;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.hive.ql.io.LlapWrappableInputFormatInterface;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordReader;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.WriterVersion;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat.Context;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileStatusWithId;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
/**
 * A MapReduce/Hive input format for ORC files.
 * <p>
 * This class implements both the classic InputFormat, which stores the rows
 * directly, and AcidInputFormat, which stores a series of events with the
 * following schema:
 * <pre>
 *   class AcidEvent&lt;ROW&gt; {
 *     enum ACTION {INSERT, UPDATE, DELETE}
 *     ACTION operation;
 *     long originalTransaction;
 *     int bucket;
 *     long rowId;
 *     long currentTransaction;
 *     ROW row;
 *   }
 * </pre>
 * Each AcidEvent object corresponds to an update event. The
 * originalTransaction, bucket, and rowId are the unique identifier for the row.
 * The operation and currentTransaction are the operation and the transaction
 * that added this event. Insert and update events include the entire row, while
 * delete events have null for row.
 */
public class OrcInputFormat  implements InputFormat<NullWritable, OrcStruct>,
  InputFormatChecker, VectorizedInputFormatInterface, LlapWrappableInputFormatInterface,
    AcidInputFormat<NullWritable, OrcStruct>, CombineHiveInputFormat.AvoidSplitCombination {

  static enum SplitStrategyKind{
    HYBRID,
    BI,
    ETL
  }

  private static final Log LOG = LogFactory.getLog(OrcInputFormat.class);
  private static boolean isDebugEnabled = LOG.isDebugEnabled();
  static final HadoopShims SHIMS = ShimLoader.getHadoopShims();
  static final String MIN_SPLIT_SIZE =
      SHIMS.getHadoopConfNames().get("MAPREDMINSPLITSIZE");
  static final String MAX_SPLIT_SIZE =
      SHIMS.getHadoopConfNames().get("MAPREDMAXSPLITSIZE");

  private static final long DEFAULT_MIN_SPLIT_SIZE = 16 * 1024 * 1024;
  private static final long DEFAULT_MAX_SPLIT_SIZE = 256 * 1024 * 1024;

  /**
   * When picking the hosts for a split that crosses block boundaries,
   * drop any host that has fewer than MIN_INCLUDED_LOCATION of the
   * number of bytes available on the host with the most.
   * If host1 has 10MB of the split, host2 has 20MB, and host3 has 18MB the
   * split will contain host2 (100% of host2) and host3 (90% of host2). Host1
   * with 50% will be dropped.
   */
  private static final double MIN_INCLUDED_LOCATION = 0.80;

  @Override
  public boolean shouldSkipCombine(Path path,
                                   Configuration conf) throws IOException {
    return (conf.get(AcidUtils.CONF_ACID_KEY) != null) || AcidUtils.isAcid(path, conf);
  }

  private static class OrcRecordReader
      implements org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct>,
      StatsProvidingRecordReader {
    private final RecordReader reader;
    private final long offset;
    private final long length;
    private final int numColumns;
    private float progress = 0.0f;
    private final Reader file;
    private final SerDeStats stats;


    OrcRecordReader(Reader file, Configuration conf,
                    FileSplit split) throws IOException {
      List<OrcProto.Type> types = file.getTypes();
      this.file = file;
      numColumns = (types.size() == 0) ? 0 : types.get(0).getSubtypesCount();
      this.offset = split.getStart();
      this.length = split.getLength();
      this.reader = createReaderFromFile(file, conf, offset, length);
      this.stats = new SerDeStats();
    }

    @Override
    public boolean next(NullWritable key, OrcStruct value) throws IOException {
      if (reader.hasNext()) {
        reader.next(value);
        progress = reader.getProgress();
        return true;
      } else {
        return false;
      }
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public OrcStruct createValue() {
      return new OrcStruct(numColumns);
    }

    @Override
    public long getPos() throws IOException {
      return offset + (long) (progress * length);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public float getProgress() throws IOException {
      return progress;
    }

    @Override
    public SerDeStats getStats() {
      stats.setRawDataSize(file.getRawDataSize());
      stats.setRowCount(file.getNumberOfRows());
      return stats;
    }
  }

  /**
   * Get the root column for the row. In ACID format files, it is offset by
   * the extra metadata columns.
   * @param isOriginal is the file in the original format?
   * @return the column number for the root of row.
   */
  private static int getRootColumn(boolean isOriginal) {
    return isOriginal ? 0 : (OrcRecordUpdater.ROW + 1);
  }

  public static RecordReader createReaderFromFile(Reader file,
                                                  Configuration conf,
                                                  long offset, long length
                                                  ) throws IOException {
    Reader.Options options = new Reader.Options().range(offset, length);
    boolean isOriginal = isOriginal(file);
    List<OrcProto.Type> types = file.getTypes();
    options.include(genIncludedColumns(types, conf, isOriginal));
    setSearchArgument(options, types, conf, isOriginal);
    return file.rowsOptions(options);
  }

  public static boolean isOriginal(Reader file) {
    return !file.hasMetadataValue(OrcRecordUpdater.ACID_KEY_INDEX_NAME);
  }

  /**
   * Recurse down into a type subtree turning on all of the sub-columns.
   * @param types the types of the file
   * @param result the global view of columns that should be included
   * @param typeId the root of tree to enable
   * @param rootColumn the top column
   */
  private static void includeColumnRecursive(List<OrcProto.Type> types,
                                             boolean[] result,
                                             int typeId,
                                             int rootColumn) {
    result[typeId - rootColumn] = true;
    OrcProto.Type type = types.get(typeId);
    int children = type.getSubtypesCount();
    for(int i=0; i < children; ++i) {
      includeColumnRecursive(types, result, type.getSubtypes(i), rootColumn);
    }
  }

  public static boolean[] genIncludedColumns(
      List<OrcProto.Type> types, List<Integer> included, boolean isOriginal) {
    int rootColumn = getRootColumn(isOriginal);
    int numColumns = types.size() - rootColumn;
    boolean[] result = new boolean[numColumns];
    result[0] = true;
    OrcProto.Type root = types.get(rootColumn);
    for (int i = 0; i < root.getSubtypesCount(); ++i) {
      if (included.contains(i)) {
        includeColumnRecursive(types, result, root.getSubtypes(i), rootColumn);
      }
    }
    return result;
  }

  /**
   * Take the configuration and figure out which columns we need to include.
   * @param types the types for the file
   * @param conf the configuration
   * @param isOriginal is the file in the original format?
   */
  public static boolean[] genIncludedColumns(
      List<OrcProto.Type> types, Configuration conf, boolean isOriginal) {
     if (!ColumnProjectionUtils.isReadAllColumns(conf)) {
      List<Integer> included = ColumnProjectionUtils.getReadColumnIDs(conf);
      return genIncludedColumns(types, included, isOriginal);
    } else {
      return null;
    }
  }

  public static String[] getSargColumnNames(String[] originalColumnNames,
      List<OrcProto.Type> types, boolean[] includedColumns, boolean isOriginal) {
    int rootColumn = getRootColumn(isOriginal);
    String[] columnNames = new String[types.size() - rootColumn];
    int i = 0;
    // The way this works is as such. originalColumnNames is the equivalent on getNeededColumns
    // from TSOP. They are assumed to be in the same order as the columns in ORC file, AND they are
    // assumed to be equivalent to the columns in includedColumns (because it was generated from
    // the same column list at some point in the past), minus the subtype columns. Therefore, when
    // we go thru all the top level ORC file columns that are included, in order, they match
    // originalColumnNames. This way, we do not depend on names stored inside ORC for SARG leaf
    // column name resolution (see mapSargColumns method).
    for(int columnId: types.get(rootColumn).getSubtypesList()) {
      if (includedColumns == null || includedColumns[columnId - rootColumn]) {
        // this is guaranteed to be positive because types only have children
        // ids greater than their own id.
        columnNames[columnId - rootColumn] = originalColumnNames[i++];
      }
    }
    return columnNames;
  }

  static void setSearchArgument(Reader.Options options,
                                List<OrcProto.Type> types,
                                Configuration conf,
                                boolean isOriginal) {
    String neededColumnNames = getNeededColumnNamesString(conf);
    if (neededColumnNames == null) {
      LOG.debug("No ORC pushdown predicate - no column names");
      options.searchArgument(null, null);
      return;
    }
    SearchArgument sarg = ConvertAstToSearchArg.createFromConf(conf);
    if (sarg == null) {
      LOG.debug("No ORC pushdown predicate");
      options.searchArgument(null, null);
      return;
    }

    LOG.info("ORC pushdown predicate: " + sarg);
    options.searchArgument(sarg, getSargColumnNames(
        neededColumnNames.split(","), types, options.getInclude(), isOriginal));
  }

  static boolean canCreateSargFromConf(Configuration conf) {
    if (getNeededColumnNamesString(conf) == null) {
      LOG.debug("No ORC pushdown predicate - no column names");
      return false;
    }
    if (!ConvertAstToSearchArg.canCreateFromConf(conf)) {
      LOG.debug("No ORC pushdown predicate");
      return false;
    }
    return true;
  }

  private static String[] extractNeededColNames(
      List<OrcProto.Type> types, Configuration conf, boolean[] include, boolean isOriginal) {
    return extractNeededColNames(types, getNeededColumnNamesString(conf), include, isOriginal);
  }

  private static String[] extractNeededColNames(
      List<OrcProto.Type> types, String columnNamesString, boolean[] include, boolean isOriginal) {
    return getSargColumnNames(columnNamesString.split(","), types, include, isOriginal);
  }

  private static String getNeededColumnNamesString(Configuration conf) {
    return conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
  }

  private static String getSargColumnIDsString(Configuration conf) {
    return conf.getBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, true) ? null
        : conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR);
  }
  @Override
  public boolean validateInput(FileSystem fs, HiveConf conf,
                               ArrayList<FileStatus> files
                              ) throws IOException {

    if (Utilities.isVectorMode(conf)) {
      return new VectorizedOrcInputFormat().validateInput(fs, conf, files);
    }

    if (files.size() <= 0) {
      return false;
    }
    for (FileStatus file : files) {
      try {
        OrcFile.createReader(file.getPath(),
            OrcFile.readerOptions(conf).filesystem(fs));
      } catch (IOException e) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get the list of input {@link Path}s for the map-reduce job.
   *
   * @param conf The configuration of the job
   * @return the list of input {@link Path}s for the map-reduce job.
   */
  static Path[] getInputPaths(Configuration conf) throws IOException {
    String dirs = conf.get("mapred.input.dir");
    if (dirs == null) {
      throw new IOException("Configuration mapred.input.dir is not defined.");
    }
    String [] list = StringUtils.split(dirs);
    Path[] result = new Path[list.length];
    for (int i = 0; i < list.length; i++) {
      result[i] = new Path(StringUtils.unEscapeString(list[i]));
    }
    return result;
  }

  /**
   * The global information about the split generation that we pass around to
   * the different worker threads.
   */
  static class Context {
    private final Configuration conf;
    private static Cache<Path, FileInfo> footerCache;
    private static ExecutorService threadPool = null;
    private final int numBuckets;
    private final long maxSize;
    private final long minSize;
    private final int minSplits;
    private final boolean footerInSplits;
    private final boolean cacheStripeDetails;
    private final AtomicInteger cacheHitCounter = new AtomicInteger(0);
    private final AtomicInteger numFilesCounter = new AtomicInteger(0);
    private ValidTxnList transactionList;
    private SplitStrategyKind splitStrategyKind;

    Context(Configuration conf) {
      this(conf, 1);
    }

    Context(Configuration conf, final int minSplits) {
      this.conf = conf;
      minSize = conf.getLong(MIN_SPLIT_SIZE, DEFAULT_MIN_SPLIT_SIZE);
      maxSize = conf.getLong(MAX_SPLIT_SIZE, DEFAULT_MAX_SPLIT_SIZE);
      String ss = conf.get(ConfVars.HIVE_ORC_SPLIT_STRATEGY.varname);
      if (ss == null || ss.equals(SplitStrategyKind.HYBRID.name())) {
        splitStrategyKind = SplitStrategyKind.HYBRID;
      } else {
        LOG.info("Enforcing " + ss + " ORC split strategy");
        splitStrategyKind = SplitStrategyKind.valueOf(ss);
      }
      footerInSplits = HiveConf.getBoolVar(conf,
          ConfVars.HIVE_ORC_INCLUDE_FILE_FOOTER_IN_SPLITS);
      numBuckets =
          Math.max(conf.getInt(hive_metastoreConstants.BUCKET_COUNT, 0), 0);
      LOG.debug("Number of buckets specified by conf file is " + numBuckets);
      int cacheStripeDetailsSize = HiveConf.getIntVar(conf,
          ConfVars.HIVE_ORC_CACHE_STRIPE_DETAILS_SIZE);
      int numThreads = HiveConf.getIntVar(conf,
          ConfVars.HIVE_ORC_COMPUTE_SPLITS_NUM_THREADS);

      cacheStripeDetails = (cacheStripeDetailsSize > 0);

      this.minSplits = Math.min(cacheStripeDetailsSize, minSplits);

      synchronized (Context.class) {
        if (threadPool == null) {
          threadPool = Executors.newFixedThreadPool(numThreads,
              new ThreadFactoryBuilder().setDaemon(true)
                  .setNameFormat("ORC_GET_SPLITS #%d").build());
        }

        if (footerCache == null && cacheStripeDetails) {
          footerCache = CacheBuilder.newBuilder()
              .concurrencyLevel(numThreads)
              .initialCapacity(cacheStripeDetailsSize)
              .maximumSize(cacheStripeDetailsSize)
              .softValues()
              .build();
        }
      }
      String value = conf.get(ValidTxnList.VALID_TXNS_KEY,
                              Long.MAX_VALUE + ":");
      transactionList = new ValidReadTxnList(value);
    }
  }

  /**
   * The full ACID directory information needed for splits; no more calls to HDFS needed.
   * We could just live with AcidUtils.Directory but...
   * 1) That doesn't have base files for the base-directory case.
   * 2) We save fs for convenience to avoid getting it twice.
   */
  @VisibleForTesting
  static final class AcidDirInfo {
    public AcidDirInfo(FileSystem fs, Path splitPath, Directory acidInfo,
        List<HdfsFileStatusWithId> baseOrOriginalFiles) {
      this.splitPath = splitPath;
      this.acidInfo = acidInfo;
      this.baseOrOriginalFiles = baseOrOriginalFiles;
      this.fs = fs;
    }

    final FileSystem fs;
    final Path splitPath;
    final AcidUtils.Directory acidInfo;
    final List<HdfsFileStatusWithId> baseOrOriginalFiles;
  }

  @VisibleForTesting
  interface SplitStrategy<T> {
    List<T> getSplits() throws IOException;
  }

  @VisibleForTesting
  static final class SplitInfo extends ACIDSplitStrategy {
    private final Context context;
    private final FileSystem fs;
    private final HdfsFileStatusWithId fileWithId;
    private final FileInfo fileInfo;
    private final boolean isOriginal;
    private final List<DeltaMetaData> deltas;
    private final boolean hasBase;

    SplitInfo(Context context, FileSystem fs,
        HdfsFileStatusWithId fileWithId, FileInfo fileInfo,
        boolean isOriginal,
        List<DeltaMetaData> deltas,
        boolean hasBase, Path dir, boolean[] covered) throws IOException {
      super(dir, context.numBuckets, deltas, covered);
      this.context = context;
      this.fs = fs;
      this.fileWithId = fileWithId;
      this.fileInfo = fileInfo;
      this.isOriginal = isOriginal;
      this.deltas = deltas;
      this.hasBase = hasBase;
    }

    @VisibleForTesting
    public SplitInfo(Context context, FileSystem fs, FileStatus fileStatus, FileInfo fileInfo,
        boolean isOriginal, ArrayList<DeltaMetaData> deltas, boolean hasBase, Path dir,
        boolean[] covered) throws IOException {
      this(context, fs, AcidUtils.createOriginalObj(null, fileStatus),
          fileInfo, isOriginal, deltas, hasBase, dir, covered);
    }
  }

  /**
   * ETL strategy is used when spending little more time in split generation is acceptable
   * (split generation reads and caches file footers).
   */
  static final class ETLSplitStrategy implements SplitStrategy<SplitInfo> {
    Context context;
    FileSystem fs;
    List<HdfsFileStatusWithId> files;
    boolean isOriginal;
    List<DeltaMetaData> deltas;
    Path dir;
    boolean[] covered;

    public ETLSplitStrategy(Context context, FileSystem fs, Path dir,
        List<HdfsFileStatusWithId> children, boolean isOriginal, List<DeltaMetaData> deltas,
        boolean[] covered) {
      this.context = context;
      this.dir = dir;
      this.fs = fs;
      this.files = children;
      this.isOriginal = isOriginal;
      this.deltas = deltas;
      this.covered = covered;
    }

    private FileInfo verifyCachedFileInfo(FileStatus file) {
      FileInfo fileInfo = Context.footerCache.getIfPresent(file.getPath());
      if (fileInfo != null) {
        if (isDebugEnabled) {
          LOG.debug("Info cached for path: " + file.getPath());
        }
        if (fileInfo.modificationTime == file.getModificationTime() &&
            fileInfo.size == file.getLen()) {
          // Cached copy is valid
          context.cacheHitCounter.incrementAndGet();
          return fileInfo;
        } else {
          // Invalidate
          Context.footerCache.invalidate(file.getPath());
          if (isDebugEnabled) {
            LOG.debug("Meta-Info for : " + file.getPath() +
                " changed. CachedModificationTime: "
                + fileInfo.modificationTime + ", CurrentModificationTime: "
                + file.getModificationTime()
                + ", CachedLength: " + fileInfo.size + ", CurrentLength: " +
                file.getLen());
          }
        }
      } else {
        if (isDebugEnabled) {
          LOG.debug("Info not cached for path: " + file.getPath());
        }
      }
      return null;
    }

    @Override
    public List<SplitInfo> getSplits() throws IOException {
      List<SplitInfo> result = Lists.newArrayList();
      for (HdfsFileStatusWithId file : files) {
        FileInfo info = null;
        if (context.cacheStripeDetails) {
          info = verifyCachedFileInfo(file.getFileStatus());
        }
        // ignore files of 0 length
        if (file.getFileStatus().getLen() > 0) {
          result.add(new SplitInfo(
              context, fs, file, info, isOriginal, deltas, true, dir, covered));
        }
      }
      return result;
    }

    @Override
    public String toString() {
      return ETLSplitStrategy.class.getSimpleName() + " strategy for " + dir;
    }
  }

  /**
   * BI strategy is used when the requirement is to spend less time in split generation
   * as opposed to query execution (split generation does not read or cache file footers).
   */
  static final class BISplitStrategy extends ACIDSplitStrategy {
    List<HdfsFileStatusWithId> fileStatuses;
    boolean isOriginal;
    List<DeltaMetaData> deltas;
    FileSystem fs;
    Context context;
    Path dir;

    public BISplitStrategy(Context context, FileSystem fs,
        Path dir, List<HdfsFileStatusWithId> fileStatuses, boolean isOriginal,
        List<DeltaMetaData> deltas, boolean[] covered) {
      super(dir, context.numBuckets, deltas, covered);
      this.context = context;
      this.fileStatuses = fileStatuses;
      this.isOriginal = isOriginal;
      this.deltas = deltas;
      this.fs = fs;
      this.dir = dir;
    }

    @Override
    public List<OrcSplit> getSplits() throws IOException {
      List<OrcSplit> splits = Lists.newArrayList();
      for (HdfsFileStatusWithId file : fileStatuses) {
        FileStatus fileStatus = file.getFileStatus();
        String[] hosts = SHIMS.getLocationsWithOffset(fs, fileStatus).firstEntry().getValue()
            .getHosts();
        OrcSplit orcSplit = new OrcSplit(fileStatus.getPath(), file.getFileId(), 0,
            fileStatus.getLen(), hosts, null, isOriginal, true, deltas, -1);
        splits.add(orcSplit);
      }

      // add uncovered ACID delta splits
      splits.addAll(super.getSplits());
      return splits;
    }

    @Override
    public String toString() {
      return BISplitStrategy.class.getSimpleName() + " strategy for " + dir;
    }
  }

  /**
   * ACID split strategy is used when there is no base directory (when transactions are enabled).
   */
  static class ACIDSplitStrategy implements SplitStrategy<OrcSplit> {
    Path dir;
    List<DeltaMetaData> deltas;
    boolean[] covered;
    int numBuckets;

    public ACIDSplitStrategy(Path dir, int numBuckets, List<DeltaMetaData> deltas, boolean[] covered) {
      this.dir = dir;
      this.numBuckets = numBuckets;
      this.deltas = deltas;
      this.covered = covered;
    }

    @Override
    public List<OrcSplit> getSplits() throws IOException {
      // Generate a split for any buckets that weren't covered.
      // This happens in the case where a bucket just has deltas and no
      // base.
      List<OrcSplit> splits = Lists.newArrayList();
      if (!deltas.isEmpty()) {
        for (int b = 0; b < numBuckets; ++b) {
          if (!covered[b]) {
            splits.add(new OrcSplit(dir, null, b, 0, new String[0], null, false, false, deltas, -1));
          }
        }
      }
      return splits;
    }

    @Override
    public String toString() {
      return ACIDSplitStrategy.class.getSimpleName() + " strategy for " + dir;
    }
  }

  /**
   * Given a directory, get the list of files and blocks in those files.
   * To parallelize file generator use "mapreduce.input.fileinputformat.list-status.num-threads"
   */
  static final class FileGenerator implements Callable<AcidDirInfo> {
    private final Context context;
    private final FileSystem fs;
    private final Path dir;
    private final boolean useFileIds;

    FileGenerator(Context context, FileSystem fs, Path dir, boolean useFileIds) {
      this.context = context;
      this.fs = fs;
      this.dir = dir;
      this.useFileIds = useFileIds;
    }

    @Override
    public AcidDirInfo call() throws IOException {
      AcidUtils.Directory dirInfo = AcidUtils.getAcidState(dir,
          context.conf, context.transactionList, useFileIds);
      Path base = dirInfo.getBaseDirectory();
      // find the base files (original or new style)
      List<HdfsFileStatusWithId> children = (base == null)
          ? dirInfo.getOriginalFiles() : findBaseFiles(base, useFileIds);
      return new AcidDirInfo(fs, dir, dirInfo, children);
    }

    private List<HdfsFileStatusWithId> findBaseFiles(
        Path base, boolean useFileIds) throws IOException {
      if (useFileIds) {
        try {
          return SHIMS.listLocatedHdfsStatus(fs, base, AcidUtils.hiddenFileFilter);
        } catch (Throwable t) {
          LOG.error("Failed to get files with ID; using regular API", t);
        }
      }

      // Fall back to regular API and create states without ID.
      List<FileStatus> children = SHIMS.listLocatedStatus(fs, base, AcidUtils.hiddenFileFilter);
      List<HdfsFileStatusWithId> result = new ArrayList<>(children.size());
      for (FileStatus child : children) {
        result.add(AcidUtils.createOriginalObj(null, child));
      }
      return result;
    }
  }

  /**
   * Split the stripes of a given file into input splits.
   * A thread is used for each file.
   */
  static final class SplitGenerator implements Callable<List<OrcSplit>> {
    private final Context context;
    private final FileSystem fs;
    private final HdfsFileStatusWithId fileWithId;
    private final FileStatus file;
    private final long blockSize;
    private final TreeMap<Long, BlockLocation> locations;
    private final FileInfo fileInfo;
    private List<StripeInformation> stripes;
    private FileMetaInfo fileMetaInfo;
    private List<StripeStatistics> stripeStats;
    private List<OrcProto.Type> types;
    private boolean[] includedCols;
    private final boolean isOriginal;
    private final List<DeltaMetaData> deltas;
    private final boolean hasBase;
    private OrcFile.WriterVersion writerVersion;
    private long projColsUncompressedSize;
    private List<OrcSplit> deltaSplits;

    public SplitGenerator(SplitInfo splitInfo) throws IOException {
      this.context = splitInfo.context;
      this.fs = splitInfo.fs;
      this.fileWithId = splitInfo.fileWithId;
      this.file = this.fileWithId.getFileStatus();
      this.blockSize = this.file.getBlockSize();
      this.fileInfo = splitInfo.fileInfo;
      locations = SHIMS.getLocationsWithOffset(fs, fileWithId.getFileStatus());
      this.isOriginal = splitInfo.isOriginal;
      this.deltas = splitInfo.deltas;
      this.hasBase = splitInfo.hasBase;
      this.projColsUncompressedSize = -1;
      this.deltaSplits = splitInfo.getSplits();
    }

    Path getPath() {
      return fileWithId.getFileStatus().getPath();
    }

    @Override
    public String toString() {
      return "splitter(" + fileWithId.getFileStatus().getPath() + ")";
    }

    /**
     * Compute the number of bytes that overlap between the two ranges.
     * @param offset1 start of range1
     * @param length1 length of range1
     * @param offset2 start of range2
     * @param length2 length of range2
     * @return the number of bytes in the overlap range
     */
    static long getOverlap(long offset1, long length1,
                           long offset2, long length2) {
      long end1 = offset1 + length1;
      long end2 = offset2 + length2;
      if (end2 <= offset1 || end1 <= offset2) {
        return 0;
      } else {
        return Math.min(end1, end2) - Math.max(offset1, offset2);
      }
    }

    /**
     * Create an input split over the given range of bytes. The location of the
     * split is based on where the majority of the byte are coming from. ORC
     * files are unlikely to have splits that cross between blocks because they
     * are written with large block sizes.
     * @param offset the start of the split
     * @param length the length of the split
     * @param fileMetaInfo file metadata from footer and postscript
     * @throws IOException
     */
    OrcSplit createSplit(long offset, long length,
                     FileMetaInfo fileMetaInfo) throws IOException {
      String[] hosts;
      Map.Entry<Long, BlockLocation> startEntry = locations.floorEntry(offset);
      BlockLocation start = startEntry.getValue();
      if (offset + length <= start.getOffset() + start.getLength()) {
        // handle the single block case
        hosts = start.getHosts();
      } else {
        Map.Entry<Long, BlockLocation> endEntry = locations.floorEntry(offset + length);
        BlockLocation end = endEntry.getValue();
        //get the submap
        NavigableMap<Long, BlockLocation> navigableMap = locations.subMap(startEntry.getKey(),
                  true, endEntry.getKey(), true);
        // Calculate the number of bytes in the split that are local to each
        // host.
        Map<String, LongWritable> sizes = new HashMap<String, LongWritable>();
        long maxSize = 0;
        for (BlockLocation block : navigableMap.values()) {
          long overlap = getOverlap(offset, length, block.getOffset(),
              block.getLength());
          if (overlap > 0) {
            for(String host: block.getHosts()) {
              LongWritable val = sizes.get(host);
              if (val == null) {
                val = new LongWritable();
                sizes.put(host, val);
              }
              val.set(val.get() + overlap);
              maxSize = Math.max(maxSize, val.get());
            }
          } else {
            throw new IOException("File " + fileWithId.getFileStatus().getPath().toString() +
                    " should have had overlap on block starting at " + block.getOffset());
          }
        }
        // filter the list of locations to those that have at least 80% of the
        // max
        long threshold = (long) (maxSize * MIN_INCLUDED_LOCATION);
        List<String> hostList = new ArrayList<String>();
        // build the locations in a predictable order to simplify testing
        for(BlockLocation block: navigableMap.values()) {
          for(String host: block.getHosts()) {
            if (sizes.containsKey(host)) {
              if (sizes.get(host).get() >= threshold) {
                hostList.add(host);
              }
              sizes.remove(host);
            }
          }
        }
        hosts = new String[hostList.size()];
        hostList.toArray(hosts);
      }

      // scale the raw data size to split level based on ratio of split wrt to file length
      final long fileLen = file.getLen();
      final double splitRatio = (double) length / (double) fileLen;
      final long scaledProjSize = projColsUncompressedSize > 0 ?
          (long) (splitRatio * projColsUncompressedSize) : fileLen;
      return new OrcSplit(file.getPath(), fileWithId.getFileId(), offset, length, hosts,
          fileMetaInfo, isOriginal, hasBase, deltas, scaledProjSize);
    }

    /**
     * Divide the adjacent stripes in the file into input splits based on the
     * block size and the configured minimum and maximum sizes.
     */
    @Override
    public List<OrcSplit> call() throws IOException {
      populateAndCacheStripeDetails();
      List<OrcSplit> splits = Lists.newArrayList();

      // figure out which stripes we need to read
      boolean[] includeStripe = null;

      // we can't eliminate stripes if there are deltas because the
      // deltas may change the rows making them match the predicate.
      if (deltas.isEmpty() && canCreateSargFromConf(context.conf)) {
        SearchArgument sarg = ConvertAstToSearchArg.createFromConf(context.conf);
        String[] sargColNames = extractNeededColNames(types, context.conf, includedCols, isOriginal);
        includeStripe = pickStripes(sarg, sargColNames, writerVersion, isOriginal,
            stripeStats, stripes.size(), file.getPath());
      }

      // if we didn't have predicate pushdown, read everything
      if (includeStripe == null) {
        includeStripe = new boolean[stripes.size()];
        Arrays.fill(includeStripe, true);
      }

      long currentOffset = -1;
      long currentLength = 0;
      int idx = -1;
      for (StripeInformation stripe : stripes) {
        idx++;

        if (!includeStripe[idx]) {
          // create split for the previous unfinished stripe
          if (currentOffset != -1) {
            splits.add(createSplit(currentOffset, currentLength, fileMetaInfo));
            currentOffset = -1;
          }
          continue;
        }

        // if we are working on a stripe, over the min stripe size, and
        // crossed a block boundary, cut the input split here.
        if (currentOffset != -1 && currentLength > context.minSize &&
            (currentOffset / blockSize != stripe.getOffset() / blockSize)) {
          splits.add(createSplit(currentOffset, currentLength, fileMetaInfo));
          currentOffset = -1;
        }
        // if we aren't building a split, start a new one.
        if (currentOffset == -1) {
          currentOffset = stripe.getOffset();
          currentLength = stripe.getLength();
        } else {
          currentLength =
              (stripe.getOffset() + stripe.getLength()) - currentOffset;
        }
        if (currentLength >= context.maxSize) {
          splits.add(createSplit(currentOffset, currentLength, fileMetaInfo));
          currentOffset = -1;
        }
      }
      if (currentOffset != -1) {
        splits.add(createSplit(currentOffset, currentLength, fileMetaInfo));
      }

      // add uncovered ACID delta splits
      splits.addAll(deltaSplits);
      return splits;
    }

    private void populateAndCacheStripeDetails() throws IOException {
      Reader orcReader = OrcFile.createReader(fileWithId.getFileStatus().getPath(),
          OrcFile.readerOptions(context.conf).filesystem(fs));
      if (fileInfo != null) {
        stripes = fileInfo.stripeInfos;
        stripeStats = fileInfo.stripeStats;
        fileMetaInfo = fileInfo.fileMetaInfo;
        types = fileInfo.types;
        writerVersion = fileInfo.writerVersion;
        // For multiple runs, in case sendSplitsInFooter changes
        if (fileMetaInfo == null && context.footerInSplits) {
          fileInfo.fileMetaInfo = ((ReaderImpl) orcReader).getFileMetaInfo();
          fileInfo.types = orcReader.getTypes();
          fileInfo.writerVersion = orcReader.getWriterVersion();
        }
      } else {
        stripes = orcReader.getStripes();
        types = orcReader.getTypes();
        writerVersion = orcReader.getWriterVersion();
        stripeStats = orcReader.getStripeStatistics();
        fileMetaInfo = context.footerInSplits ?
            ((ReaderImpl) orcReader).getFileMetaInfo() : null;
        if (context.cacheStripeDetails) {
          // Populate into cache.
          Context.footerCache.put(fileWithId.getFileStatus().getPath(),
              new FileInfo(fileWithId.getFileStatus().getModificationTime(),
                  fileWithId.getFileStatus().getLen(), stripes,
                  stripeStats, types, fileMetaInfo, writerVersion));
        }
      }
      includedCols = genIncludedColumns(types, context.conf, isOriginal);
      projColsUncompressedSize = computeProjectionSize(orcReader, includedCols, isOriginal);
    }

    private long computeProjectionSize(final Reader orcReader, final boolean[] includedCols,
        final boolean isOriginal) {
      final int rootIdx = getRootColumn(isOriginal);
      List<Integer> internalColIds = Lists.newArrayList();
      if (includedCols != null) {
        for (int i = 0; i < includedCols.length; i++) {
          if (includedCols[i]) {
            internalColIds.add(rootIdx + i);
          }
        }
      }
      return orcReader.getRawDataSizeFromColIndices(internalColIds);
    }
  }

  static List<OrcSplit> generateSplitsInfo(Configuration conf)
      throws IOException {
    return generateSplitsInfo(conf, -1);
  }

  static List<OrcSplit> generateSplitsInfo(Configuration conf, int numSplits)
      throws IOException {
    // use threads to resolve directories into splits
    Context context = new Context(conf, numSplits);
    boolean useFileIds = HiveConf.getBoolVar(conf, ConfVars.HIVE_ORC_INCLUDE_FILE_ID_IN_SPLITS);
    List<OrcSplit> splits = Lists.newArrayList();
    List<Future<AcidDirInfo>> pathFutures = Lists.newArrayList();
    List<Future<List<OrcSplit>>> splitFutures = Lists.newArrayList();

    // multi-threaded file statuses and split strategy
    Path[] paths = getInputPaths(conf);
    CompletionService<AcidDirInfo> ecs = new ExecutorCompletionService<>(Context.threadPool);
    for (Path dir : paths) {
      FileSystem fs = dir.getFileSystem(conf);
      FileGenerator fileGenerator = new FileGenerator(context, fs, dir, useFileIds);
      pathFutures.add(ecs.submit(fileGenerator));
    }

    // complete path futures and schedule split generation
    try {
      for (int notIndex = 0; notIndex < paths.length; ++notIndex) {
        AcidDirInfo adi = ecs.take().get();
        SplitStrategy<?> splitStrategy = determineSplitStrategy(
            context, adi.fs, adi.splitPath, adi.acidInfo, adi.baseOrOriginalFiles);

        if (isDebugEnabled) {
          LOG.debug(splitStrategy);
        }

        if (splitStrategy instanceof ETLSplitStrategy) {
          List<SplitInfo> splitInfos = ((ETLSplitStrategy)splitStrategy).getSplits();
          for (SplitInfo splitInfo : splitInfos) {
            splitFutures.add(Context.threadPool.submit(new SplitGenerator(splitInfo)));
          }
        } else {
          @SuppressWarnings("unchecked")
          List<OrcSplit> readySplits = (List<OrcSplit>)splitStrategy.getSplits();
          splits.addAll(readySplits);
        }
      }

      // complete split futures
      for (Future<List<OrcSplit>> splitFuture : splitFutures) {
        splits.addAll(splitFuture.get());
      }
    } catch (Exception e) {
      cancelFutures(pathFutures);
      cancelFutures(splitFutures);
      throw new RuntimeException("serious problem", e);
    }

    if (context.cacheStripeDetails) {
      LOG.info("FooterCacheHitRatio: " + context.cacheHitCounter.get() + "/"
          + context.numFilesCounter.get());
    }

    if (isDebugEnabled) {
      for (OrcSplit split : splits) {
        LOG.debug(split + " projected_columns_uncompressed_size: "
            + split.getColumnarProjectionSize());
      }
    }
    return splits;
  }

  private static <T> void cancelFutures(List<Future<T>> futures) {
    for (Future<T> future : futures) {
      future.cancel(true);
    }
  }

  @Override
  public InputSplit[] getSplits(JobConf job,
                                int numSplits) throws IOException {
    if (isDebugEnabled) {
      LOG.debug("getSplits started");
    }
    List<OrcSplit> result = generateSplitsInfo(job, numSplits);
    if (isDebugEnabled) {
      LOG.debug("getSplits finished");
    }
    return result.toArray(new InputSplit[result.size()]);
  }

  /**
   * FileInfo.
   *
   * Stores information relevant to split generation for an ORC File.
   *
   */
  private static class FileInfo {
    long modificationTime;
    long size;
    List<StripeInformation> stripeInfos;
    FileMetaInfo fileMetaInfo;
    List<StripeStatistics> stripeStats;
    List<OrcProto.Type> types;
    private OrcFile.WriterVersion writerVersion;


    FileInfo(long modificationTime, long size,
             List<StripeInformation> stripeInfos,
             List<StripeStatistics> stripeStats, List<OrcProto.Type> types,
             FileMetaInfo fileMetaInfo,
             OrcFile.WriterVersion writerVersion) {
      this.modificationTime = modificationTime;
      this.size = size;
      this.stripeInfos = stripeInfos;
      this.fileMetaInfo = fileMetaInfo;
      this.stripeStats = stripeStats;
      this.types = types;
      this.writerVersion = writerVersion;
    }
  }

  @SuppressWarnings("unchecked")
  private org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct>
    createVectorizedReader(InputSplit split, JobConf conf, Reporter reporter
                           ) throws IOException {
    return (org.apache.hadoop.mapred.RecordReader)
      new VectorizedOrcInputFormat().getRecordReader(split, conf, reporter);
  }

  @Override
  public org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct>
  getRecordReader(InputSplit inputSplit, JobConf conf,
                  Reporter reporter) throws IOException {
    boolean vectorMode = Utilities.isVectorMode(conf);

    // if HiveCombineInputFormat gives us FileSplits instead of OrcSplits,
    // we know it is not ACID. (see a check in CombineHiveInputFormat.getSplits() that assures this)
    if (inputSplit.getClass() == FileSplit.class) {
      if (vectorMode) {
        return createVectorizedReader(inputSplit, conf, reporter);
      }
      return new OrcRecordReader(OrcFile.createReader(
          ((FileSplit) inputSplit).getPath(),
          OrcFile.readerOptions(conf)), conf, (FileSplit) inputSplit);
    }

    OrcSplit split = (OrcSplit) inputSplit;
    reporter.setStatus(inputSplit.toString());

    Options options = new Options(conf).reporter(reporter);
    final RowReader<OrcStruct> inner = getReader(inputSplit, options);


    /*Even though there are no delta files, we still need to produce row ids so that an
    * UPDATE or DELETE statement would work on a table which didn't have any previous updates*/
    if (split.isOriginal() && split.getDeltas().isEmpty()) {
      if (vectorMode) {
        return createVectorizedReader(inputSplit, conf, reporter);
      } else {
        return new NullKeyRecordReader(inner, conf);
      }
    }

    if (vectorMode) {
      return (org.apache.hadoop.mapred.RecordReader)
          new VectorizedOrcAcidRowReader(inner, conf, (FileSplit) inputSplit);
    }
    return new NullKeyRecordReader(inner, conf);
  }
  /**
   * Return a RecordReader that is compatible with the Hive 0.12 reader
   * with NullWritable for the key instead of RecordIdentifier.
   */
  public static final class NullKeyRecordReader implements AcidRecordReader<NullWritable, OrcStruct> {
    private final RecordIdentifier id;
    private final RowReader<OrcStruct> inner;

    public RecordIdentifier getRecordIdentifier() {
      return id;
    }
    private NullKeyRecordReader(RowReader<OrcStruct> inner, Configuration conf) {
      this.inner = inner;
      id = inner.createKey();
    }
    @Override
    public boolean next(NullWritable nullWritable,
                        OrcStruct orcStruct) throws IOException {
      return inner.next(id, orcStruct);
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public OrcStruct createValue() {
      return inner.createValue();
    }

    @Override
    public long getPos() throws IOException {
      return inner.getPos();
    }

    @Override
    public void close() throws IOException {
      inner.close();
    }

    @Override
    public float getProgress() throws IOException {
      return inner.getProgress();
    }
  }


  @Override
  public RowReader<OrcStruct> getReader(InputSplit inputSplit,
                                        Options options) throws IOException {
    final OrcSplit split = (OrcSplit) inputSplit;
    final Path path = split.getPath();
    Path root;
    if (split.hasBase()) {
      if (split.isOriginal()) {
        root = path.getParent();
      } else {
        root = path.getParent().getParent();
      }
    } else {
      root = path;
    }
    final Path[] deltas = AcidUtils.deserializeDeltas(root, split.getDeltas());
    final Configuration conf = options.getConfiguration();
    final Reader reader;
    final int bucket;
    Reader.Options readOptions = new Reader.Options();
    readOptions.range(split.getStart(), split.getLength());
    if (split.hasBase()) {
      bucket = AcidUtils.parseBaseBucketFilename(split.getPath(), conf)
          .getBucket();
      reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
      final List<OrcProto.Type> types = reader.getTypes();
      readOptions.include(genIncludedColumns(types, conf, split.isOriginal()));
      setSearchArgument(readOptions, types, conf, split.isOriginal());
    } else {
      bucket = (int) split.getStart();
      reader = null;
      if(deltas != null && deltas.length > 0) {
        Path bucketPath = AcidUtils.createBucketFile(deltas[0], bucket);
        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf);
        FileSystem fs = readerOptions.getFilesystem();
        if(fs == null) {
          fs = path.getFileSystem(options.getConfiguration());
        }
        if(fs.exists(bucketPath)) {
        /* w/o schema evolution (which ACID doesn't support yet) all delta
        files have the same schema, so choosing the 1st one*/
          final List<OrcProto.Type> types =
            OrcFile.createReader(bucketPath, readerOptions).getTypes();
          readOptions.include(genIncludedColumns(types, conf, split.isOriginal()));
          setSearchArgument(readOptions, types, conf, split.isOriginal());
        }
      }
    }
    String txnString = conf.get(ValidTxnList.VALID_TXNS_KEY,
                                Long.MAX_VALUE + ":");
    ValidTxnList validTxnList = new ValidReadTxnList(txnString);
    final OrcRawRecordMerger records =
        new OrcRawRecordMerger(conf, true, reader, split.isOriginal(), bucket,
            validTxnList, readOptions, deltas);
    return new RowReader<OrcStruct>() {
      OrcStruct innerRecord = records.createValue();

      @Override
      public ObjectInspector getObjectInspector() {
        return ((StructObjectInspector) records.getObjectInspector())
            .getAllStructFieldRefs().get(OrcRecordUpdater.ROW)
            .getFieldObjectInspector();
      }

      @Override
      public boolean next(RecordIdentifier recordIdentifier,
                          OrcStruct orcStruct) throws IOException {
        boolean result;
        // filter out the deleted records
        do {
          result = records.next(recordIdentifier, innerRecord);
        } while (result &&
            OrcRecordUpdater.getOperation(innerRecord) ==
                OrcRecordUpdater.DELETE_OPERATION);
        if (result) {
          // swap the fields with the passed in orcStruct
          orcStruct.linkFields(OrcRecordUpdater.getRow(innerRecord));
        }
        return result;
      }

      @Override
      public RecordIdentifier createKey() {
        return records.createKey();
      }

      @Override
      public OrcStruct createValue() {
        return new OrcStruct(records.getColumns());
      }

      @Override
      public long getPos() throws IOException {
        return records.getPos();
      }

      @Override
      public void close() throws IOException {
        records.close();
      }

      @Override
      public float getProgress() throws IOException {
        return records.getProgress();
      }
    };
  }

  static Path findOriginalBucket(FileSystem fs,
                                 Path directory,
                                 int bucket) throws IOException {
    for(FileStatus stat: fs.listStatus(directory)) {
      String name = stat.getPath().getName();
      String numberPart = name.substring(0, name.indexOf('_'));
      if (org.apache.commons.lang3.StringUtils.isNumeric(numberPart) &&
          Integer.parseInt(numberPart) == bucket) {
        return stat.getPath();
      }
    }
    throw new IllegalArgumentException("Can't find bucket " + bucket + " in " +
        directory);
  }

  private static boolean[] pickStripes(SearchArgument sarg, String[] sargColNames,
      WriterVersion writerVersion, boolean isOriginal, List<StripeStatistics> stripeStats,
      int stripeCount, Path filePath) {
    LOG.info("ORC pushdown predicate: " + sarg);
    if (sarg == null || stripeStats == null || writerVersion == OrcFile.WriterVersion.ORIGINAL) {
      return null; // only do split pruning if HIVE-8732 has been fixed in the writer
    }
    // eliminate stripes that doesn't satisfy the predicate condition
    List<PredicateLeaf> sargLeaves = sarg.getLeaves();
    int[] filterColumns = RecordReaderImpl.mapSargColumnsToOrcInternalColIdx(sargLeaves,
        sargColNames, getRootColumn(isOriginal));
    return pickStripesInternal(sarg, filterColumns, stripeStats, stripeCount, filePath);
  }

  private static boolean[] pickStripesInternal(SearchArgument sarg, int[] filterColumns,
      List<StripeStatistics> stripeStats, int stripeCount, Path filePath) {
    boolean[] includeStripe = new boolean[stripeCount];
    for (int i = 0; i < includeStripe.length; ++i) {
      includeStripe[i] = (i >= stripeStats.size()) ||
          isStripeSatisfyPredicate(stripeStats.get(i), sarg, filterColumns);
      if (isDebugEnabled && !includeStripe[i]) {
        LOG.debug("Eliminating ORC stripe-" + i + " of file '" + filePath
            + "'  as it did not satisfy predicate condition.");
      }
    }
    return includeStripe;
  }

  private static boolean isStripeSatisfyPredicate(
      StripeStatistics stripeStatistics, SearchArgument sarg, int[] filterColumns) {
    List<PredicateLeaf> predLeaves = sarg.getLeaves();
    TruthValue[] truthValues = new TruthValue[predLeaves.size()];
    for (int pred = 0; pred < truthValues.length; pred++) {
      if (filterColumns[pred] != -1) {

        // column statistics at index 0 contains only the number of rows
        ColumnStatistics stats = stripeStatistics.getColumnStatistics()[filterColumns[pred]];
        truthValues[pred] = RecordReaderImpl.evaluatePredicate(stats, predLeaves.get(pred), null);
      } else {

        // parition column case.
        // partition filter will be evaluated by partition pruner so
        // we will not evaluate partition filter here.
        truthValues[pred] = TruthValue.YES_NO_NULL;
      }
    }
    return sarg.evaluate(truthValues).isNeeded();
  }

  @VisibleForTesting
  static SplitStrategy determineSplitStrategy(Context context, FileSystem fs, Path dir,
      AcidUtils.Directory dirInfo, List<HdfsFileStatusWithId> baseOrOriginalFiles) {
    Path base = dirInfo.getBaseDirectory();
    List<HdfsFileStatusWithId> original = dirInfo.getOriginalFiles();
    List<DeltaMetaData> deltas = AcidUtils.serializeDeltas(dirInfo.getCurrentDirectories());
    boolean[] covered = new boolean[context.numBuckets];
    boolean isOriginal = base == null;

    // if we have a base to work from
    if (base != null || !original.isEmpty()) {
      long totalFileSize = 0;
      for (HdfsFileStatusWithId child : baseOrOriginalFiles) {
        totalFileSize += child.getFileStatus().getLen();
        AcidOutputFormat.Options opts = AcidUtils.parseBaseBucketFilename
            (child.getFileStatus().getPath(), context.conf);
        int b = opts.getBucket();
        // If the bucket is in the valid range, mark it as covered.
        // I wish Hive actually enforced bucketing all of the time.
        if (b >= 0 && b < covered.length) {
          covered[b] = true;
        }
      }

      int numFiles = baseOrOriginalFiles.size();
      long avgFileSize = totalFileSize / numFiles;
      int totalFiles = context.numFilesCounter.addAndGet(numFiles);
      switch(context.splitStrategyKind) {
        case BI:
          // BI strategy requested through config
          return new BISplitStrategy(context, fs, dir, baseOrOriginalFiles, isOriginal, deltas, covered);
        case ETL:
          // ETL strategy requested through config
          return new ETLSplitStrategy(context, fs, dir, baseOrOriginalFiles, isOriginal, deltas, covered);
        default:
          // HYBRID strategy
          if (avgFileSize > context.maxSize || totalFiles <= context.minSplits) {
            return new ETLSplitStrategy(context, fs, dir, baseOrOriginalFiles, isOriginal, deltas, covered);
          } else {
            return new BISplitStrategy(context, fs, dir, baseOrOriginalFiles, isOriginal, deltas, covered);
          }
      }
    } else {
      // no base, only deltas
      return new ACIDSplitStrategy(dir, context.numBuckets, deltas, covered);
    }
  }

  @Override
  public RawReader<OrcStruct> getRawReader(Configuration conf,
                                           boolean collapseEvents,
                                           int bucket,
                                           ValidTxnList validTxnList,
                                           Path baseDirectory,
                                           Path[] deltaDirectory
                                           ) throws IOException {
    Reader reader = null;
    boolean isOriginal = false;
    if (baseDirectory != null) {
      Path bucketFile;
      if (baseDirectory.getName().startsWith(AcidUtils.BASE_PREFIX)) {
        bucketFile = AcidUtils.createBucketFile(baseDirectory, bucket);
      } else {
        isOriginal = true;
        bucketFile = findOriginalBucket(baseDirectory.getFileSystem(conf),
            baseDirectory, bucket);
      }
      reader = OrcFile.createReader(bucketFile, OrcFile.readerOptions(conf));
    }
    return new OrcRawRecordMerger(conf, collapseEvents, reader, isOriginal,
        bucket, validTxnList, new Reader.Options(), deltaDirectory);
  }


}
