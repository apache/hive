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
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordReader;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

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
  InputFormatChecker, VectorizedInputFormatInterface,
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

  private static final PerfLogger perfLogger = PerfLogger.getPerfLogger();
  private static final String CLASS_NAME = ReaderImpl.class.getName();

  /**
   * When picking the hosts for a split that crosses block boundaries,
   * any drop any host that has fewer than MIN_INCLUDED_LOCATION of the
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
    for(int i=0; i < root.getSubtypesCount(); ++i) {
      if (included.contains(i)) {
        includeColumnRecursive(types, result, root.getSubtypes(i),
            rootColumn);
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
    String columnNamesString = conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
    if (columnNamesString == null) {
      LOG.debug("No ORC pushdown predicate - no column names");
      options.searchArgument(null, null);
      return;
    }
    SearchArgument sarg = SearchArgumentFactory.createFromConf(conf);
    if (sarg == null) {
      LOG.debug("No ORC pushdown predicate");
      options.searchArgument(null, null);
      return;
    }

    LOG.info("ORC pushdown predicate: " + sarg);
    options.searchArgument(sarg, getSargColumnNames(
        columnNamesString.split(","), types, options.getInclude(), isOriginal));
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
    private final boolean footerInSplits;
    private final boolean cacheStripeDetails;
    private final AtomicInteger cacheHitCounter = new AtomicInteger(0);
    private final AtomicInteger numFilesCounter = new AtomicInteger(0);
    private ValidTxnList transactionList;
    private SplitStrategyKind splitStrategyKind;

    Context(Configuration conf) {
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

      synchronized (Context.class) {
        if (threadPool == null) {
          threadPool = Executors.newFixedThreadPool(numThreads,
              new ThreadFactoryBuilder().setDaemon(true)
                  .setNameFormat("ORC_GET_SPLITS #%d").build());
        }

        if (footerCache == null && cacheStripeDetails) {
          footerCache = CacheBuilder.newBuilder().concurrencyLevel(numThreads)
              .initialCapacity(cacheStripeDetailsSize).softValues().build();
        }
      }
      String value = conf.get(ValidTxnList.VALID_TXNS_KEY,
                              Long.MAX_VALUE + ":");
      transactionList = new ValidReadTxnList(value);
    }
  }

  interface SplitStrategy<T> {
    List<T> getSplits() throws IOException;
  }

  static final class SplitInfo extends ACIDSplitStrategy {
    private final Context context;
    private final FileSystem fs;
    private final FileStatus file;
    private final FileInfo fileInfo;
    private final boolean isOriginal;
    private final List<Long> deltas;
    private final boolean hasBase;

    SplitInfo(Context context, FileSystem fs,
        FileStatus file, FileInfo fileInfo,
        boolean isOriginal,
        List<Long> deltas,
        boolean hasBase, Path dir, boolean[] covered) throws IOException {
      super(dir, context.numBuckets, deltas, covered);
      this.context = context;
      this.fs = fs;
      this.file = file;
      this.fileInfo = fileInfo;
      this.isOriginal = isOriginal;
      this.deltas = deltas;
      this.hasBase = hasBase;
    }
  }

  /**
   * ETL strategy is used when spending little more time in split generation is acceptable
   * (split generation reads and caches file footers).
   */
  static final class ETLSplitStrategy implements SplitStrategy<SplitInfo> {
    Context context;
    FileSystem fs;
    List<FileStatus> files;
    boolean isOriginal;
    List<Long> deltas;
    Path dir;
    boolean[] covered;

    public ETLSplitStrategy(Context context, FileSystem fs, Path dir, List<FileStatus> children,
        boolean isOriginal, List<Long> deltas, boolean[] covered) {
      this.context = context;
      this.dir = dir;
      this.fs = fs;
      this.files = children;
      this.isOriginal = isOriginal;
      this.deltas = deltas;
      this.covered = covered;
    }

    private FileInfo verifyCachedFileInfo(FileStatus file) {
      context.numFilesCounter.incrementAndGet();
      FileInfo fileInfo = Context.footerCache.getIfPresent(file.getPath());
      if (fileInfo != null) {
        if (LOG.isDebugEnabled()) {
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
          if (LOG.isDebugEnabled()) {
            LOG.debug("Meta-Info for : " + file.getPath() +
                " changed. CachedModificationTime: "
                + fileInfo.modificationTime + ", CurrentModificationTime: "
                + file.getModificationTime()
                + ", CachedLength: " + fileInfo.size + ", CurrentLength: " +
                file.getLen());
          }
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Info not cached for path: " + file.getPath());
        }
      }
      return null;
    }

    @Override
    public List<SplitInfo> getSplits() throws IOException {
      List<SplitInfo> result = Lists.newArrayList();
      for (FileStatus file : files) {
        FileInfo info = null;
        if (context.cacheStripeDetails) {
          info = verifyCachedFileInfo(file);
        }
        // ignore files of 0 length
        if (file.getLen() > 0) {
          result.add(new SplitInfo(context, fs, file, info, isOriginal, deltas, true, dir, covered));
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
    List<FileStatus> fileStatuses;
    boolean isOriginal;
    List<Long> deltas;
    FileSystem fs;
    Context context;
    Path dir;

    public BISplitStrategy(Context context, FileSystem fs,
        Path dir, List<FileStatus> fileStatuses, boolean isOriginal,
        List<Long> deltas, boolean[] covered) {
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
      for (FileStatus fileStatus : fileStatuses) {
        String[] hosts = SHIMS.getLocationsWithOffset(fs, fileStatus).firstEntry().getValue()
            .getHosts();
        OrcSplit orcSplit = new OrcSplit(fileStatus.getPath(), 0, fileStatus.getLen(), hosts,
            null, isOriginal, true, deltas, -1);
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
    List<Long> deltas;
    boolean[] covered;
    int numBuckets;

    public ACIDSplitStrategy(Path dir, int numBuckets, List<Long> deltas, boolean[] covered) {
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
            splits.add(new OrcSplit(dir, b, 0, new String[0], null, false, false, deltas, -1));
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
  static final class FileGenerator implements Callable<SplitStrategy> {
    private final Context context;
    private final FileSystem fs;
    private final Path dir;

    FileGenerator(Context context, FileSystem fs, Path dir) {
      this.context = context;
      this.fs = fs;
      this.dir = dir;
    }

    @Override
    public SplitStrategy call() throws IOException {
      final SplitStrategy splitStrategy;
      AcidUtils.Directory dirInfo = AcidUtils.getAcidState(dir,
          context.conf, context.transactionList);
      List<Long> deltas = AcidUtils.serializeDeltas(dirInfo.getCurrentDirectories());
      Path base = dirInfo.getBaseDirectory();
      List<FileStatus> original = dirInfo.getOriginalFiles();
      boolean[] covered = new boolean[context.numBuckets];
      boolean isOriginal = base == null;

      // if we have a base to work from
      if (base != null || !original.isEmpty()) {

        // find the base files (original or new style)
        List<FileStatus> children = original;
        if (base != null) {
          children = SHIMS.listLocatedStatus(fs, base,
              AcidUtils.hiddenFileFilter);
        }

        long totalFileSize = 0;
        for (FileStatus child : children) {
          totalFileSize += child.getLen();
          AcidOutputFormat.Options opts = AcidUtils.parseBaseBucketFilename
              (child.getPath(), context.conf);
          int b = opts.getBucket();
          // If the bucket is in the valid range, mark it as covered.
          // I wish Hive actually enforced bucketing all of the time.
          if (b >= 0 && b < covered.length) {
            covered[b] = true;
          }
        }

        int numFiles = children.size();
        long avgFileSize = totalFileSize / numFiles;
        switch(context.splitStrategyKind) {
          case BI:
            // BI strategy requested through config
            splitStrategy = new BISplitStrategy(context, fs, dir, children, isOriginal,
                deltas, covered);
            break;
          case ETL:
            // ETL strategy requested through config
            splitStrategy = new ETLSplitStrategy(context, fs, dir, children, isOriginal,
                deltas, covered);
            break;
          default:
            // HYBRID strategy
            if (avgFileSize > context.maxSize) {
              splitStrategy = new ETLSplitStrategy(context, fs, dir, children, isOriginal, deltas,
                  covered);
            } else {
              splitStrategy = new BISplitStrategy(context, fs, dir, children, isOriginal, deltas,
                  covered);
            }
            break;
        }
      } else {
        // no base, only deltas
        splitStrategy = new ACIDSplitStrategy(dir, context.numBuckets, deltas, covered);
      }

      return splitStrategy;
    }
  }

  /**
   * Split the stripes of a given file into input splits.
   * A thread is used for each file.
   */
  static final class SplitGenerator implements Callable<List<OrcSplit>> {
    private final Context context;
    private final FileSystem fs;
    private final FileStatus file;
    private final long blockSize;
    private final TreeMap<Long, BlockLocation> locations;
    private final FileInfo fileInfo;
    private List<StripeInformation> stripes;
    private ReaderImpl.FileMetaInfo fileMetaInfo;
    private Metadata metadata;
    private List<OrcProto.Type> types;
    private final boolean isOriginal;
    private final List<Long> deltas;
    private final boolean hasBase;
    private OrcFile.WriterVersion writerVersion;
    private long projColsUncompressedSize;
    private List<OrcSplit> deltaSplits;

    public SplitGenerator(SplitInfo splitInfo) throws IOException {
      this.context = splitInfo.context;
      this.fs = splitInfo.fs;
      this.file = splitInfo.file;
      this.blockSize = file.getBlockSize();
      this.fileInfo = splitInfo.fileInfo;
      locations = SHIMS.getLocationsWithOffset(fs, file);
      this.isOriginal = splitInfo.isOriginal;
      this.deltas = splitInfo.deltas;
      this.hasBase = splitInfo.hasBase;
      this.projColsUncompressedSize = -1;
      this.deltaSplits = splitInfo.getSplits();
    }

    Path getPath() {
      return file.getPath();
    }

    @Override
    public String toString() {
      return "splitter(" + file.getPath() + ")";
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
                     ReaderImpl.FileMetaInfo fileMetaInfo) throws IOException {
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
            throw new IOException("File " + file.getPath().toString() +
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
      return new OrcSplit(file.getPath(), offset, length, hosts, fileMetaInfo,
          isOriginal, hasBase, deltas, projColsUncompressedSize);
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
      if (deltas.isEmpty()) {
        Reader.Options options = new Reader.Options();
        options.include(genIncludedColumns(types, context.conf, isOriginal));
        setSearchArgument(options, types, context.conf, isOriginal);
        // only do split pruning if HIVE-8732 has been fixed in the writer
        if (options.getSearchArgument() != null &&
            writerVersion != OrcFile.WriterVersion.ORIGINAL) {
          SearchArgument sarg = options.getSearchArgument();
          List<PredicateLeaf> sargLeaves = sarg.getLeaves();
          List<StripeStatistics> stripeStats = metadata.getStripeStatistics();
          int[] filterColumns = RecordReaderImpl.mapSargColumns(sargLeaves,
              options.getColumnNames(), getRootColumn(isOriginal));

          if (stripeStats != null) {
            // eliminate stripes that doesn't satisfy the predicate condition
            includeStripe = new boolean[stripes.size()];
            for (int i = 0; i < stripes.size(); ++i) {
              includeStripe[i] = (i >= stripeStats.size()) ||
                  isStripeSatisfyPredicate(stripeStats.get(i), sarg,
                      filterColumns);
              if (LOG.isDebugEnabled() && !includeStripe[i]) {
                LOG.debug("Eliminating ORC stripe-" + i + " of file '" +
                    file.getPath() + "'  as it did not satisfy " +
                    "predicate condition.");
              }
            }
          }
        }
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
      Reader orcReader = OrcFile.createReader(file.getPath(),
          OrcFile.readerOptions(context.conf).filesystem(fs));
      List<String> projCols = ColumnProjectionUtils.getReadColumnNames(context.conf);
      projColsUncompressedSize = orcReader.getRawDataSizeOfColumns(projCols);
      if (fileInfo != null) {
        stripes = fileInfo.stripeInfos;
        fileMetaInfo = fileInfo.fileMetaInfo;
        metadata = fileInfo.metadata;
        types = fileInfo.types;
        writerVersion = fileInfo.writerVersion;
        // For multiple runs, in case sendSplitsInFooter changes
        if (fileMetaInfo == null && context.footerInSplits) {
          fileInfo.fileMetaInfo = ((ReaderImpl) orcReader).getFileMetaInfo();
          fileInfo.metadata = orcReader.getMetadata();
          fileInfo.types = orcReader.getTypes();
          fileInfo.writerVersion = orcReader.getWriterVersion();
        }
      } else {
        stripes = orcReader.getStripes();
        metadata = orcReader.getMetadata();
        types = orcReader.getTypes();
        writerVersion = orcReader.getWriterVersion();
        fileMetaInfo = context.footerInSplits ?
            ((ReaderImpl) orcReader).getFileMetaInfo() : null;
        if (context.cacheStripeDetails) {
          // Populate into cache.
          Context.footerCache.put(file.getPath(),
              new FileInfo(file.getModificationTime(), file.getLen(), stripes,
                  metadata, types, fileMetaInfo, writerVersion));
        }
      }
    }

    private boolean isStripeSatisfyPredicate(StripeStatistics stripeStatistics,
                                             SearchArgument sarg,
                                             int[] filterColumns) {
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
  }

  static List<OrcSplit> generateSplitsInfo(Configuration conf)
      throws IOException {
    // use threads to resolve directories into splits
    Context context = new Context(conf);
    List<OrcSplit> splits = Lists.newArrayList();
    List<Future<?>> pathFutures = Lists.newArrayList();
    List<Future<?>> splitFutures = Lists.newArrayList();

    // multi-threaded file statuses and split strategy
    for (Path dir : getInputPaths(conf)) {
      FileSystem fs = dir.getFileSystem(conf);
      FileGenerator fileGenerator = new FileGenerator(context, fs, dir);
      pathFutures.add(context.threadPool.submit(fileGenerator));
    }

    // complete path futures and schedule split generation
    try {
      for (Future<?> pathFuture : pathFutures) {
        SplitStrategy splitStrategy = (SplitStrategy) pathFuture.get();

        if (isDebugEnabled) {
          LOG.debug(splitStrategy);
        }

        if (splitStrategy instanceof ETLSplitStrategy) {
          List<SplitInfo> splitInfos = splitStrategy.getSplits();
          for (SplitInfo splitInfo : splitInfos) {
            splitFutures.add(context.threadPool.submit(new SplitGenerator(splitInfo)));
          }
        } else {
          splits.addAll(splitStrategy.getSplits());
        }
      }

      // complete split futures
      for (Future<?> splitFuture : splitFutures) {
        splits.addAll((Collection<? extends OrcSplit>) splitFuture.get());
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
            + split.getProjectedColumnsUncompressedSize());
      }
    }
    return splits;
  }

  private static void cancelFutures(List<Future<?>> futures) {
    for (Future future : futures) {
      future.cancel(true);
    }
  }

  @Override
  public InputSplit[] getSplits(JobConf job,
                                int numSplits) throws IOException {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.ORC_GET_SPLITS);
    List<OrcSplit> result = generateSplitsInfo(job);
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.ORC_GET_SPLITS);
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
    ReaderImpl.FileMetaInfo fileMetaInfo;
    Metadata metadata;
    List<OrcProto.Type> types;
    private OrcFile.WriterVersion writerVersion;


    FileInfo(long modificationTime, long size,
             List<StripeInformation> stripeInfos,
             Metadata metadata, List<OrcProto.Type> types,
             ReaderImpl.FileMetaInfo fileMetaInfo,
             OrcFile.WriterVersion writerVersion) {
      this.modificationTime = modificationTime;
      this.size = size;
      this.stripeInfos = stripeInfos;
      this.fileMetaInfo = fileMetaInfo;
      this.metadata = metadata;
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
