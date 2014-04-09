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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnListImpl;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordReader;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
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
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
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
    AcidInputFormat<OrcStruct> {

  private static final Log LOG = LogFactory.getLog(OrcInputFormat.class);
  static final HadoopShims SHIMS = ShimLoader.getHadoopShims();
  static final String MIN_SPLIT_SIZE =
      SHIMS.getHadoopConfNames().get("MAPREDMINSPLITSIZE");
  static final String MAX_SPLIT_SIZE =
      SHIMS.getHadoopConfNames().get("MAPREDMAXSPLITSIZE");
  static final String SARG_PUSHDOWN = "sarg.pushdown";

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
    boolean isOriginal =
        !file.hasMetadataValue(OrcRecordUpdater.ACID_KEY_INDEX_NAME);
    List<OrcProto.Type> types = file.getTypes();
    setIncludedColumns(options, types, conf, isOriginal);
    setSearchArgument(options, types, conf, isOriginal);
    return file.rowsOptions(options);
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

  /**
   * Take the configuration and figure out which columns we need to include.
   * @param options the options to update
   * @param types the types for the file
   * @param conf the configuration
   * @param isOriginal is the file in the original format?
   */
  static void setIncludedColumns(Reader.Options options,
                                 List<OrcProto.Type> types,
                                 Configuration conf,
                                 boolean isOriginal) {
    int rootColumn = getRootColumn(isOriginal);
    if (!ColumnProjectionUtils.isReadAllColumns(conf)) {
      int numColumns = types.size() - rootColumn;
      boolean[] result = new boolean[numColumns];
      result[0] = true;
      OrcProto.Type root = types.get(rootColumn);
      List<Integer> included = ColumnProjectionUtils.getReadColumnIDs(conf);
      for(int i=0; i < root.getSubtypesCount(); ++i) {
        if (included.contains(i)) {
          includeColumnRecursive(types, result, root.getSubtypes(i),
              rootColumn);
        }
      }
      options.include(result);
    } else {
      options.include(null);
    }
  }

  static void setSearchArgument(Reader.Options options,
                                List<OrcProto.Type> types,
                                Configuration conf,
                                boolean isOriginal) {
    int rootColumn = getRootColumn(isOriginal);
    String serializedPushdown = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    String sargPushdown = conf.get(SARG_PUSHDOWN);
    String columnNamesString =
        conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
    if ((sargPushdown == null && serializedPushdown == null)
        || columnNamesString == null) {
      LOG.debug("No ORC pushdown predicate");
      options.searchArgument(null, null);
    } else {
      SearchArgument sarg;
      if (serializedPushdown != null) {
        sarg = SearchArgument.FACTORY.create
            (Utilities.deserializeExpression(serializedPushdown));
      } else {
        sarg = SearchArgument.FACTORY.create(sargPushdown);
      }
      LOG.info("ORC pushdown predicate: " + sarg);
      String[] neededColumnNames = columnNamesString.split(",");
      String[] columnNames = new String[types.size() - rootColumn];
      boolean[] includedColumns = options.getInclude();
      int i = 0;
      for(int columnId: types.get(rootColumn).getSubtypesList()) {
        if (includedColumns == null || includedColumns[columnId - rootColumn]) {
          // this is guaranteed to be positive because types only have children
          // ids greater than their own id.
          columnNames[columnId - rootColumn] = neededColumnNames[i++];
        }
      }
      options.searchArgument(sarg, columnNames);
    }
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
    private final ExecutorService threadPool;
    private final List<OrcSplit> splits =
        new ArrayList<OrcSplit>(10000);
    private final int numBuckets;
    private final List<Throwable> errors = new ArrayList<Throwable>();
    private final long maxSize;
    private final long minSize;
    private final boolean footerInSplits;
    private final boolean cacheStripeDetails;
    private final AtomicInteger cacheHitCounter = new AtomicInteger(0);
    private final AtomicInteger numFilesCounter = new AtomicInteger(0);
    private Throwable fatalError = null;
    private ValidTxnList transactionList;

    /**
     * A count of the number of threads that may create more work for the
     * thread pool.
     */
    private int schedulers = 0;

    Context(Configuration conf) {
      this.conf = conf;
      minSize = conf.getLong(MIN_SPLIT_SIZE, DEFAULT_MIN_SPLIT_SIZE);
      maxSize = conf.getLong(MAX_SPLIT_SIZE, DEFAULT_MAX_SPLIT_SIZE);
      footerInSplits = HiveConf.getBoolVar(conf,
          ConfVars.HIVE_ORC_INCLUDE_FILE_FOOTER_IN_SPLITS);
      numBuckets =
          Math.max(conf.getInt(hive_metastoreConstants.BUCKET_COUNT, 0), 0);
      int cacheStripeDetailsSize = HiveConf.getIntVar(conf,
          ConfVars.HIVE_ORC_CACHE_STRIPE_DETAILS_SIZE);
      int numThreads = HiveConf.getIntVar(conf,
          ConfVars.HIVE_ORC_COMPUTE_SPLITS_NUM_THREADS);

      cacheStripeDetails = (cacheStripeDetailsSize > 0);

      threadPool = Executors.newFixedThreadPool(numThreads,
          new ThreadFactoryBuilder().setDaemon(true)
              .setNameFormat("ORC_GET_SPLITS #%d").build());

      synchronized (Context.class) {
        if (footerCache == null && cacheStripeDetails) {
          footerCache = CacheBuilder.newBuilder().concurrencyLevel(numThreads)
              .initialCapacity(cacheStripeDetailsSize).softValues().build();
        }
      }
      String value = conf.get(ValidTxnList.VALID_TXNS_KEY,
                              Long.MAX_VALUE + ":");
      transactionList = new ValidTxnListImpl(value);
    }

    int getSchedulers() {
      return schedulers;
    }

    /**
     * Get the Nth split.
     * @param index if index >= 0, count from the front, otherwise count from
     *     the back.
     * @return the Nth file split
     */
    OrcSplit getResult(int index) {
      if (index >= 0) {
        return splits.get(index);
      } else {
        return splits.get(splits.size() + index);
      }
    }

    List<Throwable> getErrors() {
      return errors;
    }

    /**
     * Add a unit of work.
     * @param runnable the object to run
     */
    synchronized void schedule(Runnable runnable) {
      if (fatalError == null) {
        if (runnable instanceof FileGenerator ||
            runnable instanceof SplitGenerator) {
          schedulers += 1;
        }
        threadPool.execute(runnable);
      } else {
        throw new RuntimeException("serious problem", fatalError);
      }
    }

    /**
     * Mark a worker that may generate more work as done.
     */
    synchronized void decrementSchedulers() {
      schedulers -= 1;
      if (schedulers == 0) {
        notify();
      }
    }

    synchronized void notifyOnNonIOException(Throwable th) {
      fatalError = th;
      notify();
    }

    /**
     * Wait until all of the tasks are done. It waits until all of the
     * threads that may create more work are done and then shuts down the
     * thread pool and waits for the final threads to finish.
     */
    synchronized void waitForTasks() {
      try {
        while (schedulers != 0) {
          wait();
          if (fatalError != null) {
            threadPool.shutdownNow();
            throw new RuntimeException("serious problem", fatalError);
          }
        }
        threadPool.shutdown();
        threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      } catch (InterruptedException ie) {
        throw new IllegalStateException("interrupted", ie);
      }
    }
  }

  /**
   * Given a directory, get the list of files and blocks in those files.
   * A thread is used for each directory.
   */
  static final class FileGenerator implements Runnable {
    private final Context context;
    private final FileSystem fs;
    private final Path dir;

    FileGenerator(Context context, FileSystem fs, Path dir) {
      this.context = context;
      this.fs = fs;
      this.dir = dir;
    }

    private void scheduleSplits(FileStatus file,
                                boolean isOriginal,
                                boolean hasBase,
                                List<Long> deltas) throws IOException{
      FileInfo info = null;
      if (context.cacheStripeDetails) {
        info = verifyCachedFileInfo(file);
      }
      new SplitGenerator(context, fs, file, info, isOriginal, deltas,
          hasBase).schedule();
    }

    /**
     * For each path, get the list of files and blocks that they consist of.
     */
    @Override
    public void run() {
      try {
        AcidUtils.Directory dirInfo = AcidUtils.getAcidState(dir,
            context.conf, context.transactionList);
        List<Long> deltas =
            AcidUtils.serializeDeltas(dirInfo.getCurrentDirectories());
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

          // for each child, schedule splits and mark off the bucket
          for(FileStatus child: children) {
            AcidOutputFormat.Options opts = AcidUtils.parseBaseBucketFilename
                (child.getPath(), context.conf);
            scheduleSplits(child, isOriginal, true, deltas);
            int b = opts.getBucket();
            // If the bucket is in the valid range, mark it as covered.
            // I wish Hive actually enforced bucketing all of the time.
            if (b >= 0 && b < covered.length) {
              covered[b] = true;
            }
          }
        }

        // Generate a split for any buckets that weren't covered.
        // This happens in the case where a bucket just has deltas and no
        // base.
        if (!deltas.isEmpty()) {
          for (int b = 0; b < context.numBuckets; ++b) {
            if (!covered[b]) {
              context.splits.add(new OrcSplit(dir, b, 0, new String[0], null,
                  false, false, deltas));
            }
          }
        }
      } catch (Throwable th) {
        if (!(th instanceof IOException)) {
          LOG.error("Unexpected Exception", th);
        }
        synchronized (context.errors) {
          context.errors.add(th);
        }
        if (!(th instanceof IOException)) {
          context.notifyOnNonIOException(th);
        }
      } finally {
        context.decrementSchedulers();
      }
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
  }

  /**
   * Split the stripes of a given file into input splits.
   * A thread is used for each file.
   */
  static final class SplitGenerator implements Runnable {
    private final Context context;
    private final FileSystem fs;
    private final FileStatus file;
    private final long blockSize;
    private final BlockLocation[] locations;
    private final FileInfo fileInfo;
    private List<StripeInformation> stripes;
    private ReaderImpl.FileMetaInfo fileMetaInfo;
    private Metadata metadata;
    private List<OrcProto.Type> types;
    private final boolean isOriginal;
    private final List<Long> deltas;
    private final boolean hasBase;

    SplitGenerator(Context context, FileSystem fs,
                   FileStatus file, FileInfo fileInfo,
                   boolean isOriginal,
                   List<Long> deltas,
                   boolean hasBase) throws IOException {
      this.context = context;
      this.fs = fs;
      this.file = file;
      this.blockSize = file.getBlockSize();
      this.fileInfo = fileInfo;
      locations = SHIMS.getLocations(fs, file);
      this.isOriginal = isOriginal;
      this.deltas = deltas;
      this.hasBase = hasBase;
    }

    Path getPath() {
      return file.getPath();
    }

    void schedule() throws IOException {
      if(locations.length == 1 && file.getLen() < context.maxSize) {
        String[] hosts = locations[0].getHosts();
        synchronized (context.splits) {
          context.splits.add(new OrcSplit(file.getPath(), 0, file.getLen(),
                hosts, fileMetaInfo, isOriginal, hasBase, deltas));
        }
      } else {
        // if it requires a compute task
        context.schedule(this);
      }
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
    void createSplit(long offset, long length,
                     ReaderImpl.FileMetaInfo fileMetaInfo) throws IOException {
      String[] hosts;
      if ((offset % blockSize) + length <= blockSize) {
        // handle the single block case
        hosts = locations[(int) (offset / blockSize)].getHosts();
      } else {
        // Calculate the number of bytes in the split that are local to each
        // host.
        Map<String, LongWritable> sizes = new HashMap<String, LongWritable>();
        long maxSize = 0;
        for(BlockLocation block: locations) {
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
          }
        }
        // filter the list of locations to those that have at least 80% of the
        // max
        long threshold = (long) (maxSize * MIN_INCLUDED_LOCATION);
        List<String> hostList = new ArrayList<String>();
        // build the locations in a predictable order to simplify testing
        for(BlockLocation block: locations) {
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
      synchronized (context.splits) {
        context.splits.add(new OrcSplit(file.getPath(), offset, length,
            hosts, fileMetaInfo, isOriginal, hasBase, deltas));
      }
    }

    /**
     * Divide the adjacent stripes in the file into input splits based on the
     * block size and the configured minimum and maximum sizes.
     */
    @Override
    public void run() {
      try {
        populateAndCacheStripeDetails();

        // figure out which stripes we need to read
        boolean[] includeStripe = null;
        // we can't eliminate stripes if there are deltas because the
        // deltas may change the rows making them match the predicate.
        if (deltas.isEmpty()) {
          Reader.Options options = new Reader.Options();
          setIncludedColumns(options, types, context.conf, isOriginal);
          setSearchArgument(options, types, context.conf, isOriginal);
          if (options.getSearchArgument() != null) {
            SearchArgument sarg = options.getSearchArgument();
            List<PredicateLeaf> sargLeaves = sarg.getLeaves();
            List<StripeStatistics> stripeStats = metadata.getStripeStatistics();
            int[] filterColumns = RecordReaderImpl.mapSargColumns(sargLeaves,
                options.getColumnNames(), getRootColumn(isOriginal));

            if (stripeStats != null) {
              // eliminate stripes that doesn't satisfy the predicate condition
              includeStripe = new boolean[stripes.size()];
              for(int i=0; i < stripes.size(); ++i) {
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
        for(StripeInformation stripe: stripes) {
          idx++;

          if (!includeStripe[idx]) {
            // create split for the previous unfinished stripe
            if (currentOffset != -1) {
              createSplit(currentOffset, currentLength, fileMetaInfo);
              currentOffset = -1;
            }
            continue;
          }

          // if we are working on a stripe, over the min stripe size, and
          // crossed a block boundary, cut the input split here.
          if (currentOffset != -1 && currentLength > context.minSize &&
              (currentOffset / blockSize != stripe.getOffset() / blockSize)) {
            createSplit(currentOffset, currentLength, fileMetaInfo);
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
            createSplit(currentOffset, currentLength, fileMetaInfo);
            currentOffset = -1;
          }
        }
        if (currentOffset != -1) {
          createSplit(currentOffset, currentLength, fileMetaInfo);
        }
      } catch (Throwable th) {
        if (!(th instanceof IOException)) {
          LOG.error("Unexpected Exception", th);
        }
        synchronized (context.errors) {
          context.errors.add(th);
        }
        if (!(th instanceof IOException)) {
          context.notifyOnNonIOException(th);
        }
      } finally {
        context.decrementSchedulers();
      }
    }

    private void populateAndCacheStripeDetails() {
      try {
        Reader orcReader;
        if (fileInfo != null) {
          stripes = fileInfo.stripeInfos;
          fileMetaInfo = fileInfo.fileMetaInfo;
          metadata = fileInfo.metadata;
          types = fileInfo.types;
          // For multiple runs, in case sendSplitsInFooter changes
          if (fileMetaInfo == null && context.footerInSplits) {
            orcReader = OrcFile.createReader(file.getPath(),
                OrcFile.readerOptions(context.conf).filesystem(fs));
            fileInfo.fileMetaInfo = ((ReaderImpl) orcReader).getFileMetaInfo();
            fileInfo.metadata = orcReader.getMetadata();
            fileInfo.types = orcReader.getTypes();
          }
        } else {
          orcReader = OrcFile.createReader(file.getPath(),
              OrcFile.readerOptions(context.conf).filesystem(fs));
          stripes = orcReader.getStripes();
          metadata = orcReader.getMetadata();
          types = orcReader.getTypes();
          fileMetaInfo = context.footerInSplits ?
              ((ReaderImpl) orcReader).getFileMetaInfo() : null;
          if (context.cacheStripeDetails) {
            // Populate into cache.
            Context.footerCache.put(file.getPath(),
                new FileInfo(file.getModificationTime(), file.getLen(), stripes,
                    metadata, types, fileMetaInfo));
          }
        }
      } catch (Throwable th) {
        if (!(th instanceof IOException)) {
          LOG.error("Unexpected Exception", th);
        }
        synchronized (context.errors) {
          context.errors.add(th);
        }
        if (!(th instanceof IOException)) {
          context.notifyOnNonIOException(th);
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
          ColumnStatistics stats =
              stripeStatistics.getColumnStatistics()[filterColumns[pred]];
          Object minValue = RecordReaderImpl.getMin(stats);
          Object maxValue = RecordReaderImpl.getMax(stats);
          truthValues[pred] =
              RecordReaderImpl.evaluatePredicateRange(predLeaves.get(pred),
                  minValue, maxValue);
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
	  for(Path dir: getInputPaths(conf)) {
	    FileSystem fs = dir.getFileSystem(conf);
	    context.schedule(new FileGenerator(context, fs, dir));
	  }
	  context.waitForTasks();
	  // deal with exceptions
	  if (!context.errors.isEmpty()) {
	    List<IOException> errors =
	        new ArrayList<IOException>(context.errors.size());
	    for(Throwable th: context.errors) {
	      if (th instanceof IOException) {
	        errors.add((IOException) th);
	      } else {
	        throw new RuntimeException("serious problem", th);
	      }
	    }
	    throw new InvalidInputException(errors);
	  }
    if (context.cacheStripeDetails) {
      LOG.info("FooterCacheHitRatio: " + context.cacheHitCounter.get() + "/"
          + context.numFilesCounter.get());
    }
	  return context.splits;
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


    FileInfo(long modificationTime, long size,
             List<StripeInformation> stripeInfos,
             Metadata metadata, List<OrcProto.Type> types,
             ReaderImpl.FileMetaInfo fileMetaInfo) {
      this.modificationTime = modificationTime;
      this.size = size;
      this.stripeInfos = stripeInfos;
      this.fileMetaInfo = fileMetaInfo;
      this.metadata = metadata;
      this.types = types;
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
    // we know it is not ACID.
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

    // if we are strictly old-school, just use the old code
    if (split.isOriginal() && split.getDeltas().isEmpty()) {
      if (vectorMode) {
        return createVectorizedReader(inputSplit, conf, reporter);
      } else {
        return new OrcRecordReader(OrcFile.createReader(split.getPath(),
            OrcFile.readerOptions(conf)), conf, split);
      }
    }

    Options options = new Options(conf).reporter(reporter);
    final RowReader<OrcStruct> inner = getReader(inputSplit, options);
    if (vectorMode) {
      return (org.apache.hadoop.mapred.RecordReader)
          new VectorizedOrcAcidRowReader(inner, conf, (FileSplit) inputSplit);
    }
    final RecordIdentifier id = inner.createKey();

    // Return a RecordReader that is compatible with the Hive 0.12 reader
    // with NullWritable for the key instead of RecordIdentifier.
    return new org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct>(){
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
    };
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
      setIncludedColumns(readOptions, types, conf, split.isOriginal());
      setSearchArgument(readOptions, types, conf, split.isOriginal());
    } else {
      bucket = (int) split.getStart();
      reader = null;
    }
    String txnString = conf.get(ValidTxnList.VALID_TXNS_KEY,
                                Long.MAX_VALUE + ":");
    ValidTxnList validTxnList = new ValidTxnListImpl(txnString);
    final OrcRawRecordMerger records =
        new OrcRawRecordMerger(conf, true, reader, split.isOriginal(), bucket,
            validTxnList, readOptions, deltas);
    return new RowReader<OrcStruct>() {
      OrcStruct innerRecord = records.createValue();

      @Override
      public ObjectInspector getObjectInspector() {
        return ((StructObjectInspector) reader.getObjectInspector())
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
      if (Integer.parseInt(name.substring(0, name.indexOf('_'))) == bucket) {
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
