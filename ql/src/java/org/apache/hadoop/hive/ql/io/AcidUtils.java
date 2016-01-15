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

package org.apache.hadoop.hive.ql.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileStatusWithId;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Utilities that are shared by all of the ACID input and output formats. They
 * are used by the compactor and cleaner and thus must be format agnostic.
 */
public class AcidUtils {
  // This key will be put in the conf file when planning an acid operation
  public static final String CONF_ACID_KEY = "hive.doing.acid";
  public static final String BASE_PREFIX = "base_";
  public static final PathFilter baseFileFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return path.getName().startsWith(BASE_PREFIX);
    }
  };
  public static final String DELTA_PREFIX = "delta_";
  public static final String DELTA_SIDE_FILE_SUFFIX = "_flush_length";
  public static final PathFilter deltaFileFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return path.getName().startsWith(DELTA_PREFIX);
    }
  };
  public static final String BUCKET_PREFIX = "bucket_";
  public static final PathFilter bucketFileFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return path.getName().startsWith(BUCKET_PREFIX) &&
          !path.getName().endsWith(DELTA_SIDE_FILE_SUFFIX);
    }
  };
  public static final String BUCKET_DIGITS = "%05d";
  public static final String LEGACY_FILE_BUCKET_DIGITS = "%06d";
  public static final String DELTA_DIGITS = "%07d";
  /**
   * 10K statements per tx.  Probably overkill ... since that many delta files
   * would not be good for performance
   */
  public static final String STATEMENT_DIGITS = "%04d";
  /**
   * This must be in sync with {@link #STATEMENT_DIGITS}
   */
  public static final int MAX_STATEMENTS_PER_TXN = 10000;
  public static final Pattern BUCKET_DIGIT_PATTERN = Pattern.compile("[0-9]{5}$");
  public static final Pattern   LEGACY_BUCKET_DIGIT_PATTERN = Pattern.compile("^[0-9]{6}");
  public static final PathFilter originalBucketFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return ORIGINAL_PATTERN.matcher(path.getName()).matches();
    }
  };

  private AcidUtils() {
    // NOT USED
  }
  private static final Logger LOG = LoggerFactory.getLogger(AcidUtils.class);

  private static final Pattern ORIGINAL_PATTERN =
      Pattern.compile("[0-9]+_[0-9]+");

  public static final PathFilter hiddenFileFilter = new PathFilter(){
    public boolean accept(Path p){
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  private static final HadoopShims SHIMS = ShimLoader.getHadoopShims();

  /**
   * Create the bucket filename.
   * @param subdir the subdirectory for the bucket.
   * @param bucket the bucket number
   * @return the filename
   */
  public static Path createBucketFile(Path subdir, int bucket) {
    return new Path(subdir,
        BUCKET_PREFIX + String.format(BUCKET_DIGITS, bucket));
  }

  /**
   * This is format of delta dir name prior to Hive 1.3.x
   */
  public static String deltaSubdir(long min, long max) {
    return DELTA_PREFIX + String.format(DELTA_DIGITS, min) + "_" +
        String.format(DELTA_DIGITS, max);
  }

  /**
   * Each write statement in a transaction creates its own delta dir.
   * @since 1.3.x
   */
  public static String deltaSubdir(long min, long max, int statementId) {
    return deltaSubdir(min, max) + "_" + String.format(STATEMENT_DIGITS, statementId);
  }

  public static String baseDir(long txnId) {
    return BASE_PREFIX + String.format(DELTA_DIGITS, txnId);
  }
  /**
   * Create a filename for a bucket file.
   * @param directory the partition directory
   * @param options the options for writing the bucket
   * @return the filename that should store the bucket
   */
  public static Path createFilename(Path directory,
                                    AcidOutputFormat.Options options) {
    String subdir;
    if (options.getOldStyle()) {
      return new Path(directory, String.format(LEGACY_FILE_BUCKET_DIGITS,
          options.getBucket()) + "_0");
    } else if (options.isWritingBase()) {
      subdir = BASE_PREFIX + String.format(DELTA_DIGITS,
          options.getMaximumTransactionId());
    } else if(options.getStatementId() == -1) {
      //when minor compaction runs, we collapse per statement delta files inside a single
      //transaction so we no longer need a statementId in the file name
      subdir = deltaSubdir(options.getMinimumTransactionId(),
        options.getMaximumTransactionId());
    } else {
      subdir = deltaSubdir(options.getMinimumTransactionId(),
        options.getMaximumTransactionId(),
        options.getStatementId());
    }
    return createBucketFile(new Path(directory, subdir), options.getBucket());
  }

  /**
   * Get the transaction id from a base directory name.
   * @param path the base directory name
   * @return the maximum transaction id that is included
   */
  static long parseBase(Path path) {
    String filename = path.getName();
    if (filename.startsWith(BASE_PREFIX)) {
      return Long.parseLong(filename.substring(BASE_PREFIX.length()));
    }
    throw new IllegalArgumentException(filename + " does not start with " +
        BASE_PREFIX);
  }

  /**
   * Parse a bucket filename back into the options that would have created
   * the file.
   * @param bucketFile the path to a bucket file
   * @param conf the configuration
   * @return the options used to create that filename
   */
  public static AcidOutputFormat.Options
                    parseBaseBucketFilename(Path bucketFile,
                                            Configuration conf) {
    AcidOutputFormat.Options result = new AcidOutputFormat.Options(conf);
    String filename = bucketFile.getName();
    result.writingBase(true);
    if (ORIGINAL_PATTERN.matcher(filename).matches()) {
      int bucket =
          Integer.parseInt(filename.substring(0, filename.indexOf('_')));
      result
          .setOldStyle(true)
          .minimumTransactionId(0)
          .maximumTransactionId(0)
          .bucket(bucket);
    } else if (filename.startsWith(BUCKET_PREFIX)) {
      int bucket =
          Integer.parseInt(filename.substring(filename.indexOf('_') + 1));
      result
          .setOldStyle(false)
          .minimumTransactionId(0)
          .maximumTransactionId(parseBase(bucketFile.getParent()))
          .bucket(bucket);
    } else {
      result.setOldStyle(true).bucket(-1).minimumTransactionId(0)
          .maximumTransactionId(0);
    }
    return result;
  }

  public enum Operation { NOT_ACID, INSERT, UPDATE, DELETE }

  public static interface Directory {

    /**
     * Get the base directory.
     * @return the base directory to read
     */
    Path getBaseDirectory();

    /**
     * Get the list of original files.  Not {@code null}.
     * @return the list of original files (eg. 000000_0)
     */
    List<HdfsFileStatusWithId> getOriginalFiles();

    /**
     * Get the list of base and delta directories that are valid and not
     * obsolete.  Not {@code null}.  List must be sorted in a specific way.
     * See {@link org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDelta#compareTo(org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDelta)}
     * for details.
     * @return the minimal list of current directories
     */
    List<ParsedDelta> getCurrentDirectories();

    /**
     * Get the list of obsolete directories. After filtering out bases and
     * deltas that are not selected by the valid transaction list, return the
     * list of original files, bases, and deltas that have been replaced by
     * more up to date ones.  Not {@code null}.
     */
    List<FileStatus> getObsolete();
  }

  public static class ParsedDelta implements Comparable<ParsedDelta> {
    private final long minTransaction;
    private final long maxTransaction;
    private final FileStatus path;
    //-1 is for internal (getAcidState()) purposes and means the delta dir
    //had no statement ID
    private final int statementId;

    /**
     * for pre 1.3.x delta files
     */
    ParsedDelta(long min, long max, FileStatus path) {
      this(min, max, path, -1);
    }
    ParsedDelta(long min, long max, FileStatus path, int statementId) {
      this.minTransaction = min;
      this.maxTransaction = max;
      this.path = path;
      this.statementId = statementId;
    }

    public long getMinTransaction() {
      return minTransaction;
    }

    public long getMaxTransaction() {
      return maxTransaction;
    }

    public Path getPath() {
      return path.getPath();
    }

    public int getStatementId() {
      return statementId == -1 ? 0 : statementId;
    }

    /**
     * Compactions (Major/Minor) merge deltas/bases but delete of old files
     * happens in a different process; thus it's possible to have bases/deltas with
     * overlapping txnId boundaries.  The sort order helps figure out the "best" set of files
     * to use to get data.
     * This sorts "wider" delta before "narrower" i.e. delta_5_20 sorts before delta_5_10 (and delta_11_20)
     */
    @Override
    public int compareTo(ParsedDelta parsedDelta) {
      if (minTransaction != parsedDelta.minTransaction) {
        if (minTransaction < parsedDelta.minTransaction) {
          return -1;
        } else {
          return 1;
        }
      } else if (maxTransaction != parsedDelta.maxTransaction) {
        if (maxTransaction < parsedDelta.maxTransaction) {
          return 1;
        } else {
          return -1;
        }
      }
      else if(statementId != parsedDelta.statementId) {
        /**
         * We want deltas after minor compaction (w/o statementId) to sort
         * earlier so that getAcidState() considers compacted files (into larger ones) obsolete
         * Before compaction, include deltas with all statementIds for a given txnId
         * in a {@link org.apache.hadoop.hive.ql.io.AcidUtils.Directory}
         */
        if(statementId < parsedDelta.statementId) {
          return -1;
        }
        else {
          return 1;
        }
      }
      else {
        return path.compareTo(parsedDelta.path);
      }
    }
  }

  /**
   * Convert a list of deltas to a list of delta directories.
   * @param deltas the list of deltas out of a Directory object.
   * @return a list of delta directory paths that need to be read
   */
  public static Path[] getPaths(List<ParsedDelta> deltas) {
    Path[] result = new Path[deltas.size()];
    for(int i=0; i < result.length; ++i) {
      result[i] = deltas.get(i).getPath();
    }
    return result;
  }

  /**
   * Convert the list of deltas into an equivalent list of begin/end
   * transaction id pairs.  Assumes {@code deltas} is sorted.
   * @param deltas
   * @return the list of transaction ids to serialize
   */
  public static List<AcidInputFormat.DeltaMetaData> serializeDeltas(List<ParsedDelta> deltas) {
    List<AcidInputFormat.DeltaMetaData> result = new ArrayList<>(deltas.size());
    AcidInputFormat.DeltaMetaData last = null;
    for(ParsedDelta parsedDelta : deltas) {
      if(last != null && last.getMinTxnId() == parsedDelta.getMinTransaction() && last.getMaxTxnId() == parsedDelta.getMaxTransaction()) {
        last.getStmtIds().add(parsedDelta.getStatementId());
        continue;
      }
      last = new AcidInputFormat.DeltaMetaData(parsedDelta.getMinTransaction(), parsedDelta.getMaxTransaction(), new ArrayList<Integer>());
      result.add(last);
      if(parsedDelta.statementId >= 0) {
        last.getStmtIds().add(parsedDelta.getStatementId());
      }
    }
    return result;
  }

  /**
   * Convert the list of begin/end transaction id pairs to a list of delta
   * directories.  Note that there may be multiple delta files for the exact same txn range starting
   * with 1.3.x;
   * see {@link org.apache.hadoop.hive.ql.io.AcidUtils#deltaSubdir(long, long, int)}
   * @param root the root directory
   * @param deltas list of begin/end transaction id pairs
   * @return the list of delta paths
   */
  public static Path[] deserializeDeltas(Path root, final List<AcidInputFormat.DeltaMetaData> deltas) throws IOException {
    List<Path> results = new ArrayList<Path>(deltas.size());
    for(AcidInputFormat.DeltaMetaData dmd : deltas) {
      if(dmd.getStmtIds().isEmpty()) {
        results.add(new Path(root, deltaSubdir(dmd.getMinTxnId(), dmd.getMaxTxnId())));
        continue;
      }
      for(Integer stmtId : dmd.getStmtIds()) {
        results.add(new Path(root, deltaSubdir(dmd.getMinTxnId(), dmd.getMaxTxnId(), stmtId)));
      }
    }
    return results.toArray(new Path[results.size()]);
  }

  private static ParsedDelta parseDelta(FileStatus path) {
    ParsedDelta p = parsedDelta(path.getPath());
    return new ParsedDelta(p.getMinTransaction(),
      p.getMaxTransaction(), path, p.statementId);
  }
  public static ParsedDelta parsedDelta(Path deltaDir) {
    String filename = deltaDir.getName();
    if (filename.startsWith(DELTA_PREFIX)) {
      String rest = filename.substring(DELTA_PREFIX.length());
      int split = rest.indexOf('_');
      int split2 = rest.indexOf('_', split + 1);//may be -1 if no statementId
      long min = Long.parseLong(rest.substring(0, split));
      long max = split2 == -1 ?
        Long.parseLong(rest.substring(split + 1)) :
        Long.parseLong(rest.substring(split + 1, split2));
      if(split2 == -1) {
        return new ParsedDelta(min, max, null);
      }
      int statementId = Integer.parseInt(rest.substring(split2 + 1));
      return new ParsedDelta(min, max, null, statementId);
    }
    throw new IllegalArgumentException(deltaDir + " does not start with " +
                                       DELTA_PREFIX);
  }

  /**
   * Is the given directory in ACID format?
   * @param directory the partition directory to check
   * @param conf the query configuration
   * @return true, if it is an ACID directory
   * @throws IOException
   */
  public static boolean isAcid(Path directory,
                               Configuration conf) throws IOException {
    FileSystem fs = directory.getFileSystem(conf);
    for(FileStatus file: fs.listStatus(directory)) {
      String filename = file.getPath().getName();
      if (filename.startsWith(BASE_PREFIX) ||
          filename.startsWith(DELTA_PREFIX)) {
        if (file.isDir()) {
          return true;
        }
      }
    }
    return false;
  }

  @VisibleForTesting
  public static Directory getAcidState(Path directory,
      Configuration conf,
      ValidTxnList txnList
      ) throws IOException {
    return getAcidState(directory, conf, txnList, false);
  }

  /** State class for getChildState; cannot modify 2 things in a method. */
  private static class TxnBase {
    private FileStatus status;
    private long txn;
  }

  /**
   * Get the ACID state of the given directory. It finds the minimal set of
   * base and diff directories. Note that because major compactions don't
   * preserve the history, we can't use a base directory that includes a
   * transaction id that we must exclude.
   * @param directory the partition directory to analyze
   * @param conf the configuration
   * @param txnList the list of transactions that we are reading
   * @return the state of the directory
   * @throws IOException
   */
  public static Directory getAcidState(Path directory,
                                       Configuration conf,
                                       ValidTxnList txnList,
                                       boolean useFileIds
                                       ) throws IOException {
    FileSystem fs = directory.getFileSystem(conf);
    final List<ParsedDelta> deltas = new ArrayList<ParsedDelta>();
    List<ParsedDelta> working = new ArrayList<ParsedDelta>();
    List<FileStatus> originalDirectories = new ArrayList<FileStatus>();
    final List<FileStatus> obsolete = new ArrayList<FileStatus>();
    List<HdfsFileStatusWithId> childrenWithId = null;
    if (useFileIds) {
      try {
        childrenWithId = SHIMS.listLocatedHdfsStatus(fs, directory, hiddenFileFilter);
      } catch (Throwable t) {
        LOG.error("Failed to get files with ID; using regular API", t);
        useFileIds = false;
      }
    }
    TxnBase bestBase = new TxnBase();
    final List<HdfsFileStatusWithId> original = new ArrayList<>();
    if (childrenWithId != null) {
      for (HdfsFileStatusWithId child : childrenWithId) {
        getChildState(child.getFileStatus(), child, txnList, working,
            originalDirectories, original, obsolete, bestBase);
      }
    } else {
      List<FileStatus> children = SHIMS.listLocatedStatus(fs, directory, hiddenFileFilter);
      for (FileStatus child : children) {
        getChildState(
            child, null, txnList, working, originalDirectories, original, obsolete, bestBase);
      }
    }

    // If we have a base, the original files are obsolete.
    if (bestBase.status != null) {
      // Add original files to obsolete list if any
      for (HdfsFileStatusWithId fswid : original) {
        obsolete.add(fswid.getFileStatus());
      }
      // Add original direcotries to obsolete list if any
      obsolete.addAll(originalDirectories);
      // remove the entries so we don't get confused later and think we should
      // use them.
      original.clear();
      originalDirectories.clear();
    } else {
      // Okay, we're going to need these originals.  Recurse through them and figure out what we
      // really need.
      for (FileStatus origDir : originalDirectories) {
        findOriginals(fs, origDir, original, useFileIds);
      }
    }

    Collections.sort(working);
    //so now, 'working' should be sorted like delta_5_20 delta_5_10 delta_11_20 delta_51_60 for example
    //and we want to end up with the best set containing all relevant data: delta_5_20 delta_51_60,
    //subject to list of 'exceptions' in 'txnList' (not show in above example).
    long current = bestBase.txn;
    int lastStmtId = -1;
    for(ParsedDelta next: working) {
      if (next.maxTransaction > current) {
        // are any of the new transactions ones that we care about?
        if (txnList.isTxnRangeValid(current+1, next.maxTransaction) !=
          ValidTxnList.RangeResponse.NONE) {
          deltas.add(next);
          current = next.maxTransaction;
          lastStmtId = next.statementId;
        }
      }
      else if(next.maxTransaction == current && lastStmtId >= 0) {
        //make sure to get all deltas within a single transaction;  multi-statement txn
        //generate multiple delta files with the same txnId range
        //of course, if maxTransaction has already been minor compacted, all per statement deltas are obsolete
        deltas.add(next);
      }
      else {
        obsolete.add(next.path);
      }
    }

    final Path base = bestBase.status == null ? null : bestBase.status.getPath();
    LOG.debug("in directory " + directory.toUri().toString() + " base = " + base + " deltas = " +
        deltas.size());

    return new Directory(){

      @Override
      public Path getBaseDirectory() {
        return base;
      }

      @Override
      public List<HdfsFileStatusWithId> getOriginalFiles() {
        return original;
      }

      @Override
      public List<ParsedDelta> getCurrentDirectories() {
        return deltas;
      }

      @Override
      public List<FileStatus> getObsolete() {
        return obsolete;
      }
    };
  }

  private static void getChildState(FileStatus child, HdfsFileStatusWithId childWithId,
      ValidTxnList txnList, List<ParsedDelta> working, List<FileStatus> originalDirectories,
      List<HdfsFileStatusWithId> original, List<FileStatus> obsolete, TxnBase bestBase) {
    Path p = child.getPath();
    String fn = p.getName();
    if (fn.startsWith(BASE_PREFIX) && child.isDir()) {
      long txn = parseBase(p);
      if (bestBase.status == null) {
        bestBase.status = child;
        bestBase.txn = txn;
      } else if (bestBase.txn < txn) {
        obsolete.add(bestBase.status);
        bestBase.status = child;
        bestBase.txn = txn;
      } else {
        obsolete.add(child);
      }
    } else if (fn.startsWith(DELTA_PREFIX) && child.isDir()) {
      ParsedDelta delta = parseDelta(child);
      if (txnList.isTxnRangeValid(delta.minTransaction,
          delta.maxTransaction) !=
          ValidTxnList.RangeResponse.NONE) {
        working.add(delta);
      }
    } else if (child.isDir()) {
      // This is just the directory.  We need to recurse and find the actual files.  But don't
      // do this until we have determined there is no base.  This saves time.  Plus,
      // it is possible that the cleaner is running and removing these original files,
      // in which case recursing through them could cause us to get an error.
      originalDirectories.add(child);
    } else {
      original.add(createOriginalObj(childWithId, child));
    }
  }

  public static HdfsFileStatusWithId createOriginalObj(
      HdfsFileStatusWithId childWithId, FileStatus child) {
    return childWithId != null ? childWithId : new HdfsFileStatusWithoutId(child);
  }

  private static class HdfsFileStatusWithoutId implements HdfsFileStatusWithId {
    private FileStatus fs;

    public HdfsFileStatusWithoutId(FileStatus fs) {
      this.fs = fs;
    }

    @Override
    public FileStatus getFileStatus() {
      return fs;
    }

    @Override
    public Long getFileId() {
      return null;
    }
  }

  /**
   * Find the original files (non-ACID layout) recursively under the partition directory.
   * @param fs the file system
   * @param stat the directory to add
   * @param original the list of original files
   * @throws IOException
   */
  private static void findOriginals(FileSystem fs, FileStatus stat,
      List<HdfsFileStatusWithId> original, boolean useFileIds) throws IOException {
    assert stat.isDir();
    List<HdfsFileStatusWithId> childrenWithId = null;
    if (useFileIds) {
      try {
        childrenWithId = SHIMS.listLocatedHdfsStatus(fs, stat.getPath(), hiddenFileFilter);
      } catch (Throwable t) {
        LOG.error("Failed to get files with ID; using regular API", t);
        useFileIds = false;
      }
    }
    if (childrenWithId != null) {
      for (HdfsFileStatusWithId child : childrenWithId) {
        if (child.getFileStatus().isDir()) {
          findOriginals(fs, child.getFileStatus(), original, useFileIds);
        } else {
          original.add(child);
        }
      }
    } else {
      List<FileStatus> children = SHIMS.listLocatedStatus(fs, stat.getPath(), hiddenFileFilter);
      for (FileStatus child : children) {
        if (child.isDir()) {
          findOriginals(fs, child, original, useFileIds);
        } else {
          original.add(createOriginalObj(null, child));
        }
      }
    }
  }

  public static boolean isTablePropertyTransactional(Properties props) {
    String resultStr = props.getProperty(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    if (resultStr == null) {
      resultStr = props.getProperty(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL.toUpperCase());
    }
    return resultStr != null && resultStr.equalsIgnoreCase("true");
  }

  public static boolean isTablePropertyTransactional(Map<String, String> parameters) {
    String resultStr = parameters.get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    if (resultStr == null) {
      resultStr = parameters.get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL.toUpperCase());
    }
    return resultStr != null && resultStr.equalsIgnoreCase("true");
  }
}
