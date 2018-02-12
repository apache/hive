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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.TransactionalValidationListener;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcRecordUpdater;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileStatusWithId;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.common.util.Ref;
import org.apache.orc.FileFormatException;
import org.apache.orc.impl.OrcAcidUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.hadoop.hive.ql.exec.Utilities.COPY_KEYWORD;

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
  public static final String DELETE_DELTA_PREFIX = "delete_delta_";
  /**
   * Acid Streaming Ingest writes multiple transactions to the same file.  It also maintains a
   * {@link org.apache.orc.impl.OrcAcidUtils#getSideFile(Path)} side file which stores the length of
   * the primary file as of the last commit ({@link OrcRecordUpdater#flush()}).  That is the 'logical length'.
   * Once the primary is closed, the side file is deleted (logical length = actual length) but if
   * the writer dies or the primary file is being read while its still being written to, anything
   * past the logical length should be ignored.
   *
   * @see org.apache.orc.impl.OrcAcidUtils#DELTA_SIDE_FILE_SUFFIX
   * @see org.apache.orc.impl.OrcAcidUtils#getLastFlushLength(FileSystem, Path)
   * @see #getLogicalLength(FileSystem, FileStatus)
   */
  public static final String DELTA_SIDE_FILE_SUFFIX = "_flush_length";
  public static final PathFilter deltaFileFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return path.getName().startsWith(DELTA_PREFIX);
    }
  };
  public static final PathFilter deleteEventDeltaDirFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return path.getName().startsWith(DELETE_DELTA_PREFIX);
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
  /**
   * A write into a non-aicd table produces files like 0000_0 or 0000_0_copy_1
   * (Unless via Load Data statement)
   */
  public static final PathFilter originalBucketFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return ORIGINAL_PATTERN.matcher(path.getName()).matches() ||
        ORIGINAL_PATTERN_COPY.matcher(path.getName()).matches();
    }
  };

  private AcidUtils() {
    // NOT USED
  }
  private static final Logger LOG = LoggerFactory.getLogger(AcidUtils.class);

  public static final Pattern BUCKET_PATTERN = Pattern.compile(BUCKET_PREFIX + "_[0-9]{5}$");
  public static final Pattern ORIGINAL_PATTERN =
      Pattern.compile("[0-9]+_[0-9]+");
  /**
   * @see org.apache.hadoop.hive.ql.exec.Utilities#COPY_KEYWORD
   */
  public static final Pattern ORIGINAL_PATTERN_COPY =
    Pattern.compile("[0-9]+_[0-9]+" + COPY_KEYWORD + "[0-9]+");

  public static final PathFilter hiddenFileFilter = new PathFilter(){
    @Override
    public boolean accept(Path p){
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  private static final HadoopShims SHIMS = ShimLoader.getHadoopShims();

  /**
   * Create the bucket filename in Acid format
   * @param subdir the subdirectory for the bucket.
   * @param bucket the bucket number
   * @return the filename
   */
  public static Path createBucketFile(Path subdir, int bucket) {
    return createBucketFile(subdir, bucket, true);
  }

  /**
   * Create acid or original bucket name
   * @param subdir the subdirectory for the bucket.
   * @param bucket the bucket number
   * @return the filename
   */
  private static Path createBucketFile(Path subdir, int bucket, boolean isAcidSchema) {
    if(isAcidSchema) {
      return new Path(subdir,
        BUCKET_PREFIX + String.format(BUCKET_DIGITS, bucket));
    }
    else {
      return new Path(subdir,
        String.format(BUCKET_DIGITS, bucket));
    }
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

  /**
   * This is format of delete delta dir name prior to Hive 2.2.x
   */
  @VisibleForTesting
  public static String deleteDeltaSubdir(long min, long max) {
    return DELETE_DELTA_PREFIX + String.format(DELTA_DIGITS, min) + "_" +
        String.format(DELTA_DIGITS, max);
  }

  /**
   * Each write statement in a transaction creates its own delete delta dir,
   * when split-update acid operational property is turned on.
   * @since 2.2.x
   */
  @VisibleForTesting
  public static String deleteDeltaSubdir(long min, long max, int statementId) {
    return deleteDeltaSubdir(min, max) + "_" + String.format(STATEMENT_DIGITS, statementId);
  }

  public static String baseDir(long txnId) {
    return BASE_PREFIX + String.format(DELTA_DIGITS, txnId);
  }

  /**
   * Return a base or delta directory string
   * according to the given "baseDirRequired".
   */
  public static String baseOrDeltaSubdir(boolean baseDirRequired, long min, long max, int statementId) {
    if (!baseDirRequired) {
       return deltaSubdir(min, max, statementId);
    } else {
       return baseDir(min);
    }
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
          options.getBucketId()) + "_0");
    } else if (options.isWritingBase()) {
      subdir = BASE_PREFIX + String.format(DELTA_DIGITS,
          options.getMaximumTransactionId());
    } else if(options.getStatementId() == -1) {
      //when minor compaction runs, we collapse per statement delta files inside a single
      //transaction so we no longer need a statementId in the file name
      subdir = options.isWritingDeleteDelta() ?
          deleteDeltaSubdir(options.getMinimumTransactionId(),
                            options.getMaximumTransactionId())
          : deltaSubdir(options.getMinimumTransactionId(),
                        options.getMaximumTransactionId());
    } else {
      subdir = options.isWritingDeleteDelta() ?
          deleteDeltaSubdir(options.getMinimumTransactionId(),
                            options.getMaximumTransactionId(),
                            options.getStatementId())
          : deltaSubdir(options.getMinimumTransactionId(),
                        options.getMaximumTransactionId(),
                        options.getStatementId());
    }
    return createBucketFile(new Path(directory, subdir), options.getBucketId());
  }

  /**
   * Get the transaction id from a base directory name.
   * @param path the base directory name
   * @return the maximum transaction id that is included
   */
  public static long parseBase(Path path) {
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
                    parseBaseOrDeltaBucketFilename(Path bucketFile,
                                                   Configuration conf) throws IOException {
    AcidOutputFormat.Options result = new AcidOutputFormat.Options(conf);
    String filename = bucketFile.getName();
    if (ORIGINAL_PATTERN.matcher(filename).matches()) {
      int bucket =
          Integer.parseInt(filename.substring(0, filename.indexOf('_')));
      result
          .setOldStyle(true)
          .minimumTransactionId(0)
          .maximumTransactionId(0)
          .bucket(bucket)
          .writingBase(!bucketFile.getParent().getName().startsWith(DELTA_PREFIX));
    }
    else if(ORIGINAL_PATTERN_COPY.matcher(filename).matches()) {
      //todo: define groups in regex and use parseInt(Matcher.group(2))....
      int bucket =
        Integer.parseInt(filename.substring(0, filename.indexOf('_')));
      int copyNumber = Integer.parseInt(filename.substring(filename.lastIndexOf('_') + 1));
      result
        .setOldStyle(true)
        .minimumTransactionId(0)
        .maximumTransactionId(0)
        .bucket(bucket)
        .copyNumber(copyNumber)
        .writingBase(!bucketFile.getParent().getName().startsWith(DELTA_PREFIX));
    }
    else if (filename.startsWith(BUCKET_PREFIX)) {
      int bucket =
          Integer.parseInt(filename.substring(filename.indexOf('_') + 1));
      if (bucketFile.getParent().getName().startsWith(BASE_PREFIX)) {
        result
            .setOldStyle(false)
            .minimumTransactionId(0)
            .maximumTransactionId(parseBase(bucketFile.getParent()))
            .bucket(bucket)
            .writingBase(true);
      } else if (bucketFile.getParent().getName().startsWith(DELTA_PREFIX)) {
        ParsedDelta parsedDelta = parsedDelta(bucketFile.getParent(), DELTA_PREFIX,
          bucketFile.getFileSystem(conf));
        result
            .setOldStyle(false)
            .minimumTransactionId(parsedDelta.minTransaction)
            .maximumTransactionId(parsedDelta.maxTransaction)
            .bucket(bucket);
      } else if (bucketFile.getParent().getName().startsWith(DELETE_DELTA_PREFIX)) {
        ParsedDelta parsedDelta = parsedDelta(bucketFile.getParent(), DELETE_DELTA_PREFIX,
          bucketFile.getFileSystem(conf));
        result
            .setOldStyle(false)
            .minimumTransactionId(parsedDelta.minTransaction)
            .maximumTransactionId(parsedDelta.maxTransaction)
            .bucket(bucket);
      }
    } else {
      result.setOldStyle(true).bucket(-1).minimumTransactionId(0)
          .maximumTransactionId(0);
    }
    return result;
  }
  //This is used for (full) Acid tables.  InsertOnly use NOT_ACID
  public enum Operation implements Serializable {
    NOT_ACID, INSERT, UPDATE, DELETE;
  }

  /**
   * Logically this should have been defined in Operation but that causes a dependency
   * on metastore package from exec jar (from the cluster) which is not allowed.
   * This method should only be called from client side where metastore.* classes are present.
   * Not following this will not be caught by unit tests since they have all the jar loaded.
   */
  public static DataOperationType toDataOperationType(Operation op) {
    switch (op) {
      case NOT_ACID:
        return DataOperationType.UNSET;
      case INSERT:
        return DataOperationType.INSERT;
      case UPDATE:
        return DataOperationType.UPDATE;
      case DELETE:
        return DataOperationType.DELETE;
      default:
        throw new IllegalArgumentException("Unexpected Operation: " + op);
    }
  }
  public enum AcidBaseFileType {
  /**
   * File w/o Acid meta columns.  This this would be the case for files that were added to the table
   * before it was converted to Acid but not yet major compacted.  May also be the the result of
   * Load Data statement on an acid table.
   */
  ORIGINAL_BASE,
  /**
   * File that has Acid metadata columns embedded in it.  Found in base_x/ or delta_x_y/.
   */
  ACID_SCHEMA,
  }

  /**
   * A simple wrapper class that stores the information about a base file and its type.
   * Orc splits can be generated on three kinds of base files: an original file (non-acid converted
   * files), a regular base file (created by major compaction) or an insert delta (which can be
   * treated as a base when split-update is enabled for acid).
   */
  public static class AcidBaseFileInfo {
    final private HdfsFileStatusWithId fileId;
    final private AcidBaseFileType acidBaseFileType;

    public AcidBaseFileInfo(HdfsFileStatusWithId fileId, AcidBaseFileType acidBaseFileType) {
      this.fileId = fileId;
      this.acidBaseFileType = acidBaseFileType;
    }

    public boolean isOriginal() {
      return this.acidBaseFileType == AcidBaseFileType.ORIGINAL_BASE;
    }

    public boolean isAcidSchema() {
      return this.acidBaseFileType == AcidBaseFileType.ACID_SCHEMA;
    }

    public HdfsFileStatusWithId getHdfsFileStatusWithId() {
      return this.fileId;
    }
  }

  /**
   * Current syntax for creating full acid transactional tables is any one of following 3 ways:
   * create table T (a int, b int) stored as orc tblproperties('transactional'='true').
   * create table T (a int, b int) stored as orc tblproperties('transactional'='true',
   * 'transactional_properties'='default').
   * create table T (a int, b int) stored as orc tblproperties('transactional'='true',
   * 'transactional_properties'='split_update').
   * These are all identical and create a table capable of insert/update/delete/merge operations
   * with full ACID semantics at Snapshot Isolation.  These tables require ORC input/output format.
   *
   * To create a 1/4 acid, aka Micro Managed table:
   * create table T (a int, b int) stored as orc tblproperties('transactional'='true',
   * 'transactional_properties'='insert_only').
   * These tables only support insert operation (also with full ACID semantics at SI).
   *
   */
  public static class AcidOperationalProperties {
    private int description = 0x00;
    public static final int SPLIT_UPDATE_BIT = 0x01;
    public static final String SPLIT_UPDATE_STRING = "split_update";
    public static final int HASH_BASED_MERGE_BIT = 0x02;
    public static final String HASH_BASED_MERGE_STRING = "hash_merge";
    public static final int INSERT_ONLY_BIT = 0x04;
    public static final String INSERT_ONLY_STRING = "insert_only";
    public static final String DEFAULT_VALUE_STRING = TransactionalValidationListener.DEFAULT_TRANSACTIONAL_PROPERTY;
    public static final String INSERTONLY_VALUE_STRING = TransactionalValidationListener.INSERTONLY_TRANSACTIONAL_PROPERTY;

    private AcidOperationalProperties() {
    }


    /**
     * Returns an acidOperationalProperties object that represents default ACID behavior for tables
     * that do no explicitly specify/override the default behavior.
     * @return the acidOperationalProperties object.
     */
    public static AcidOperationalProperties getDefault() {
      AcidOperationalProperties obj = new AcidOperationalProperties();
      obj.setSplitUpdate(true);
      obj.setHashBasedMerge(false);
      obj.setInsertOnly(false);
      return obj;
    }

    /**
     * Returns an acidOperationalProperties object for tables that uses ACID framework but only
     * supports INSERT operation and does not require ORC or bucketing
     * @return the acidOperationalProperties object
     */
    public static AcidOperationalProperties getInsertOnly() {
      AcidOperationalProperties obj = new AcidOperationalProperties();
      obj.setInsertOnly(true);
      return obj;
    }

    /**
     * Returns an acidOperationalProperties object that is represented by an encoded string.
     * @param propertiesStr an encoded string representing the acidOperationalProperties.
     * @return the acidOperationalProperties object.
     */
    public static AcidOperationalProperties parseString(String propertiesStr) {
      if (propertiesStr == null) {
        return AcidOperationalProperties.getDefault();
      }
      if (propertiesStr.equalsIgnoreCase(DEFAULT_VALUE_STRING)) {
        return AcidOperationalProperties.getDefault();
      }
      if (propertiesStr.equalsIgnoreCase(INSERTONLY_VALUE_STRING)) {
        return AcidOperationalProperties.getInsertOnly();
      }
      AcidOperationalProperties obj = new AcidOperationalProperties();
      String[] options = propertiesStr.split("\\|");
      for (String option : options) {
        if (option.trim().length() == 0) continue; // ignore empty strings
        switch (option) {
          case SPLIT_UPDATE_STRING:
            obj.setSplitUpdate(true);
            break;
          case HASH_BASED_MERGE_STRING:
            obj.setHashBasedMerge(true);
            break;
          default:
            throw new IllegalArgumentException(
                "Unexpected value " + option + " for ACID operational properties!");
        }
      }
      return obj;
    }

    /**
     * Returns an acidOperationalProperties object that is represented by an encoded 32-bit integer.
     * @param properties an encoded 32-bit representing the acidOperationalProperties.
     * @return the acidOperationalProperties object.
     */
    public static AcidOperationalProperties parseInt(int properties) {
      AcidOperationalProperties obj = new AcidOperationalProperties();
      if ((properties & SPLIT_UPDATE_BIT)  > 0) {
        obj.setSplitUpdate(true);
      }
      if ((properties & HASH_BASED_MERGE_BIT)  > 0) {
        obj.setHashBasedMerge(true);
      }
      if ((properties & INSERT_ONLY_BIT) > 0) {
        obj.setInsertOnly(true);
      }
      return obj;
    }

    /**
     * Sets the split update property for ACID operations based on the boolean argument.
     * When split update is turned on, an update ACID event is interpreted as a combination of
     * delete event followed by an update event.
     * @param isSplitUpdate a boolean property that turns on split update when true.
     * @return the acidOperationalProperties object.
     */
    public AcidOperationalProperties setSplitUpdate(boolean isSplitUpdate) {
      description = (isSplitUpdate
              ? (description | SPLIT_UPDATE_BIT) : (description & ~SPLIT_UPDATE_BIT));
      return this;
    }

    /**
     * Sets the hash-based merge property for ACID operations that combines delta files using
     * GRACE hash join based approach, when turned on. (Currently unimplemented!)
     * @param isHashBasedMerge a boolean property that turns on hash-based merge when true.
     * @return the acidOperationalProperties object.
     */
    public AcidOperationalProperties setHashBasedMerge(boolean isHashBasedMerge) {
      description = (isHashBasedMerge
              ? (description | HASH_BASED_MERGE_BIT) : (description & ~HASH_BASED_MERGE_BIT));
      return this;
    }

    public AcidOperationalProperties setInsertOnly(boolean isInsertOnly) {
      description = (isInsertOnly
              ? (description | INSERT_ONLY_BIT) : (description & ~INSERT_ONLY_BIT));
      return this;
    }

    public boolean isSplitUpdate() {
      return (description & SPLIT_UPDATE_BIT) > 0;
    }

    public boolean isHashBasedMerge() {
      return (description & HASH_BASED_MERGE_BIT) > 0;
    }

    public boolean isInsertOnly() {
      return (description & INSERT_ONLY_BIT) > 0;
    }

    public int toInt() {
      return description;
    }

    @Override
    public String toString() {
      StringBuilder str = new StringBuilder();
      if (isSplitUpdate()) {
        str.append("|" + SPLIT_UPDATE_STRING);
      }
      if (isHashBasedMerge()) {
        str.append("|" + HASH_BASED_MERGE_STRING);
      }
      if (isInsertOnly()) {
        str.append("|" + INSERT_ONLY_STRING);
      }
      return str.toString();
    }
  }

  public static interface Directory {

    /**
     * Get the base directory.
     * @return the base directory to read
     */
    Path getBaseDirectory();
    boolean isBaseInRawFormat();

    /**
     * Get the list of original files.  Not {@code null}.  Must be sorted.
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

    /**
     * Get the list of directories that has nothing but aborted transactions.
     * @return the list of aborted directories
     */
    List<FileStatus> getAbortedDirectories();
  }

  /**
   * Immutable
   */
  public static final class ParsedDelta implements Comparable<ParsedDelta> {
    private final long minTransaction;
    private final long maxTransaction;
    private final FileStatus path;
    //-1 is for internal (getAcidState()) purposes and means the delta dir
    //had no statement ID
    private final int statementId;
    private final boolean isDeleteDelta; // records whether delta dir is of type 'delete_delta_x_y...'
    private final boolean isRawFormat;

    /**
     * for pre 1.3.x delta files
     */
    private ParsedDelta(long min, long max, FileStatus path, boolean isDeleteDelta,
        boolean isRawFormat) {
      this(min, max, path, -1, isDeleteDelta, isRawFormat);
    }
    private ParsedDelta(long min, long max, FileStatus path, int statementId,
        boolean isDeleteDelta, boolean isRawFormat) {
      this.minTransaction = min;
      this.maxTransaction = max;
      this.path = path;
      this.statementId = statementId;
      this.isDeleteDelta = isDeleteDelta;
      this.isRawFormat = isRawFormat;
      assert !isDeleteDelta || !isRawFormat : " deleteDelta should not be raw format";
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

    public boolean isDeleteDelta() {
      return isDeleteDelta;
    }
    /**
     * Files w/o Acid meta columns embedded in the file. See {@link AcidBaseFileType#ORIGINAL_BASE}
     */
    public boolean isRawFormat() {
      return isRawFormat;
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
   * Convert the list of begin/end transaction id pairs to a list of delete delta
   * directories.  Note that there may be multiple delete_delta files for the exact same txn range starting
   * with 2.2.x;
   * see {@link org.apache.hadoop.hive.ql.io.AcidUtils#deltaSubdir(long, long, int)}
   * @param root the root directory
   * @param deleteDeltas list of begin/end transaction id pairs
   * @return the list of delta paths
   */
  public static Path[] deserializeDeleteDeltas(Path root, final List<AcidInputFormat.DeltaMetaData> deleteDeltas) throws IOException {
    List<Path> results = new ArrayList<Path>(deleteDeltas.size());
    for(AcidInputFormat.DeltaMetaData dmd : deleteDeltas) {
      if(dmd.getStmtIds().isEmpty()) {
        results.add(new Path(root, deleteDeltaSubdir(dmd.getMinTxnId(), dmd.getMaxTxnId())));
        continue;
      }
      for(Integer stmtId : dmd.getStmtIds()) {
        results.add(new Path(root, deleteDeltaSubdir(dmd.getMinTxnId(), dmd.getMaxTxnId(), stmtId)));
      }
    }
    return results.toArray(new Path[results.size()]);
  }

  public static ParsedDelta parsedDelta(Path deltaDir, FileSystem fs) throws IOException {
    String deltaDirName = deltaDir.getName();
    if (deltaDirName.startsWith(DELETE_DELTA_PREFIX)) {
      return parsedDelta(deltaDir, DELETE_DELTA_PREFIX, fs);
    }
    return parsedDelta(deltaDir, DELTA_PREFIX, fs); // default prefix is delta_prefix
  }

  private static ParsedDelta parseDelta(FileStatus path, String deltaPrefix, FileSystem fs)
    throws IOException {
    ParsedDelta p = parsedDelta(path.getPath(), deltaPrefix, fs);
    boolean isDeleteDelta = deltaPrefix.equals(DELETE_DELTA_PREFIX);
    return new ParsedDelta(p.getMinTransaction(),
        p.getMaxTransaction(), path, p.statementId, isDeleteDelta, p.isRawFormat());
  }

  public static ParsedDelta parsedDelta(Path deltaDir, String deltaPrefix, FileSystem fs)
    throws IOException {
    String filename = deltaDir.getName();
    boolean isDeleteDelta = deltaPrefix.equals(DELETE_DELTA_PREFIX);
    if (filename.startsWith(deltaPrefix)) {
      //small optimization - delete delta can't be in raw format
      boolean isRawFormat = !isDeleteDelta && MetaDataFile.isRawFormat(deltaDir, fs);
      String rest = filename.substring(deltaPrefix.length());
      int split = rest.indexOf('_');
      int split2 = rest.indexOf('_', split + 1);//may be -1 if no statementId
      long min = Long.parseLong(rest.substring(0, split));
      long max = split2 == -1 ?
        Long.parseLong(rest.substring(split + 1)) :
        Long.parseLong(rest.substring(split + 1, split2));
      if(split2 == -1) {
        return new ParsedDelta(min, max, null, isDeleteDelta, isRawFormat);
      }
      int statementId = Integer.parseInt(rest.substring(split2 + 1));
      return new ParsedDelta(min, max, null, statementId, isDeleteDelta, isRawFormat);
    }
    throw new IllegalArgumentException(deltaDir + " does not start with " +
                                       deltaPrefix);
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
          filename.startsWith(DELTA_PREFIX) ||
          filename.startsWith(DELETE_DELTA_PREFIX)) {
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
    return getAcidState(directory, conf, txnList, false, false);
  }

  /** State class for getChildState; cannot modify 2 things in a method. */
  private static class TxnBase {
    private FileStatus status;
    private long txn = 0;
    private long oldestBaseTxnId = Long.MAX_VALUE;
    private Path oldestBase = null;
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
                                       boolean useFileIds,
                                       boolean ignoreEmptyFiles
                                       ) throws IOException {
    return getAcidState(directory, conf, txnList, Ref.from(useFileIds), ignoreEmptyFiles, null);
  }

  public static Directory getAcidState(Path directory,
                                       Configuration conf,
                                       ValidTxnList txnList,
                                       Ref<Boolean> useFileIds,
                                       boolean ignoreEmptyFiles,
                                       Map<String, String> tblproperties) throws IOException {
    FileSystem fs = directory.getFileSystem(conf);
    // The following 'deltas' includes all kinds of delta files including insert & delete deltas.
    final List<ParsedDelta> deltas = new ArrayList<ParsedDelta>();
    List<ParsedDelta> working = new ArrayList<ParsedDelta>();
    List<FileStatus> originalDirectories = new ArrayList<FileStatus>();
    final List<FileStatus> obsolete = new ArrayList<FileStatus>();
    final List<FileStatus> abortedDirectories = new ArrayList<>();
    List<HdfsFileStatusWithId> childrenWithId = null;
    Boolean val = useFileIds.value;
    if (val == null || val) {
      try {
        childrenWithId = SHIMS.listLocatedHdfsStatus(fs, directory, hiddenFileFilter);
        if (val == null) {
          useFileIds.value = true;
        }
      } catch (Throwable t) {
        LOG.error("Failed to get files with ID; using regular API: " + t.getMessage());
        if (val == null && t instanceof UnsupportedOperationException) {
          useFileIds.value = false;
        }
      }
    }
    TxnBase bestBase = new TxnBase();
    final List<HdfsFileStatusWithId> original = new ArrayList<>();
    if (childrenWithId != null) {
      for (HdfsFileStatusWithId child : childrenWithId) {
        getChildState(child.getFileStatus(), child, txnList, working, originalDirectories, original,
            obsolete, bestBase, ignoreEmptyFiles, abortedDirectories, tblproperties, fs);
      }
    } else {
      List<FileStatus> children = HdfsUtils.listLocatedStatus(fs, directory, hiddenFileFilter);
      for (FileStatus child : children) {
        getChildState(child, null, txnList, working, originalDirectories, original, obsolete,
            bestBase, ignoreEmptyFiles, abortedDirectories, tblproperties, fs);
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
        findOriginals(fs, origDir, original, useFileIds, ignoreEmptyFiles);
      }
    }

    Collections.sort(working);
    //so now, 'working' should be sorted like delta_5_20 delta_5_10 delta_11_20 delta_51_60 for example
    //and we want to end up with the best set containing all relevant data: delta_5_20 delta_51_60,
    //subject to list of 'exceptions' in 'txnList' (not show in above example).
    long current = bestBase.txn;
    int lastStmtId = -1;
    ParsedDelta prev = null;
    for(ParsedDelta next: working) {
      if (next.maxTransaction > current) {
        // are any of the new transactions ones that we care about?
        if (txnList.isTxnRangeValid(current+1, next.maxTransaction) !=
          ValidTxnList.RangeResponse.NONE) {
          deltas.add(next);
          current = next.maxTransaction;
          lastStmtId = next.statementId;
          prev = next;
        }
      }
      else if(next.maxTransaction == current && lastStmtId >= 0) {
        //make sure to get all deltas within a single transaction;  multi-statement txn
        //generate multiple delta files with the same txnId range
        //of course, if maxTransaction has already been minor compacted, all per statement deltas are obsolete
        deltas.add(next);
        prev = next;
      }
      else if (prev != null && next.maxTransaction == prev.maxTransaction
                  && next.minTransaction == prev.minTransaction
                  && next.statementId == prev.statementId) {
        // The 'next' parsedDelta may have everything equal to the 'prev' parsedDelta, except
        // the path. This may happen when we have split update and we have two types of delta
        // directories- 'delta_x_y' and 'delete_delta_x_y' for the SAME txn range.

        // Also note that any delete_deltas in between a given delta_x_y range would be made
        // obsolete. For example, a delta_30_50 would make delete_delta_40_40 obsolete.
        // This is valid because minor compaction always compacts the normal deltas and the delete
        // deltas for the same range. That is, if we had 3 directories, delta_30_30,
        // delete_delta_40_40 and delta_50_50, then running minor compaction would produce
        // delta_30_50 and delete_delta_30_50.

        deltas.add(next);
        prev = next;
      }
      else {
        obsolete.add(next.path);
      }
    }

    if(bestBase.oldestBase != null && bestBase.status == null) {
      /**
       * If here, it means there was a base_x (> 1 perhaps) but none were suitable for given
       * {@link txnList}.  Note that 'original' files are logically a base_Long.MIN_VALUE and thus
       * cannot have any data for an open txn.  We could check {@link deltas} has files to cover
       * [1,n] w/o gaps but this would almost never happen...*/
      long[] exceptions = txnList.getInvalidTransactions();
      String minOpenTxn = exceptions != null && exceptions.length > 0 ?
        Long.toString(exceptions[0]) : "x";
      throw new IOException(ErrorMsg.ACID_NOT_ENOUGH_HISTORY.format(
        Long.toString(txnList.getHighWatermark()),
        minOpenTxn, bestBase.oldestBase.toString()));
    }

    final Path base = bestBase.status == null ? null : bestBase.status.getPath();
    LOG.debug("in directory " + directory.toUri().toString() + " base = " + base + " deltas = " +
        deltas.size());
    /**
     * If this sort order is changed and there are tables that have been converted to transactional
     * and have had any update/delete/merge operations performed but not yet MAJOR compacted, it
     * may result in data loss since it may change how
     * {@link org.apache.hadoop.hive.ql.io.orc.OrcRawRecordMerger.OriginalReaderPair} assigns 
     * {@link RecordIdentifier#rowId} for read (that have happened) and compaction (yet to happen).
     */
    Collections.sort(original, (HdfsFileStatusWithId o1, HdfsFileStatusWithId o2) -> {
      //this does "Path.uri.compareTo(that.uri)"
      return o1.getFileStatus().compareTo(o2.getFileStatus());
    });

    final boolean isBaseInRawFormat = base != null && MetaDataFile.isRawFormat(base, fs);
    return new Directory() {

      @Override
      public Path getBaseDirectory() {
        return base;
      }
      @Override
      public boolean isBaseInRawFormat() {
        return isBaseInRawFormat;
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

      @Override
      public List<FileStatus> getAbortedDirectories() {
        return abortedDirectories;
      }
    };
  }
  /**
   * We can only use a 'base' if it doesn't have an open txn (from specific reader's point of view)
   * A 'base' with open txn in its range doesn't have 'enough history' info to produce a correct
   * snapshot for this reader.
   * Note that such base is NOT obsolete.  Obsolete files are those that are "covered" by other
   * files within the snapshot.
   * A base produced by Insert Overwrite is different.  Logically it's a delta file but one that
   * causes anything written previously is ignored (hence the overwrite).  In this case, base_x
   * is visible if txnid:x is committed for current reader.
   */
  private static boolean isValidBase(long baseTxnId, ValidTxnList txnList, Path baseDir,
      FileSystem fs) throws IOException {
    if(baseTxnId == Long.MIN_VALUE) {
      //such base is created by 1st compaction in case of non-acid to acid table conversion
      //By definition there are no open txns with id < 1.
      return true;
    }
    if(!MetaDataFile.isCompacted(baseDir, fs)) {
      //this is the IOW case
      return txnList.isTxnValid(baseTxnId);
    }
    return txnList.isValidBase(baseTxnId);
  }
  private static void getChildState(FileStatus child, HdfsFileStatusWithId childWithId,
      ValidTxnList txnList, List<ParsedDelta> working, List<FileStatus> originalDirectories,
      List<HdfsFileStatusWithId> original, List<FileStatus> obsolete, TxnBase bestBase,
      boolean ignoreEmptyFiles, List<FileStatus> aborted, Map<String, String> tblproperties,
      FileSystem fs) throws IOException {
    Path p = child.getPath();
    String fn = p.getName();
    if (fn.startsWith(BASE_PREFIX) && child.isDir()) {
      long txn = parseBase(p);
      if(bestBase.oldestBaseTxnId > txn) {
        //keep track for error reporting
        bestBase.oldestBase = p;
        bestBase.oldestBaseTxnId = txn;
      }
      if (bestBase.status == null) {
        if(isValidBase(txn, txnList, p, fs)) {
          bestBase.status = child;
          bestBase.txn = txn;
        }
      } else if (bestBase.txn < txn) {
        if(isValidBase(txn, txnList, p, fs)) {
          obsolete.add(bestBase.status);
          bestBase.status = child;
          bestBase.txn = txn;
        }
      } else {
        obsolete.add(child);
      }
    } else if ((fn.startsWith(DELTA_PREFIX) || fn.startsWith(DELETE_DELTA_PREFIX))
                    && child.isDir()) {
      String deltaPrefix =
              (fn.startsWith(DELTA_PREFIX)) ? DELTA_PREFIX : DELETE_DELTA_PREFIX;
      ParsedDelta delta = parseDelta(child, deltaPrefix, fs);
      if (tblproperties != null && AcidUtils.isInsertOnlyTable(tblproperties) &&
          ValidTxnList.RangeResponse.ALL == txnList.isTxnRangeAborted(delta.minTransaction, delta.maxTransaction)) {
        aborted.add(child);
      }
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
    } else if (!ignoreEmptyFiles || child.getLen() != 0){
      original.add(createOriginalObj(childWithId, child));
    }
  }

  public static HdfsFileStatusWithId createOriginalObj(
      HdfsFileStatusWithId childWithId, FileStatus child) {
    return childWithId != null ? childWithId : new HdfsFileStatusWithoutId(child);
  }

  private static class HdfsFileStatusWithoutId implements HdfsFileStatusWithId {
    private final FileStatus fs;

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
      List<HdfsFileStatusWithId> original, Ref<Boolean> useFileIds, boolean ignoreEmptyFiles) throws IOException {
    assert stat.isDir();
    List<HdfsFileStatusWithId> childrenWithId = null;
    Boolean val = useFileIds.value;
    if (val == null || val) {
      try {
        childrenWithId = SHIMS.listLocatedHdfsStatus(fs, stat.getPath(), hiddenFileFilter);
        if (val == null) {
          useFileIds.value = true;
        }
      } catch (Throwable t) {
        LOG.error("Failed to get files with ID; using regular API: " + t.getMessage());
        if (val == null && t instanceof UnsupportedOperationException) {
          useFileIds.value = false;
        }
      }
    }
    if (childrenWithId != null) {
      for (HdfsFileStatusWithId child : childrenWithId) {
        if (child.getFileStatus().isDir()) {
          findOriginals(fs, child.getFileStatus(), original, useFileIds, ignoreEmptyFiles);
        } else {
          if(!ignoreEmptyFiles || child.getFileStatus().getLen() > 0) {
            original.add(child);
          }
        }
      }
    } else {
      List<FileStatus> children = HdfsUtils.listLocatedStatus(fs, stat.getPath(), hiddenFileFilter);
      for (FileStatus child : children) {
        if (child.isDir()) {
          findOriginals(fs, child, original, useFileIds, ignoreEmptyFiles);
        } else {
          if(!ignoreEmptyFiles || child.getLen() > 0) {
            original.add(createOriginalObj(null, child));
          }
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

  public static boolean isTablePropertyTransactional(Configuration conf) {
    String resultStr = conf.get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    if (resultStr == null) {
      resultStr = conf.get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL.toUpperCase());
    }
    return resultStr != null && resultStr.equalsIgnoreCase("true");
  }


  /**
   * @param p - not null
   */
  public static boolean isDeleteDelta(Path p) {
    return p.getName().startsWith(DELETE_DELTA_PREFIX);
  }

  public static boolean isTransactionalTable(CreateTableDesc table) {
    if (table == null || table.getTblProps() == null) {
      return false;
    }
    String tableIsTransactional = table.getTblProps().get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    if (tableIsTransactional == null) {
      tableIsTransactional = table.getTblProps().get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL.toUpperCase());
    }
    return tableIsTransactional != null && tableIsTransactional.equalsIgnoreCase("true");
  }

  /**
   * Should produce the same result as
   * {@link org.apache.hadoop.hive.metastore.txn.TxnUtils#isAcidTable(org.apache.hadoop.hive.metastore.api.Table)}
   */
  public static boolean isFullAcidTable(Table table) {
    return isFullAcidTable(table == null ? null : table.getTTable());
  }

  public static boolean isTransactionalTable(Table table) {
    return isTransactionalTable(table == null ? null : table.getTTable());
  }

  /**
   * Should produce the same result as
   * {@link org.apache.hadoop.hive.metastore.txn.TxnUtils#isAcidTable(org.apache.hadoop.hive.metastore.api.Table)}
   */
  public static boolean isFullAcidTable(org.apache.hadoop.hive.metastore.api.Table table) {
    return isTransactionalTable(table) &&
        !isInsertOnlyTable(table.getParameters());
  }

  public static boolean isTransactionalTable(org.apache.hadoop.hive.metastore.api.Table table) {
    return table != null && table.getParameters() != null &&
        isTablePropertyTransactional(table.getParameters());
  }

  public static boolean isFullAcidTable(CreateTableDesc td) {
    if (td == null || td.getTblProps() == null) {
      return false;
    }
    String tableIsTransactional = td.getTblProps().get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    if (tableIsTransactional == null) {
      tableIsTransactional = td.getTblProps().get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL.toUpperCase());
    }
    return tableIsTransactional != null && tableIsTransactional.equalsIgnoreCase("true") &&
      !AcidUtils.isInsertOnlyTable(td.getTblProps());
  }

  public static boolean isFullAcidScan(Configuration conf) {
    if (!HiveConf.getBoolVar(conf, ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN)) return false;
    int propInt = conf.getInt(ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname, -1);
    if (propInt == -1) return true;
    AcidOperationalProperties props = AcidOperationalProperties.parseInt(propInt);
    return !props.isInsertOnly();
  }

  /**
   * Sets the acidOperationalProperties in the configuration object argument.
   * @param conf Mutable configuration object
   * @param properties An acidOperationalProperties object to initialize from. If this is null,
   *                   we assume this is a full transactional table.
   */
  public static void setAcidOperationalProperties(
      Configuration conf, boolean isTxnTable, AcidOperationalProperties properties) {
    if (isTxnTable) {
      HiveConf.setBoolVar(conf, ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN, isTxnTable);
      if (properties != null) {
        HiveConf.setIntVar(conf, ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES, properties.toInt());
      }
    } else {
      conf.unset(ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN.varname);
      conf.unset(ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname);
    }
  }

  /**
   * Sets the acidOperationalProperties in the map object argument.
   * @param parameters Mutable map object
   * @param properties An acidOperationalProperties object to initialize from.
   */
  public static void setAcidOperationalProperties(Map<String, String> parameters,
      boolean isTxnTable, AcidOperationalProperties properties) {
    parameters.put(ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN.varname, Boolean.toString(isTxnTable));
    if (properties != null) {
      parameters.put(ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname, properties.toString());
    }
  }

  /**
   * Returns the acidOperationalProperties for a given table.
   * @param table A table object
   * @return the acidOperationalProperties object for the corresponding table.
   */
  public static AcidOperationalProperties getAcidOperationalProperties(Table table) {
    String transactionalProperties = table.getProperty(
            hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES);
    if (transactionalProperties == null) {
      // If the table does not define any transactional properties, we return a default type.
      return AcidOperationalProperties.getDefault();
    }
    return AcidOperationalProperties.parseString(transactionalProperties);
  }

  /**
   * Returns the acidOperationalProperties for a given configuration.
   * @param conf A configuration object
   * @return the acidOperationalProperties object for the corresponding configuration.
   */
  public static AcidOperationalProperties getAcidOperationalProperties(Configuration conf) {
    // If the conf does not define any transactional properties, the parseInt() should receive
    // a value of 1, which will set AcidOperationalProperties to a default type and return that.
    return AcidOperationalProperties.parseInt(
            HiveConf.getIntVar(conf, ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES));
  }

  /**
   * Returns the acidOperationalProperties for a given set of properties.
   * @param props A properties object
   * @return the acidOperationalProperties object for the corresponding properties.
   */
  public static AcidOperationalProperties getAcidOperationalProperties(Properties props) {
    String resultStr = props.getProperty(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES);
    if (resultStr == null) {
      // If the properties does not define any transactional properties, we return a default type.
      return AcidOperationalProperties.getDefault();
    }
    return AcidOperationalProperties.parseString(resultStr);
  }

  /**
   * Returns the acidOperationalProperties for a given map.
   * @param parameters A parameters object
   * @return the acidOperationalProperties object for the corresponding map.
   */
  public static AcidOperationalProperties getAcidOperationalProperties(
          Map<String, String> parameters) {
    String resultStr = parameters.get(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES);
    if (resultStr == null) {
      // If the parameters does not define any transactional properties, we return a default type.
      return AcidOperationalProperties.getDefault();
    }
    return AcidOperationalProperties.parseString(resultStr);
  }
  /**
   * See comments at {@link AcidUtils#DELTA_SIDE_FILE_SUFFIX}.
   *
   * Returns the logical end of file for an acid data file.
   *
   * This relies on the fact that if delta_x_y has no committed transactions it wil be filtered out
   * by {@link #getAcidState(Path, Configuration, ValidTxnList)} and so won't be read at all.
   * @param file - data file to read/compute splits on
   */
  public static long getLogicalLength(FileSystem fs, FileStatus file) throws IOException {
    Path lengths = OrcAcidUtils.getSideFile(file.getPath());
    if(!fs.exists(lengths)) {
      /**
       * if here for delta_x_y that means txn y is resolved and all files in this delta are closed so
       * they should all have a valid ORC footer and info from NameNode about length is good
       */
      return file.getLen();
    }
    long len = OrcAcidUtils.getLastFlushLength(fs, file.getPath());
    if(len >= 0) {
      /**
       * if here something is still writing to delta_x_y so  read only as far as the last commit,
       * i.e. where last footer was written.  The file may contain more data after 'len' position
       * belonging to an open txn.
       */
      return len;
    }
    /**
     * if here, side file is there but we couldn't read it.  We want to avoid a read where
     * (file.getLen() < 'value from side file' which may happen if file is not closed) because this
     * means some committed data from 'file' would be skipped.
     * This should be very unusual.
     */
    throw new IOException(lengths + " found but is not readable.  Consider waiting or orcfiledump --recover");
  }


  /**
   * Checks if a table is a transactional table that only supports INSERT, but not UPDATE/DELETE
   * @param params table properties
   * @return true if table is an INSERT_ONLY table, false otherwise
   */
  public static boolean isInsertOnlyTable(Map<String, String> params) {
    return isInsertOnlyTable(params, false);
  }
  public static boolean isInsertOnlyTable(Table table) {
    return isTransactionalTable(table) && getAcidOperationalProperties(table).isInsertOnly();
  }

  // TODO [MM gap]: CTAS may currently be broken. It used to work. See the old code, and why isCtas isn't used?
  public static boolean isInsertOnlyTable(Map<String, String> params, boolean isCtas) {
    String transactionalProp = params.get(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES);
    return (transactionalProp != null && "insert_only".equalsIgnoreCase(transactionalProp));
  }

  public static boolean isInsertOnlyTable(Properties params) {
    String transactionalProp = params.getProperty(
        hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES);
    return (transactionalProp != null && "insert_only".equalsIgnoreCase(transactionalProp));
  }

   /**
    * The method for altering table props; may set the table to MM, non-MM, or not affect MM.
    * todo: All such validation logic should be TransactionValidationListener
    * @param tbl object image before alter table command
    * @param props prop values set in this alter table command
    */
  public static Boolean isToInsertOnlyTable(Table tbl, Map<String, String> props) {
    // Note: Setting these separately is a very hairy issue in certain combinations, since we
    //       cannot decide what type of table this becomes without taking both into account, and
    //       in many cases the conversion might be illegal.
    //       The only thing we allow is tx = true w/o tx-props, for backward compat.
    String transactional = props.get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    String transactionalProp = props.get(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES);

    if (transactional == null && transactionalProp == null) {
      // Not affected or the op is not about transactional.
      return null;
    }

    if(transactional == null) {
      transactional = tbl.getParameters().get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    }
    boolean isSetToTxn = "true".equalsIgnoreCase(transactional);
    if (transactionalProp == null) {
      if (isSetToTxn) return false; // Assume the full ACID table.
      throw new RuntimeException("Cannot change '" + hive_metastoreConstants.TABLE_IS_TRANSACTIONAL
          + "' without '" + hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES + "'");
    }
    if (!"insert_only".equalsIgnoreCase(transactionalProp)) return false; // Not MM.
    if (!isSetToTxn) {
      throw new RuntimeException("Cannot set '"
          + hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES + "' to 'insert_only' without "
          + "setting '" + hive_metastoreConstants.TABLE_IS_TRANSACTIONAL + "' to 'true'");
    }
    return true;
  }

  public static boolean isRemovedInsertOnlyTable(Set<String> removedSet) {
    boolean hasTxn = removedSet.contains(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL),
        hasProps = removedSet.contains(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES);
    return hasTxn || hasProps;
  }

  /**
   * General facility to place a metadta file into a dir created by acid/compactor write.
   *
   * Load Data commands against Acid tables write {@link AcidBaseFileType#ORIGINAL_BASE} type files
   * into delta_x_x/ (or base_x in case there is Overwrite clause).  {@link MetaDataFile} is a
   * small JSON file in this directory that indicates that these files don't have Acid metadata
   * columns and so the values for these columns need to be assigned at read time/compaction.
   */
  public static class MetaDataFile {
    //export command uses _metadata....
    private static final String METADATA_FILE = "_metadata_acid";
    private static final String CURRENT_VERSION = "0";
    //todo: enums? that have both field name and value list
    private interface Field {
      String VERSION = "thisFileVersion";
      String DATA_FORMAT = "dataFormat";
    }
    private interface Value {
      //written by Major compaction
      String COMPACTED = "compacted";
    }

    /**
     * @param baseOrDeltaDir detla or base dir, must exist
     */
    public static void createCompactorMarker(Path baseOrDeltaDir, FileSystem fs) throws IOException {
      /**
       * create _meta_data json file in baseOrDeltaDir
       * write thisFileVersion, dataFormat
       *
       * on read if the file is not there, assume version 0 and dataFormat=acid
       */
      Path formatFile = new Path(baseOrDeltaDir, METADATA_FILE);
      Map<String, String> metaData = new HashMap<>();
      metaData.put(Field.VERSION, CURRENT_VERSION);
      metaData.put(Field.DATA_FORMAT, Value.COMPACTED);
      try (FSDataOutputStream strm = fs.create(formatFile, false)) {
        new ObjectMapper().writeValue(strm, metaData);
      } catch (IOException ioe) {
        String msg = "Failed to create " + baseOrDeltaDir + "/" + METADATA_FILE
            + ": " + ioe.getMessage();
        LOG.error(msg, ioe);
        throw ioe;
      }
    }
    static boolean isCompacted(Path baseOrDeltaDir, FileSystem fs) throws IOException {
      Path formatFile = new Path(baseOrDeltaDir, METADATA_FILE);
      if(!fs.exists(formatFile)) {
        return false;
      }
      try (FSDataInputStream strm = fs.open(formatFile)) {
        Map<String, String> metaData = new ObjectMapper().readValue(strm, Map.class);
        if(!CURRENT_VERSION.equalsIgnoreCase(metaData.get(Field.VERSION))) {
          throw new IllegalStateException("Unexpected Meta Data version: "
              + metaData.get(Field.VERSION));
        }
        String dataFormat = metaData.getOrDefault(Field.DATA_FORMAT, "null");
        switch (dataFormat) {
          case Value.COMPACTED:
            return true;
          default:
            throw new IllegalArgumentException("Unexpected value for " + Field.DATA_FORMAT
                + ": " + dataFormat);
        }
      }
      catch(IOException e) {
        String msg = "Failed to read " + baseOrDeltaDir + "/" + METADATA_FILE
            + ": " + e.getMessage();
        LOG.error(msg, e);
        throw e;
      }
    }

    /**
     * Chooses 1 representantive file from {@code baseOrDeltaDir}
     * This assumes that all files in the dir are of the same type: either written by an acid
     * write or Load Data.  This should always be the case for an Acid table.
     */
    private static Path chooseFile(Path baseOrDeltaDir, FileSystem fs) throws IOException {
      if(!(baseOrDeltaDir.getName().startsWith(BASE_PREFIX) ||
          baseOrDeltaDir.getName().startsWith(DELTA_PREFIX))) {
        throw new IllegalArgumentException(baseOrDeltaDir + " is not a base/delta");
      }
      FileStatus[] dataFiles = fs.listStatus(new Path[] {baseOrDeltaDir}, originalBucketFilter);
      return dataFiles != null && dataFiles.length > 0 ? dataFiles[0].getPath() : null;
    }

    /**
     * Checks if the files in base/delta dir are a result of Load Data statement and thus do not
     * have ROW_IDs embedded in the data.
     * @param baseOrDeltaDir base or delta file.
     */
    public static boolean isRawFormat(Path baseOrDeltaDir, FileSystem fs) throws IOException {
      Path dataFile = chooseFile(baseOrDeltaDir, fs);
      if (dataFile == null) {
        //directory is empty or doesn't have any that could have been produced by load data
        return false;
      }
      try {
        Reader reader = OrcFile.createReader(dataFile, OrcFile.readerOptions(fs.getConf()));
        /*
          acid file would have schema like <op, otid, writerId, rowid, ctid, <f1, ... fn>> so could
          check it this way once/if OrcRecordUpdater.ACID_KEY_INDEX_NAME is removed
          TypeDescription schema = reader.getSchema();
          List<String> columns = schema.getFieldNames();
         */
        return OrcInputFormat.isOriginal(reader);
      } catch (FileFormatException ex) {
        //We may be parsing a delta for Insert-only table which may not even be an ORC file so
        //cannot have ROW_IDs in it.
        LOG.debug("isRawFormat() called on " + dataFile + " which is not an ORC file: " +
            ex.getMessage());
        return true;
      }
    }
  }
}
