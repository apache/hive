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

import static org.apache.hadoop.hive.ql.exec.Utilities.COPY_KEYWORD;
import static org.apache.hadoop.hive.ql.parse.CalcitePlanner.ASTSearcher;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.base.Strings;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.common.*;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.TransactionalValidationListener;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.table.create.CreateTableDesc;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcRecordUpdater;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.LoadSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
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

import javax.annotation.concurrent.Immutable;
import java.nio.charset.Charset;
import java.util.stream.Stream;

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
  
  public static final PathFilter acidHiddenFileFilter = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      String name = p.getName();
      // Don't filter out MetaDataFile.METADATA_FILE
      if (name.startsWith(MetaDataFile.METADATA_FILE)) {
        return true;
      }
      // Don't filter out OrcAcidVersion.ACID_FORMAT
      if (name.startsWith(OrcAcidVersion.ACID_FORMAT)) {
        return true;
      }
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };
  
  public static final PathFilter acidTempDirFilter = new PathFilter() {
    @Override
    public boolean accept(Path dirPath) {
      String dirPathStr = dirPath.toString();
      // We don't want to filter out temp tables
      if (dirPathStr.contains(SessionState.TMP_PREFIX)) {
        return true;
      }
      if ((dirPathStr.contains("/.")) || (dirPathStr.contains("/_"))) {
        return false;
      } else {
        return true;
      }
    }
  };
  
  public static final String VISIBILITY_PREFIX = "_v";
  public static final Pattern VISIBILITY_PATTERN = Pattern.compile(VISIBILITY_PREFIX + "\\d+");

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

  public static String baseDir(long writeId) {
    return BASE_PREFIX + String.format(DELTA_DIGITS, writeId);
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
   * Return a base or delta directory path according to the given "options".
   */
  public static Path baseOrDeltaSubdirPath(Path directory, AcidOutputFormat.Options options) {
    String subdir;
    if (options.isWritingBase()) {
      subdir = BASE_PREFIX + String.format(DELTA_DIGITS,
              options.getMaximumWriteId());
    } else if(options.getStatementId() == -1) {
      //when minor compaction runs, we collapse per statement delta files inside a single
      //transaction so we no longer need a statementId in the file name
      subdir = options.isWritingDeleteDelta() ?
              deleteDeltaSubdir(options.getMinimumWriteId(),
                      options.getMaximumWriteId())
              : deltaSubdir(options.getMinimumWriteId(),
              options.getMaximumWriteId());
    } else {
      subdir = options.isWritingDeleteDelta() ?
              deleteDeltaSubdir(options.getMinimumWriteId(),
                      options.getMaximumWriteId(),
                      options.getStatementId())
              : deltaSubdir(options.getMinimumWriteId(),
              options.getMaximumWriteId(),
              options.getStatementId());
    }
    subdir = addVisibilitySuffix(subdir, options.getVisibilityTxnId());
    return new Path(directory, subdir);
  }

  /**
   * Create a filename for a bucket file.
   * @param directory the partition directory
   * @param options the options for writing the bucket
   * @return the filename that should store the bucket
   */
  public static Path createFilename(Path directory,
                                    AcidOutputFormat.Options options) {
    if (options.getOldStyle()) {
      return new Path(directory, String.format(LEGACY_FILE_BUCKET_DIGITS,
          options.getBucketId()) + "_0");
    } else {
      return createBucketFile(baseOrDeltaSubdirPath(directory, options), options.getBucketId());
    }
  }

  /**
   * Since Hive 4.0, compactor produces directories with {@link #VISIBILITY_PATTERN} suffix.
   * _v0 is equivalent to no suffix, for backwards compatibility.
   */
  static String addVisibilitySuffix(String baseOrDeltaDir, long visibilityTxnId) {
    if(visibilityTxnId == 0) {
      return baseOrDeltaDir;
    }
    return baseOrDeltaDir + VISIBILITY_PREFIX
        + String.format(DELTA_DIGITS, visibilityTxnId);
  }
  /**
   * Represents bucketId and copy_N suffix
   */
  public static final class BucketMetaData {
    private static final BucketMetaData INVALID = new BucketMetaData(-1, 0);
    /**
     * @param bucketFileName {@link #ORIGINAL_PATTERN} or {@link #ORIGINAL_PATTERN_COPY}
     */
    public static BucketMetaData parse(String bucketFileName) {
      if (ORIGINAL_PATTERN.matcher(bucketFileName).matches()) {
        int bucketId = Integer
            .parseInt(bucketFileName.substring(0, bucketFileName.indexOf('_')));
        return new BucketMetaData(bucketId, 0);
      }
      else if(ORIGINAL_PATTERN_COPY.matcher(bucketFileName).matches()) {
        int copyNumber = Integer.parseInt(
            bucketFileName.substring(bucketFileName.lastIndexOf('_') + 1));
        int bucketId = Integer
            .parseInt(bucketFileName.substring(0, bucketFileName.indexOf('_')));
        return new BucketMetaData(bucketId, copyNumber);
      }
      else if (bucketFileName.startsWith(BUCKET_PREFIX)) {
        return new BucketMetaData(Integer
            .parseInt(bucketFileName.substring(bucketFileName.indexOf('_') + 1)), 0);
      }
      return INVALID;
    }
    public static BucketMetaData parse(Path bucketFile) {
      return parse(bucketFile.getName());
    }
    /**
     * -1 if non-standard file name
     */
    public final int bucketId;
    /**
     * 0 means no copy_N suffix
     */
    public final int copyNumber;
    private BucketMetaData(int bucketId, int copyNumber) {
      this.bucketId = bucketId;
      this.copyNumber = copyNumber;
    }
  }
  /**
   * Get the bucket id from the file path
   * @param bucketFile - bucket file path
   * @return - bucket id
   */
  public static int parseBucketId(Path bucketFile) {
    String filename = bucketFile.getName();
    if (ORIGINAL_PATTERN.matcher(filename).matches() || ORIGINAL_PATTERN_COPY.matcher(filename).matches()) {
      return Integer.parseInt(filename.substring(0, filename.indexOf('_')));
    } else if (filename.startsWith(BUCKET_PREFIX)) {
      return Integer.parseInt(filename.substring(filename.indexOf('_') + 1));
    }
    return -1;
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
    int bucket = parseBucketId(bucketFile);
    if (ORIGINAL_PATTERN.matcher(filename).matches()) {
      result
          .setOldStyle(true)
          .minimumWriteId(0)
          .maximumWriteId(0)
          .bucket(bucket)
          .writingBase(!bucketFile.getParent().getName().startsWith(DELTA_PREFIX));
    }
    else if(ORIGINAL_PATTERN_COPY.matcher(filename).matches()) {
      //todo: define groups in regex and use parseInt(Matcher.group(2))....
      int copyNumber = Integer.parseInt(filename.substring(filename.lastIndexOf('_') + 1));
      result
        .setOldStyle(true)
        .minimumWriteId(0)
        .maximumWriteId(0)
        .bucket(bucket)
        .copyNumber(copyNumber)
        .writingBase(!bucketFile.getParent().getName().startsWith(DELTA_PREFIX));
    }
    else if (filename.startsWith(BUCKET_PREFIX)) {
      if (bucketFile.getParent().getName().startsWith(BASE_PREFIX)) {
        result
            .setOldStyle(false)
            .minimumWriteId(0)
            .maximumWriteId(ParsedBase.parseBase(bucketFile.getParent()).getWriteId())
            .bucket(bucket)
            .writingBase(true);
      } else if (bucketFile.getParent().getName().startsWith(DELTA_PREFIX)) {
        ParsedDelta parsedDelta = parsedDelta(bucketFile.getParent(), DELTA_PREFIX,
          bucketFile.getFileSystem(conf), null);
        result
            .setOldStyle(false)
            .minimumWriteId(parsedDelta.minWriteId)
            .maximumWriteId(parsedDelta.maxWriteId)
            .bucket(bucket);
      } else if (bucketFile.getParent().getName().startsWith(DELETE_DELTA_PREFIX)) {
        ParsedDelta parsedDelta = parsedDelta(bucketFile.getParent(), DELETE_DELTA_PREFIX,
          bucketFile.getFileSystem(conf), null);
        result
            .setOldStyle(false)
            .minimumWriteId(parsedDelta.minWriteId)
            .maximumWriteId(parsedDelta.maxWriteId)
            .bucket(bucket);
      }
    } else {
      result.setOldStyle(true).bucket(bucket).minimumWriteId(0)
          .maximumWriteId(0);
    }
    return result;
  }

  public static final class DirectoryImpl implements Directory {
    private final List<Path> abortedDirectories;
    private final boolean isBaseInRawFormat;
    private final List<HdfsFileStatusWithId> original;
    private final List<Path> obsolete;
    private final List<ParsedDelta> deltas;
    private final Path base;

    public DirectoryImpl(List<Path> abortedDirectories,
        boolean isBaseInRawFormat, List<HdfsFileStatusWithId> original,
        List<Path> obsolete, List<ParsedDelta> deltas, Path base) {
      this.abortedDirectories = abortedDirectories == null ?
          Collections.emptyList() : abortedDirectories;
      this.isBaseInRawFormat = isBaseInRawFormat;
      this.original = original == null ? Collections.emptyList() : original;
      this.obsolete = obsolete == null ? Collections.emptyList() : obsolete;
      this.deltas = deltas == null ? Collections.emptyList() : deltas;
      this.base = base;
    }

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
    public List<Path> getObsolete() {
      return obsolete;
    }

    @Override
    public List<Path> getAbortedDirectories() {
      return abortedDirectories;
    }
    
    @Override
    public String toString() {
      return "Aborted Directories: " + abortedDirectories + "; isBaseInRawFormat: " + isBaseInRawFormat + "; original: "
          + original + "; obsolete: " + obsolete + "; deltas: " + deltas + "; base: " + base;
    }
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

  /**
   * Interface used to provide ACID directory information.
   */
  public interface Directory {

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
     * See {@link org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDeltaLight#compareTo(org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDeltaLight)}
     * for details.
     * @return the minimal list of current directories
     */
    List<ParsedDelta> getCurrentDirectories();

    /**
     * Get the list of obsolete directories. After filtering out bases and
     * deltas that are not selected by the valid transaction/write ids list, return the
     * list of original files, bases, and deltas that have been replaced by
     * more up to date ones.  Not {@code null}.
     */
    List<Path> getObsolete();

    /**
     * Get the list of directories that has nothing but aborted transactions.
     * @return the list of aborted directories
     */
    List<Path> getAbortedDirectories();
  }
  /**
   * Since version 3 but prior to version 4, format of a base is "base_X" where X is a writeId.
   * If this base was produced by a compactor, X is the highest writeId that the compactor included.
   * If this base is produced by Insert Overwrite stmt, X is a writeId of the transaction that
   * executed the insert.
   * Since Hive Version 4.0, the format of a base produced by a compactor is
   * base_X_vY.  X is like before, i.e. the highest writeId compactor included and Y is the
   * visibilityTxnId of the transaction in which the compactor ran.
   * (v(isibility) is a literal to help parsing).
   */
  public static final class ParsedBase {
    private final long writeId;
    private final long visibilityTxnId;
    private final Path baseDirPath;
    ParsedBase(long writeId, Path baseDirPath) {
      this(writeId, 0, baseDirPath);
    }
    ParsedBase(long writeId, long visibilityTxnId, Path baseDirPath) {
      this.writeId = writeId;
      this.visibilityTxnId = visibilityTxnId;
      this.baseDirPath = baseDirPath;
    }
    public long getWriteId() {
      return writeId;
    }
    public long getVisibilityTxnId() {
      return visibilityTxnId;
    }
    public Path getBaseDirPath() {
      return baseDirPath;
    }
    public static ParsedBase parseBase(Path path) {
      String filename = path.getName();
      if(!filename.startsWith(BASE_PREFIX)) {
        throw new IllegalArgumentException(filename + " does not start with " + BASE_PREFIX);
      }
      int idxOfv = filename.indexOf(VISIBILITY_PREFIX);
      if(idxOfv < 0) {
        return new ParsedBase(Long.parseLong(filename.substring(BASE_PREFIX.length())), path);
      }
      return new ParsedBase(Long.parseLong(filename.substring(BASE_PREFIX.length(), idxOfv)),
          Long.parseLong(filename.substring(idxOfv + VISIBILITY_PREFIX.length())), path);
    }
  }

  /**
   * In addition to {@link ParsedDeltaLight} this knows if the data is in raw format, i.e. doesn't
   * have acid metadata columns embedded in the files.  To determine this in some cases
   * requires looking at the footer of the data file which can be expensive so if this info is
   * not needed {@link ParsedDeltaLight} should be used.
   */
  @Immutable
  public static final class ParsedDelta extends ParsedDeltaLight {
    private final boolean isRawFormat;
    /**
     * for pre 1.3.x delta files
     */
    private ParsedDelta(long min, long max, Path path, boolean isDeleteDelta,
        boolean isRawFormat, long visibilityTxnId) {
      this(min, max, path, -1, isDeleteDelta, isRawFormat, visibilityTxnId);
    }
    private ParsedDelta(long min, long max, Path path, int statementId,
        boolean isDeleteDelta, boolean isRawFormat, long visibilityTxnId) {
      super(min, max, path, statementId, isDeleteDelta, visibilityTxnId);
      this.isRawFormat = isRawFormat;
    }
    /**
     * Files w/o Acid meta columns embedded in the file. See {@link AcidBaseFileType#ORIGINAL_BASE}
     */
    public boolean isRawFormat() {
      return isRawFormat;
    }
  }
  /**
   * This encapsulates info obtained form the file path.
   * See also {@link ParsedDelta}.
   */
  @Immutable
  public static class ParsedDeltaLight implements Comparable<ParsedDeltaLight> {
    final long minWriteId;
    final long maxWriteId;
    final Path path;
    //-1 is for internal (getAcidState()) purposes and means the delta dir
    //had no statement ID
    final int statementId;
    final boolean isDeleteDelta; // records whether delta dir is of type 'delete_delta_x_y...'
    /**
     * transaction Id of txn which created this delta.  This dir should be considered
     * invisible unless this txn is committed
     *
     * TODO: define TransactionallyVisible interface - add getVisibilityTxnId() etc and all comments
     * use in {@link ParsedBase}, {@link ParsedDelta}, {@link AcidInputFormat.Options}, AcidInputFormat.DeltaMetaData etc
     */
    final long visibilityTxnId;

    public static ParsedDeltaLight parse(Path deltaDir) {
      //passing isRawFormat=false is bogus.  This is just to parse the file name.
      ParsedDelta pd = parsedDelta(deltaDir, false);
      return new ParsedDeltaLight(pd.getMinWriteId(), pd.getMaxWriteId(), deltaDir,
          pd.getStatementId(), pd.isDeleteDelta(), pd.getVisibilityTxnId());
    }

    private ParsedDeltaLight(long min, long max, Path path, int statementId,
        boolean isDeleteDelta, long visibilityTxnId) {
      this.minWriteId = min;
      this.maxWriteId = max;
      this.path = path;
      this.statementId = statementId;
      this.isDeleteDelta = isDeleteDelta;
      this.visibilityTxnId = visibilityTxnId;
    }

    public long getMinWriteId() {
      return minWriteId;
    }

    public long getMaxWriteId() {
      return maxWriteId;
    }

    public Path getPath() {
      return path;
    }

    public int getStatementId() {
      return statementId == -1 ? 0 : statementId;
    }

    public boolean isDeleteDelta() {
      return isDeleteDelta;
    }
    public long getVisibilityTxnId() {
      return visibilityTxnId;
    }
    /**
     * Only un-compacted delta_x_y (x != y) (created by streaming ingest with batch size > 1)
     * may contain a {@link OrcAcidUtils#getSideFile(Path)}.
     * @return
     */
    boolean mayContainSideFile() {
      return !isDeleteDelta() && getMinWriteId() != getMaxWriteId() && getVisibilityTxnId() <= 0;
    }
    /**
     * Compactions (Major/Minor) merge deltas/bases but delete of old files
     * happens in a different process; thus it's possible to have bases/deltas with
     * overlapping writeId boundaries.  The sort order helps figure out the "best" set of files
     * to use to get data.
     * This sorts "wider" delta before "narrower" i.e. delta_5_20 sorts before delta_5_10 (and delta_11_20)
     */
    @Override
    public int compareTo(ParsedDeltaLight parsedDelta) {
      if (minWriteId != parsedDelta.minWriteId) {
        if (minWriteId < parsedDelta.minWriteId) {
          return -1;
        } else {
          return 1;
        }
      } else if (maxWriteId != parsedDelta.maxWriteId) {
        if (maxWriteId < parsedDelta.maxWriteId) {
          return 1;
        } else {
          return -1;
        }
      }
      else if(statementId != parsedDelta.statementId) {
        /**
         * We want deltas after minor compaction (w/o statementId) to sort
         * earlier so that getAcidState() considers compacted files (into larger ones) obsolete
         * Before compaction, include deltas with all statementIds for a given writeId
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
   * todo: rename serializeDeleteDelta()?
   * Convert the list of deltas into an equivalent list of begin/end
   * write id pairs.  Assumes {@code deltas} is sorted.
   * @param deltas
   * @return the list of write ids to serialize
   */
  public static List<AcidInputFormat.DeltaMetaData> serializeDeltas(List<ParsedDelta> deltas) {
    List<AcidInputFormat.DeltaMetaData> result = new ArrayList<>(deltas.size());
    AcidInputFormat.DeltaMetaData last = null;
    for (ParsedDelta parsedDelta : deltas) {
      assert parsedDelta.isDeleteDelta() : "expected delete_delta, got " + parsedDelta.getPath();
      if ((last != null)
              && (last.getMinWriteId() == parsedDelta.getMinWriteId())
              && (last.getMaxWriteId() == parsedDelta.getMaxWriteId())) {
        last.getStmtIds().add(parsedDelta.getStatementId());
        continue;
      }
      last = new AcidInputFormat.DeltaMetaData(parsedDelta.getMinWriteId(),
              parsedDelta.getMaxWriteId(), new ArrayList<>(), parsedDelta.getVisibilityTxnId());
      result.add(last);
      if (parsedDelta.statementId >= 0) {
        last.getStmtIds().add(parsedDelta.getStatementId());
      }
    }
    return result;
  }

  /**
   * Convert the list of begin/end write id pairs to a list of delete delta
   * directories.  Note that there may be multiple delete_delta files for the exact same txn range starting
   * with 2.2.x;
   * see {@link org.apache.hadoop.hive.ql.io.AcidUtils#deltaSubdir(long, long, int)}
   * @param root the root directory
   * @param deleteDeltas list of begin/end write id pairs
   * @return the list of delta paths
   */
  public static Path[] deserializeDeleteDeltas(Path root, final List<AcidInputFormat.DeltaMetaData> deleteDeltas) throws IOException {
    List<Path> results = new ArrayList<>(deleteDeltas.size());
    for(AcidInputFormat.DeltaMetaData dmd : deleteDeltas) {
      if(dmd.getStmtIds().isEmpty()) {
        results.add(new Path(root, dmd.getName()));
        continue;
      }
      for(Integer stmtId : dmd.getStmtIds()) {
        results.add(new Path(root, dmd.getName(stmtId)));
      }
    }
    return results.toArray(new Path[results.size()]);
  }

  /**
   * This will look at a footer of one of the files in the delta to see if the
   * file is in Acid format, i.e. has acid metadata columns.  The assumption is
   * that for any dir, either all files are acid or all are not.
   */
  public static ParsedDelta parsedDelta(Path deltaDir, FileSystem fs) throws IOException {
    String deltaDirName = deltaDir.getName();
    if (deltaDirName.startsWith(DELETE_DELTA_PREFIX)) {
      return parsedDelta(deltaDir, DELETE_DELTA_PREFIX, fs, null);
    }
    return parsedDelta(deltaDir, DELTA_PREFIX, fs, null); // default prefix is delta_prefix
  }

  private static ParsedDelta parseDelta(Path path, String deltaPrefix, FileSystem fs, HdfsDirSnapshot dirSnapshot)
    throws IOException {
    ParsedDelta p = parsedDelta(path, deltaPrefix, fs, dirSnapshot);
    boolean isDeleteDelta = deltaPrefix.equals(DELETE_DELTA_PREFIX);
    return new ParsedDelta(p.getMinWriteId(),
        p.getMaxWriteId(), path, p.statementId, isDeleteDelta, p.isRawFormat(), p.visibilityTxnId);
  }

  public static ParsedDelta parsedDelta(Path deltaDir, String deltaPrefix, FileSystem fs, HdfsDirSnapshot dirSnapshot)
    throws IOException {
    String filename = deltaDir.getName();
    boolean isDeleteDelta = deltaPrefix.equals(DELETE_DELTA_PREFIX);
    if (filename.startsWith(deltaPrefix)) {
      //small optimization - delete delta can't be in raw format
      boolean isRawFormat = !isDeleteDelta && MetaDataFile.isRawFormat(deltaDir, fs, dirSnapshot);
      return parsedDelta(deltaDir, isRawFormat);
    }
    throw new IllegalArgumentException(deltaDir + " does not start with " +
                                       deltaPrefix);
  }

  /**
   * This method just parses the file name.  It relies on caller to figure if
   * the file is in Acid format (i.e. has acid metadata columns) or not.
   * {@link #parsedDelta(Path, FileSystem)}
   */
  public static ParsedDelta parsedDelta(Path deltaDir, boolean isRawFormat) {
    String filename = deltaDir.getName();
    int idxOfVis = filename.indexOf(VISIBILITY_PREFIX);
    long visibilityTxnId = 0;//visibilityTxnId:0 is always visible
    if(idxOfVis >= 0) {
      visibilityTxnId = Long.parseLong(filename.substring(idxOfVis + VISIBILITY_PREFIX.length()));
      filename = filename.substring(0, idxOfVis);
    }
    boolean isDeleteDelta = filename.startsWith(DELETE_DELTA_PREFIX);
    //make sure it's null for delete delta no matter what was passed in - this
    //doesn't apply to delete deltas
    isRawFormat = isDeleteDelta ? false : isRawFormat;
    String rest = filename.substring((isDeleteDelta ?
        DELETE_DELTA_PREFIX : DELTA_PREFIX).length());
    int split = rest.indexOf('_');
    //split2 may be -1 if no statementId
    int split2 = rest.indexOf('_', split + 1);
    long min = Long.parseLong(rest.substring(0, split));
    long max = split2 == -1 ?
        Long.parseLong(rest.substring(split + 1)) :
        Long.parseLong(rest.substring(split + 1, split2));
    if(split2 == -1) {
      return new ParsedDelta(min, max, null, isDeleteDelta, isRawFormat, visibilityTxnId);
    }
    int statementId = Integer.parseInt(rest.substring(split2 + 1));
    return new ParsedDelta(min, max, null, statementId, isDeleteDelta,
        isRawFormat, visibilityTxnId);

  }

  /**
   * Is the given directory in ACID format?
   * @param directory the partition directory to check
   * @param conf the query configuration
   * @return true, if it is an ACID directory
   * @throws IOException
   */
  public static boolean isAcid(Path directory, Configuration conf) throws IOException {
    return isAcid(null, directory, conf);
  }

  public static boolean isAcid(FileSystem fileSystem, Path directory,
                               Configuration conf) throws IOException {
    FileSystem fs = fileSystem == null ? directory.getFileSystem(conf) : fileSystem;
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

  /** State class for getChildState; cannot modify 2 things in a method. */
  private static class TxnBase {
    private FileStatus status;
    private long writeId = 0;
    private long oldestBaseWriteId = Long.MAX_VALUE;
    private Path oldestBase = null;
    private HdfsDirSnapshot dirSnapShot;
  }

  /**
   * Get the ACID state of the given directory. It finds the minimal set of
   * base and diff directories. Note that because major compactions don't
   * preserve the history, we can't use a base directory that includes a
   * write id that we must exclude.
   * @param directory the partition directory to analyze
   * @param conf the configuration
   * @param writeIdList the list of write ids that we are reading
   * @return the state of the directory
   * @throws IOException
   */
  @VisibleForTesting
  public static Directory getAcidState(FileSystem fileSystem, Path candidateDirectory, Configuration conf,
      ValidWriteIdList writeIdList, Ref<Boolean> useFileIds, boolean ignoreEmptyFiles,
      Map<String, String> tblproperties, boolean generateDirSnapshots) throws IOException {
    ValidTxnList validTxnList = null;
    String s = conf.get(ValidTxnList.VALID_TXNS_KEY);
    if(!Strings.isNullOrEmpty(s)) {
      /**
       * getAcidState() is sometimes called on non-transactional tables, e.g.
       * OrcInputFileFormat.FileGenerator.callInternal().  e.g. orc_merge3.q In that case
       * writeIdList is bogus - doesn't even have a table name.
       * see https://issues.apache.org/jira/browse/HIVE-20856.
       *
       * For now, assert that ValidTxnList.VALID_TXNS_KEY is set only if this is really a read
       * of a transactional table.
       * see {@link #getChildState(FileStatus, HdfsFileStatusWithId, ValidWriteIdList, List, List, List, List, TxnBase, boolean, List, Map, FileSystem, ValidTxnList)}
       */
      validTxnList = new ValidReadTxnList();
      validTxnList.readFromString(s);
    }

    FileSystem fs = fileSystem == null ? candidateDirectory.getFileSystem(conf) : fileSystem;
    // The following 'deltas' includes all kinds of delta files including insert & delete deltas.
    final List<ParsedDelta> deltas = new ArrayList<ParsedDelta>();
    List<ParsedDelta> working = new ArrayList<ParsedDelta>();
    List<Path> originalDirectories = new ArrayList<>();
    final List<Path> obsolete = new ArrayList<>();
    final List<Path> abortedDirectories = new ArrayList<>();
    List<HdfsFileStatusWithId> childrenWithId = tryListLocatedHdfsStatus(useFileIds, fs, candidateDirectory);

    TxnBase bestBase = new TxnBase();
    final List<HdfsFileStatusWithId> original = new ArrayList<>();
    Map<Path, HdfsDirSnapshot> dirSnapshots = null;
    if (childrenWithId != null) {
      for (HdfsFileStatusWithId child : childrenWithId) {
        getChildState(child.getFileStatus(), child, writeIdList, working, originalDirectories, original, obsolete,
            bestBase, ignoreEmptyFiles, abortedDirectories, tblproperties, fs, validTxnList);
      }
    } else {
      if (generateDirSnapshots) {
        dirSnapshots = getHdfsDirSnapshots(fs, candidateDirectory);
        getChildState(candidateDirectory, dirSnapshots, writeIdList, working, originalDirectories, original, obsolete,
            bestBase, ignoreEmptyFiles, abortedDirectories, tblproperties, fs, validTxnList);
      } else {
        List<FileStatus> children = HdfsUtils.listLocatedStatus(fs, candidateDirectory, hiddenFileFilter);
        for (FileStatus child : children) {
          getChildState(child, null, writeIdList, working, originalDirectories, original, obsolete, bestBase,
              ignoreEmptyFiles, abortedDirectories, tblproperties, fs, validTxnList);
        }
      }
    }
    // If we have a base, the original files are obsolete.
    if (bestBase.status != null) {
      // Add original files to obsolete list if any
      for (HdfsFileStatusWithId fswid : original) {
        obsolete.add(fswid.getFileStatus().getPath());
      }
      // Add original directories to obsolete list if any
      obsolete.addAll(originalDirectories);
      // remove the entries so we don't get confused later and think we should
      // use them.
      original.clear();
      originalDirectories.clear();
    } else {
      // Okay, we're going to need these originals.  
      // Recurse through them and figure out what we really need.
      // If we already have the original list, do nothing
      // If dirSnapshots != null, we would have already populated "original"
      if (dirSnapshots == null) {
        for (Path origDir : originalDirectories) {
          findOriginals(fs, origDir, original, useFileIds, ignoreEmptyFiles, true);
        }
      }
    }
    Collections.sort(working);
    //so now, 'working' should be sorted like delta_5_20 delta_5_10 delta_11_20 delta_51_60 for example
    //and we want to end up with the best set containing all relevant data: delta_5_20 delta_51_60,
    //subject to list of 'exceptions' in 'writeIdList' (not show in above example).
    long current = bestBase.writeId;
    int lastStmtId = -1;
    ParsedDelta prev = null;
    for(ParsedDelta next: working) {
      if (next.maxWriteId > current) {
        // are any of the new transactions ones that we care about?
        if (writeIdList.isWriteIdRangeValid(current+1, next.maxWriteId) !=
                ValidWriteIdList.RangeResponse.NONE) {
          deltas.add(next);
          current = next.maxWriteId;
          lastStmtId = next.statementId;
          prev = next;
        }
      }
      else if(next.maxWriteId == current && lastStmtId >= 0) {
        //make sure to get all deltas within a single transaction;  multi-statement txn
        //generate multiple delta files with the same txnId range
        //of course, if maxWriteId has already been minor compacted, all per statement deltas are obsolete
        deltas.add(next);
        prev = next;
      }
      else if (prev != null && next.maxWriteId == prev.maxWriteId
                  && next.minWriteId == prev.minWriteId
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

    if(bestBase.oldestBase != null && bestBase.status == null &&
        isCompactedBase(ParsedBase.parseBase(bestBase.oldestBase), fs, dirSnapshots)) {
      /**
       * If here, it means there was a base_x (> 1 perhaps) but none were suitable for given
       * {@link writeIdList}.  Note that 'original' files are logically a base_Long.MIN_VALUE and thus
       * cannot have any data for an open txn.  We could check {@link deltas} has files to cover
       * [1,n] w/o gaps but this would almost never happen...
       *
       * We only throw for base_x produced by Compactor since that base erases all history and
       * cannot be used for a client that has a snapshot in which something inside this base is
       * open.  (Nor can we ignore this base of course)  But base_x which is a result of IOW,
       * contains all history so we treat it just like delta wrt visibility.  Imagine, IOW which
       * aborts. It creates a base_x, which can and should just be ignored.*/
      long[] exceptions = writeIdList.getInvalidWriteIds();
      String minOpenWriteId = exceptions != null && exceptions.length > 0 ?
        Long.toString(exceptions[0]) : "x";
      throw new IOException(ErrorMsg.ACID_NOT_ENOUGH_HISTORY.format(
        Long.toString(writeIdList.getHighWatermark()),
              minOpenWriteId, bestBase.oldestBase.toString()));
    }

    Path base = null;
    boolean isBaseInRawFormat = false;
    if (bestBase.status != null) {
      base = bestBase.status.getPath();
      isBaseInRawFormat = MetaDataFile.isRawFormat(base, fs, dirSnapshots != null ? dirSnapshots.get(base) : null);
      if (isBaseInRawFormat && (bestBase.dirSnapShot != null)) {
        for (FileStatus stat : bestBase.dirSnapShot.getFiles()) {
          if ((!ignoreEmptyFiles) || (stat.getLen() != 0)) {
            original.add(createOriginalObj(null, stat));
          }
        }
      }
    }
    LOG.debug("in directory " + candidateDirectory.toUri().toString() + " base = " + base + " deltas = " +
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
    return new DirectoryImpl(abortedDirectories, isBaseInRawFormat, original,
        obsolete, deltas, base);
  }
  
  public static Map<Path, HdfsDirSnapshot> getHdfsDirSnapshots(final FileSystem fs,
      final Path path) throws IOException {
    try {
      Map<Path, HdfsDirSnapshot> dirToSnapshots = new HashMap<Path, HdfsDirSnapshot>();
      RemoteIterator<LocatedFileStatus> itr = fs.listFiles(path, true);
      while (itr.hasNext()) {
        FileStatus fStatus = itr.next();
        Path fPath = fStatus.getPath();
        if (acidHiddenFileFilter.accept(fPath)) {
          if (fStatus.isDirectory() && acidTempDirFilter.accept(fPath)) {
            HdfsDirSnapshot dirSnapshot = dirToSnapshots.get(fPath);
            if (dirSnapshot == null) {
              dirSnapshot = new HdfsDirSnapshotImpl(fPath, fStatus);
              dirToSnapshots.put(fPath, dirSnapshot);
            }
          } else {
            Path parentDirPath = fPath.getParent();
            if (acidTempDirFilter.accept(parentDirPath)) {
              HdfsDirSnapshot dirSnapshot = dirToSnapshots.get(parentDirPath);
              FileStatus parentDirFStatus = null;
              if (!parentDirPath.equals(path)) {
                parentDirFStatus = fs.getFileStatus(parentDirPath);
              }
              if (dirSnapshot == null) {
                dirSnapshot = new HdfsDirSnapshotImpl(parentDirPath, parentDirFStatus);
                dirToSnapshots.put(parentDirPath, dirSnapshot);
              }
              // We're not filtering out the metadata file and acid format file, as they represent parts of a valid snapshot
              // We're not using the cached values downstream, but we can potentially optimize more in a follow-up task
              if (fStatus.getPath().toString().contains(MetaDataFile.METADATA_FILE)) {
                dirSnapshot.addMetadataFile(fStatus);
              } else if (fStatus.getPath().toString().contains(OrcAcidVersion.ACID_FORMAT)) {
                dirSnapshot.addOrcAcidFormatFile(fStatus);
              } else {
                dirSnapshot.addFile(fStatus);
              }
            }
          }
        }
      }
      return dirToSnapshots;
    } catch (IOException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }
  
  /**
   * DFS dir listing. 
   * Captures a dir and the corresponding list of files it contains,
   * with additional properties about the dir (like isBase etc)
   *
   */
  public static interface HdfsDirSnapshot {
    public Path getPath();

    public void addOrcAcidFormatFile(FileStatus fStatus);

    public FileStatus getOrcAcidFormatFile();

    public void addMetadataFile(FileStatus fStatus);

    public FileStatus getMetadataFile(FileStatus fStatus);

    // FileStatus of this HDFS directory
    public FileStatus getFileStatus();

    // Get the list of files if any within this directory
    public List<FileStatus> getFiles();

    public void setFileStatus(FileStatus fStatus);

    public void addFile(FileStatus file);

    // File id or null
    public Long getFileId();

    public Boolean isRawFormat();

    public void setIsRawFormat(boolean isRawFormat);

    public Boolean isBase();

    public void setIsBase(boolean isBase);

    Boolean isValidBase();

    public void setIsValidBase(boolean isValidBase);

    Boolean isCompactedBase();

    public void setIsCompactedBase(boolean isCompactedBase);

    boolean contains(Path path);
  }
  
  public static class HdfsDirSnapshotImpl implements HdfsDirSnapshot {
    private Path dirPath;
    private FileStatus fStatus;
    private FileStatus metadataFStatus = null;
    private FileStatus orcAcidFormatFStatus = null;
    private List<FileStatus> files = new ArrayList<FileStatus>();
    private Long fileId = null;
    private Boolean isRawFormat = null;
    private Boolean isBase = null;
    private Boolean isValidBase = null;
    private Boolean isCompactedBase = null;

    public HdfsDirSnapshotImpl(Path path, FileStatus fStatus, List<FileStatus> files) {
      this.dirPath = path;
      this.fStatus = fStatus;
      this.files = files;
    }
    
    public HdfsDirSnapshotImpl(Path path, FileStatus fStatus) {
      this.dirPath = path;
      this.fStatus = fStatus;
    }
    
    @Override
    public Path getPath() {
      return dirPath;
    }
    
    @Override
    public FileStatus getFileStatus() {
      return fStatus;
    }
    
    @Override
    public void setFileStatus(FileStatus fStatus) {
      this.fStatus = fStatus;
    }

    @Override
    public List<FileStatus> getFiles() {
      return files;
    }
    
    @Override
    public void addFile(FileStatus file) {
      files.add(file);
    }

    @Override
    public Long getFileId() {
      return fileId;
    }

    @Override
    public Boolean isRawFormat() {
      return isRawFormat;
    }
    
    @Override
    public void setIsRawFormat(boolean isRawFormat) {
       this.isRawFormat = isRawFormat;
    }

    @Override
    public Boolean isBase() {
      return isBase;
    }

    @Override
    public Boolean isValidBase() {
      return isValidBase;
    }

    @Override
    public Boolean isCompactedBase() {
      return isCompactedBase;
    }

    @Override
    public void setIsBase(boolean isBase) {
      this.isBase = isBase;  
    }

    @Override
    public void setIsValidBase(boolean isValidBase) {
      this.isValidBase = isValidBase;
    }

    @Override
    public void setIsCompactedBase(boolean isCompactedBase) {
      this.isCompactedBase = isCompactedBase;     
    }

    @Override
    public void addOrcAcidFormatFile(FileStatus fStatus) {
      this.orcAcidFormatFStatus = fStatus;
    }
    
    @Override
    public FileStatus getOrcAcidFormatFile() {
      return orcAcidFormatFStatus;
    }

    @Override
    public void addMetadataFile(FileStatus fStatus) {
      this.metadataFStatus = fStatus;
    }

    @Override
    public FileStatus getMetadataFile(FileStatus fStatus) {
      return metadataFStatus;
    }

    @Override
    public boolean contains(Path path) {
      for (FileStatus fileStatus: getFiles()) {
        if (fileStatus.getPath().equals(path)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Path: " + dirPath);
      sb.append("; ");
      sb.append("Files: { ");
      for (FileStatus fstatus : files) {
        sb.append(fstatus);
        sb.append(", ");
      }
      sb.append(" }");
      return sb.toString();
    }
  }
  
  /**
   * We can only use a 'base' if it doesn't have an open txn (from specific reader's point of view)
   * A 'base' with open txn in its range doesn't have 'enough history' info to produce a correct
   * snapshot for this reader.
   * Note that such base is NOT obsolete.  Obsolete files are those that are "covered" by other
   * files within the snapshot.
   * A base produced by Insert Overwrite is different.  Logically it's a delta file but one that
   * causes anything written previously to be ignored (hence the overwrite).  In this case, base_x
   * is visible if writeid:x is committed for current reader.
   */
  private static boolean isValidBase(ParsedBase parsedBase, ValidWriteIdList writeIdList,
            FileSystem fs) throws IOException {
    if(parsedBase.getWriteId() == Long.MIN_VALUE) {
      //such base is created by 1st compaction in case of non-acid to acid table conversion
      //By definition there are no open txns with id < 1.
      return true;
    }
    if(isCompactedBase(parsedBase, fs, (HdfsDirSnapshot) null)) {
      return writeIdList.isValidBase(parsedBase.getWriteId());
    }
    //if here, it's a result of IOW
    return writeIdList.isWriteIdValid(parsedBase.getWriteId());
  }
  
  private static boolean isValidBase(HdfsDirSnapshot dirSnapshot, ParsedBase parsedBase, ValidWriteIdList writeIdList,
      FileSystem fs) throws IOException {
    boolean isValidBase;
    if (dirSnapshot.isValidBase() != null) {
      isValidBase = dirSnapshot.isValidBase();
    } else {
      if (isCompactedBase(parsedBase, fs, dirSnapshot)) {
        isValidBase = writeIdList.isValidBase(parsedBase.getWriteId());
      } else {
        isValidBase = writeIdList.isWriteIdValid(parsedBase.getWriteId());
      }
      dirSnapshot.setIsValidBase(isValidBase);
    }
    return isValidBase;
  }
  
  /**
   * Returns {@code true} if {@code parsedBase} was created by compaction.
   * As of Hive 4.0 we can tell if a directory is a result of compaction based on the
   * presence of {@link AcidUtils#VISIBILITY_PATTERN} suffix.  Base directories written prior to
   * that, have to rely on the {@link MetaDataFile} in the directory. So look at the filename first
   * since that is the cheaper test.*/
  private static boolean isCompactedBase(ParsedBase parsedBase, FileSystem fs,
      Map<Path, HdfsDirSnapshot> snapshotMap) throws IOException {
    return isCompactedBase(parsedBase, fs, snapshotMap != null ? snapshotMap.get(parsedBase.getBaseDirPath()) : null);
  }

  private static boolean isCompactedBase(ParsedBase parsedBase, FileSystem fs,
      HdfsDirSnapshot snapshot) throws IOException {
    return parsedBase.getVisibilityTxnId() > 0 || MetaDataFile.isCompacted(parsedBase.getBaseDirPath(), fs, snapshot);
  }
  
  private static void getChildState(FileStatus child, HdfsFileStatusWithId childWithId,
      ValidWriteIdList writeIdList, List<ParsedDelta> working, List<Path> originalDirectories,
      List<HdfsFileStatusWithId> original, List<Path> obsolete, TxnBase bestBase,
      boolean ignoreEmptyFiles, List<Path> aborted, Map<String, String> tblproperties,
      FileSystem fs, ValidTxnList validTxnList) throws IOException {
    Path p = child.getPath();
    String fn = p.getName();
    if (!child.isDirectory()) {
      if (!ignoreEmptyFiles || child.getLen() != 0) {
        original.add(createOriginalObj(childWithId, child));
      }
      return;
    }
    if (fn.startsWith(BASE_PREFIX)) {
      ParsedBase parsedBase = ParsedBase.parseBase(p);
      if(!isDirUsable(child.getPath(), parsedBase.getVisibilityTxnId(), aborted, validTxnList)) {
        return;
      }
      final long writeId = parsedBase.getWriteId();
      if(bestBase.oldestBaseWriteId > writeId) {
        //keep track for error reporting
        bestBase.oldestBase = p;
        bestBase.oldestBaseWriteId = writeId;
      }
      if (bestBase.status == null) {
        if(isValidBase(parsedBase, writeIdList, fs)) {
          bestBase.status = child;
          bestBase.writeId = writeId;
        }
      } else if (bestBase.writeId < writeId) {
        if(isValidBase(parsedBase, writeIdList, fs)) {
          obsolete.add(bestBase.status.getPath());
          bestBase.status = child;
          bestBase.writeId = writeId;
        }
      } else {
        obsolete.add(child.getPath());
      }
    } else if (fn.startsWith(DELTA_PREFIX) || fn.startsWith(DELETE_DELTA_PREFIX)) {
      String deltaPrefix = fn.startsWith(DELTA_PREFIX)  ? DELTA_PREFIX : DELETE_DELTA_PREFIX;
      ParsedDelta delta = parseDelta(child.getPath(), deltaPrefix, fs, null);
      if(!isDirUsable(child.getPath(), delta.getVisibilityTxnId(), aborted, validTxnList)) {
        return;
      }
      if(ValidWriteIdList.RangeResponse.ALL ==
          writeIdList.isWriteIdRangeAborted(delta.minWriteId, delta.maxWriteId)) {
        aborted.add(child.getPath());
      }
      else if (writeIdList.isWriteIdRangeValid(
          delta.minWriteId, delta.maxWriteId) != ValidWriteIdList.RangeResponse.NONE) {
        working.add(delta);
      }
    } else {
      // This is just the directory.  We need to recurse and find the actual files.  But don't
      // do this until we have determined there is no base.  This saves time.  Plus,
      // it is possible that the cleaner is running and removing these original files,
      // in which case recursing through them could cause us to get an error.
      originalDirectories.add(child.getPath());
    }
  }
  
  private static void getChildState(Path candidateDirectory, Map<Path, HdfsDirSnapshot> dirSnapshots,
      ValidWriteIdList writeIdList, List<ParsedDelta> working, List<Path> originalDirectories,
      List<HdfsFileStatusWithId> original,
      List<Path> obsolete, TxnBase bestBase, boolean ignoreEmptyFiles, List<Path> aborted,
      Map<String, String> tblproperties, FileSystem fs, ValidTxnList validTxnList) throws IOException {
    for (HdfsDirSnapshot dirSnapshot : dirSnapshots.values()) {
      FileStatus fStat = dirSnapshot.getFileStatus();
      Path dirPath = dirSnapshot.getPath();
      String dirName = dirPath.getName();
      if (dirPath.equals(candidateDirectory)) {
        // if the candidateDirectory is itself a delta directory, we need to add originals in that directory
        // and return. This is the case when compaction thread calls getChildState.
        for (FileStatus fileStatus : dirSnapshot.getFiles()) {
          if (!ignoreEmptyFiles || fileStatus.getLen() != 0) {
            original.add(createOriginalObj(null, fileStatus));
          }
        }
      } else if (dirName.startsWith(BASE_PREFIX)) {
        bestBase.dirSnapShot = dirSnapshot;
        ParsedBase parsedBase = ParsedBase.parseBase(dirPath);
        if (!isDirUsable(dirPath, parsedBase.getVisibilityTxnId(), aborted, validTxnList)) {
          continue;
        }
        final long writeId = parsedBase.getWriteId();
        if (bestBase.oldestBaseWriteId > writeId) {
          //keep track for error reporting
          bestBase.oldestBase = dirPath;
          bestBase.oldestBaseWriteId = writeId;
        }
        if (bestBase.status == null) {
          if (isValidBase(dirSnapshot, parsedBase, writeIdList, fs)) {
            bestBase.status = fStat;
            bestBase.writeId = writeId;
          }
        } else if (bestBase.writeId < writeId) {
          if (isValidBase(dirSnapshot, parsedBase, writeIdList, fs)) {
            obsolete.add(bestBase.status.getPath());
            bestBase.status = fStat;
            bestBase.writeId = writeId;
          }
        } else {
          obsolete.add(dirPath);
        }
      } else if (dirName.startsWith(DELTA_PREFIX) || dirName.startsWith(DELETE_DELTA_PREFIX)) {
        String deltaPrefix = dirName.startsWith(DELTA_PREFIX) ? DELTA_PREFIX : DELETE_DELTA_PREFIX;
        ParsedDelta delta = parseDelta(dirPath, deltaPrefix, fs, dirSnapshot);
        if (!isDirUsable(dirPath, delta.getVisibilityTxnId(), aborted, validTxnList)) {
          continue;
        }
        if (ValidWriteIdList.RangeResponse.ALL == writeIdList
            .isWriteIdRangeAborted(delta.minWriteId, delta.maxWriteId)) {
          aborted.add(dirPath);
        } else if (writeIdList.isWriteIdRangeValid(delta.minWriteId, delta.maxWriteId)
            != ValidWriteIdList.RangeResponse.NONE) {
          working.add(delta);
        }
      } else {
        originalDirectories.add(dirPath);
        for (FileStatus stat : dirSnapshot.getFiles()) {
          if ((!ignoreEmptyFiles) || (stat.getLen() != 0)) {
            original.add(createOriginalObj(null, stat));
          }
        }
      }
    }
  }
  
  /**
   * checks {@code visibilityTxnId} to see if {@code child} is committed in current snapshot
   */
  private static boolean isDirUsable(Path child, long visibilityTxnId,
      List<Path> aborted, ValidTxnList validTxnList) {
    if(validTxnList == null) {
      throw new IllegalArgumentException("No ValidTxnList for " + child);
    }
    if(!validTxnList.isTxnValid(visibilityTxnId)) {
      boolean isAborted = validTxnList.isTxnAborted(visibilityTxnId);
      if(isAborted) {
        aborted.add(child);//so we can clean it up
      }
      LOG.debug("getChildState() ignoring(" + aborted + ") " + child);
      return false;
    }
    return true;
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
   * @param dir the directory to add
   * @param original the list of original files
   * @throws IOException
   */
  public static void findOriginals(FileSystem fs, Path dir,
      List<HdfsFileStatusWithId> original, Ref<Boolean> useFileIds,
      boolean ignoreEmptyFiles, boolean recursive) throws IOException {
    List<HdfsFileStatusWithId> childrenWithId = tryListLocatedHdfsStatus(useFileIds, fs, dir);
    if (childrenWithId != null) {
      for (HdfsFileStatusWithId child : childrenWithId) {
        if (child.getFileStatus().isDirectory()) {
          if (recursive) {
            findOriginals(fs, child.getFileStatus().getPath(), original, useFileIds,
                ignoreEmptyFiles, true);
          }
        } else {
          if(!ignoreEmptyFiles || child.getFileStatus().getLen() > 0) {
            original.add(child);
          }
        }
      }
    } else {
      List<FileStatus> children = HdfsUtils.listLocatedStatus(fs, dir, hiddenFileFilter);
      for (FileStatus child : children) {
        if (child.isDirectory()) {
          if (recursive) {
            findOriginals(fs, child.getPath(), original, useFileIds, ignoreEmptyFiles, true);
          }
        } else {
          if(!ignoreEmptyFiles || child.getLen() > 0) {
            original.add(createOriginalObj(null, child));
          }
        }
      }
    }
  }

  private static List<HdfsFileStatusWithId> tryListLocatedHdfsStatus(Ref<Boolean> useFileIds, FileSystem fs,
      Path directory) {
    List<HdfsFileStatusWithId> childrenWithId = null;
    if (useFileIds == null) {
      return childrenWithId;
    }
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
    return childrenWithId;
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
  public static boolean isInsertDelta(Path p) {
    return p.getName().startsWith(DELTA_PREFIX);
  }
  public static boolean isTransactionalTable(CreateTableDesc table) {
    if (table == null || table.getTblProps() == null) {
      return false;
    }
    return isTransactionalTable(table.getTblProps());
  }

  public static boolean isTransactionalTable(Map<String, String> props) {
    String tableIsTransactional = props.get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    if (tableIsTransactional == null) {
      tableIsTransactional = props.get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL.toUpperCase());
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

  public static boolean isFullAcidTable(Map<String, String> params) {
    return isTransactionalTable(params) && !isInsertOnlyTable(params);
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
    if (!HiveConf.getBoolVar(conf, ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN)) {
      return false;
    }
    int propInt = conf.getInt(ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname, -1);
    if (propInt == -1) {
      return true;
    }
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
   * by {@link #getAcidState(Path, Configuration, ValidWriteIdList)} and so won't be read at all.
   * @param file - data file to read/compute splits on
   */
  public static long getLogicalLength(FileSystem fs, FileStatus file) throws IOException {
    Path acidDir = file.getPath().getParent(); //should be base_x or delta_x_y_
    if(AcidUtils.isInsertDelta(acidDir)) {
      ParsedDeltaLight pd = ParsedDeltaLight.parse(acidDir);
      if(!pd.mayContainSideFile()) {
        return file.getLen();
      }
    }
    else {
      return file.getLen();
    }
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
    * @param tbl object image before alter table command (or null if not retrieved yet).
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

    if (transactional == null && tbl != null) {
      transactional = tbl.getParameters().get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    }
    boolean isSetToTxn = "true".equalsIgnoreCase(transactional);
    if (transactionalProp == null) {
      if (isSetToTxn || tbl == null) return false; // Assume the full ACID table.
      throw new RuntimeException("Cannot change '" + hive_metastoreConstants.TABLE_IS_TRANSACTIONAL
          + "' without '" + hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES + "'");
    }
    if (!"insert_only".equalsIgnoreCase(transactionalProp)) return false; // Not MM.
    if (!isSetToTxn) {
      if (tbl == null) return true; // No table information yet; looks like it could be valid.
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
   * Get the ValidTxnWriteIdList saved in the configuration.
   */
  public static ValidTxnWriteIdList getValidTxnWriteIdList(Configuration conf) {
    String txnString = conf.get(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY);
    ValidTxnWriteIdList validTxnList = new ValidTxnWriteIdList(txnString);
    return validTxnList;
  }

  /**
   * Extract the ValidWriteIdList for the given table from the list of tables' ValidWriteIdList.
   */
  public static ValidWriteIdList getTableValidWriteIdList(Configuration conf, String fullTableName) {
    ValidTxnWriteIdList validTxnList = getValidTxnWriteIdList(conf);
    return validTxnList.getTableValidWriteIdList(fullTableName);
  }

  /**
   * Set the valid write id list for the current table scan.
   */
  public static void setValidWriteIdList(Configuration conf, ValidWriteIdList validWriteIds) {
    conf.set(ValidWriteIdList.VALID_WRITEIDS_KEY, validWriteIds.toString());
    LOG.debug("Setting ValidWriteIdList: " + validWriteIds.toString()
            + " isAcidTable: " + HiveConf.getBoolVar(conf, ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN, false)
            + " acidProperty: " + getAcidOperationalProperties(conf));
  }

  /**
   * Set the valid write id list for the current table scan.
   */
  public static void setValidWriteIdList(Configuration conf, TableScanDesc tsDesc) {
    if (tsDesc.isTranscationalTable()) {
      String dbName = tsDesc.getDatabaseName();
      String tableName = tsDesc.getTableName();
      ValidWriteIdList validWriteIdList = getTableValidWriteIdList(conf,
                                                    AcidUtils.getFullTableName(dbName, tableName));
      if (validWriteIdList != null) {
        setValidWriteIdList(conf, validWriteIdList);
      } else {
        // Log error if the acid table is missing from the ValidWriteIdList conf
        LOG.error("setValidWriteIdList on table: " + AcidUtils.getFullTableName(dbName, tableName)
                + " isAcidTable: " + true
                + " acidProperty: " + getAcidOperationalProperties(conf)
                + " couldn't find the ValidWriteId list from ValidTxnWriteIdList: "
                + conf.get(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY));
        throw new IllegalStateException("ACID table: " + AcidUtils.getFullTableName(dbName, tableName)
                + " is missing from the ValidWriteIdList config: "
                + conf.get(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY));
      }
    }
  }

  public static class TableSnapshot {
    private long writeId;
    private String validWriteIdList;

    public TableSnapshot() {
    }

    public TableSnapshot(long writeId, String validWriteIdList) {
      this.writeId = writeId;
      this.validWriteIdList = validWriteIdList;
    }

    public String getValidWriteIdList() {
      return validWriteIdList;
    }

    public long getWriteId() {
      return writeId;
    }

    public void setWriteId(long writeId) {
      this.writeId = writeId;
    }

    public void setValidWriteIdList(String validWriteIdList) {
      this.validWriteIdList = validWriteIdList;
    }

    @Override
    public String toString() {
      return "[validWriteIdList=" + validWriteIdList + ", writeId=" + writeId + "]";
    }
  }

  public static TableSnapshot getTableSnapshot(
          Configuration conf,
          Table tbl) throws LockException {
    return getTableSnapshot(conf, tbl, false);
  }


  /** Note: this is generally called in Hive.java; so, the callers of Hive.java make sure
   *        to set up the acid state during compile, and Hive.java retrieves it if needed. */
  public static TableSnapshot getTableSnapshot(
      Configuration conf, Table tbl, boolean isStatsUpdater) throws LockException {
    return getTableSnapshot(conf, tbl, tbl.getDbName(), tbl.getTableName(), isStatsUpdater);
  }

  /** Note: this is generally called in Hive.java; so, the callers of Hive.java make sure
   *        to set up the acid state during compile, and Hive.java retrieves it if needed. */
  public static TableSnapshot getTableSnapshot(Configuration conf,
      Table tbl, String dbName, String tblName, boolean isStatsUpdater)
      throws LockException, AssertionError {
    if (!isTransactionalTable(tbl)) {
      return null;
    }
    if (dbName == null) {
      dbName = tbl.getDbName();
    }
    if (tblName == null) {
      tblName = tbl.getTableName();
    }
    long writeId = -1;
    ValidWriteIdList validWriteIdList = null;

    HiveTxnManager sessionTxnMgr = SessionState.get().getTxnMgr();
    String fullTableName = getFullTableName(dbName, tblName);
    if (sessionTxnMgr != null && sessionTxnMgr.getCurrentTxnId() > 0) {
      validWriteIdList = getTableValidWriteIdList(conf, fullTableName);
      if (isStatsUpdater) {
        writeId = SessionState.get().getTxnMgr() != null ?
                SessionState.get().getTxnMgr().getAllocatedTableWriteId(
                  dbName, tblName) : -1;
        if (writeId < 1) {
          // TODO: this is not ideal... stats updater that doesn't have write ID is currently
          //       "create table"; writeId would be 0/-1 here. No need to call this w/true.
          LOG.debug("Stats updater for {}.{} doesn't have a write ID ({})",
              dbName, tblName, writeId);
        }
      }


      if (HiveConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST)
          && conf.get(ValidTxnList.VALID_TXNS_KEY) == null) {
        return null;
      }
      if (validWriteIdList == null) {
        validWriteIdList = getTableValidWriteIdListWithTxnList(
            conf, dbName, tblName);
      }
      if (validWriteIdList == null) {
        throw new AssertionError("Cannot find valid write ID list for " + tblName);
      }
    }
    return new TableSnapshot(writeId,
        validWriteIdList != null ? validWriteIdList.toString() : null);
  }

  /**
   * Returns ValidWriteIdList for the table with the given "dbName" and "tableName".
   * This is called when HiveConf has no list for the table.
   * Otherwise use getTableSnapshot().
   * @param conf       Configuration
   * @param dbName
   * @param tableName
   * @return ValidWriteIdList on success, null on failure to get a list.
   * @throws LockException
   */
  public static ValidWriteIdList getTableValidWriteIdListWithTxnList(
      Configuration conf, String dbName, String tableName) throws LockException {
    HiveTxnManager sessionTxnMgr = SessionState.get().getTxnMgr();
    if (sessionTxnMgr == null) {
      return null;
    }
    ValidWriteIdList validWriteIdList = null;
    ValidTxnWriteIdList validTxnWriteIdList = null;

    String validTxnList = conf.get(ValidTxnList.VALID_TXNS_KEY);
    List<String> tablesInput = new ArrayList<>();
    String fullTableName = getFullTableName(dbName, tableName);
    tablesInput.add(fullTableName);

    validTxnWriteIdList = sessionTxnMgr.getValidWriteIds(tablesInput, validTxnList);
    return validTxnWriteIdList != null ?
        validTxnWriteIdList.getTableValidWriteIdList(fullTableName) : null;
  }

  public static String getFullTableName(String dbName, String tableName) {
    return TableName.fromString(tableName, null, dbName).getNotEmptyDbTable().toLowerCase();
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
    public static final String METADATA_FILE = "_metadata_acid";
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

    static boolean isCompacted(Path baseOrDeltaDir, FileSystem fs, HdfsDirSnapshot dirSnapshot) throws IOException {
      /**
       * this file was written by Hive versions before 4.0 into a base_x/ dir
       * created by compactor so that it can be distinguished from the one
       * created by Insert Overwrite
       */
      Path formatFile = new Path(baseOrDeltaDir, METADATA_FILE);
      if (dirSnapshot != null && !dirSnapshot.contains(formatFile)) {
        return false;
      }
      if(dirSnapshot == null && !fs.exists(formatFile)) {
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
     * Chooses 1 representative file from {@code baseOrDeltaDir}
     * This assumes that all files in the dir are of the same type: either written by an acid
     * write or Load Data.  This should always be the case for an Acid table.
     */
    private static Path chooseFile(Path baseOrDeltaDir, FileSystem fs) throws IOException {
      if(!(baseOrDeltaDir.getName().startsWith(BASE_PREFIX) ||
          baseOrDeltaDir.getName().startsWith(DELTA_PREFIX))) {
        throw new IllegalArgumentException(baseOrDeltaDir + " is not a base/delta");
      }
      FileStatus[] dataFiles;
      try {
        dataFiles = fs.listStatus(new Path[]{baseOrDeltaDir}, originalBucketFilter);
      } catch (FileNotFoundException e) {
        // HIVE-22001: If the file was not found, this means that baseOrDeltaDir (which was listed
        // earlier during AcidUtils.getAcidState()) was removed sometime between the FS list call
        // and now. In the case of ACID tables the file would only have been removed by the transactional
        // cleaner thread, in which case this is currently an old base/delta which has already been
        // compacted. So a new set of base files from the compaction should exist which
        // the current call to AcidUtils.getAcidState() would use rather than this old baes/delta.
        // It should be ok to ignore this FileNotFound error and skip processing of this file - the list
        // of files for this old base/delta will be incomplete, but it will not matter since this base/delta
        // would be ignored (in favor of the new base files) by the selection logic in AcidUtils.getAcidState().
        dataFiles = null;
      }
      return dataFiles != null && dataFiles.length > 0 ? dataFiles[0].getPath() : null;
    }

    /**
     * Checks if the files in base/delta dir are a result of Load Data/Add Partition statement
     * and thus do not have ROW_IDs embedded in the data.
     * This is only meaningful for full CRUD tables - Insert-only tables have all their data
     * in raw format by definition.
     * @param baseOrDeltaDir base or delta file.
     * @param dirSnapshot 
     */
    public static boolean isRawFormat(Path baseOrDeltaDir, FileSystem fs, HdfsDirSnapshot dirSnapshot) throws IOException {
      //todo: this could be optimized - for full CRUD table only base_x and delta_x_x could have
      // files in raw format delta_x_y (x != y) whether from streaming ingested or compaction
      // must be native Acid format by definition
      if(isDeleteDelta(baseOrDeltaDir)) {
        return false;
      }
      if(isInsertDelta(baseOrDeltaDir)) {
        ParsedDeltaLight pd = ParsedDeltaLight.parse(baseOrDeltaDir);
        if(pd.getMinWriteId() != pd.getMaxWriteId()) {
          //must be either result of streaming or compaction
          return false;
        }
      }
      else {
        //must be base_x
        if(isCompactedBase(ParsedBase.parseBase(baseOrDeltaDir), fs, dirSnapshot)) {
          return false;
        }
      }
      //if here, have to check the files
      Path dataFile = null;
      if ((dirSnapshot != null) && (dirSnapshot.getFiles() != null) && (dirSnapshot.getFiles().size() > 0)) {
        for (FileStatus fileStatus: dirSnapshot.getFiles()) {
          if (originalBucketFilter.accept(fileStatus.getPath())) {
            dataFile = fileStatus.getPath();
          }
        }
      } else {
        dataFile = chooseFile(baseOrDeltaDir, fs);
      }
      if (dataFile == null) {
        //directory is empty or doesn't have any that could have been produced by load data
        return false;
      }
      return isRawFormatFile(dataFile, fs);
    }
    
    public static boolean isRawFormatFile(Path dataFile, FileSystem fs) throws IOException {
      try {
        Reader reader = OrcFile.createReader(dataFile, OrcFile.readerOptions(fs.getConf()));
        /*
          acid file would have schema like <op, owid, writerId, rowid, cwid, <f1, ... fn>> so could
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

  /**
   * Logic related to versioning acid data format.  An {@code ACID_FORMAT} file is written to each
   * base/delta/delete_delta dir written by a full acid write or compaction.  This is the primary
   * mechanism for versioning acid data.
   *
   * Each individual ORC file written stores the current version as a user property in ORC footer.
   * All data files produced by Acid write should have this (starting with Hive 3.0), including
   * those written by compactor.  This is more for sanity checking in case someone moved the files
   * around or something like that.
   */
  public static final class OrcAcidVersion {
    private static final String ACID_VERSION_KEY = "hive.acid.version";
    private static final String ACID_FORMAT = "_orc_acid_version";
    private static final Charset UTF8 = Charset.forName("UTF-8");
    public static final int ORC_ACID_VERSION_DEFAULT = 0;
    /**
     * 2 is the version of Acid released in Hive 3.0.
     */
    public static final int ORC_ACID_VERSION = 2;
    /**
     * Inlucde current acid version in file footer.
     * @param writer - file written
     */
    public static void setAcidVersionInDataFile(Writer writer) {
      //so that we know which version wrote the file
      writer.addUserMetadata(ACID_VERSION_KEY, UTF8.encode(String.valueOf(ORC_ACID_VERSION)));
    }
    /**
     * This is smart enough to handle streaming ingest where there could be a
     * {@link OrcAcidUtils#DELTA_SIDE_FILE_SUFFIX} side file.
     * @param dataFile - ORC acid data file
     * @return version property from file if there,
     *          {@link #ORC_ACID_VERSION_DEFAULT} otherwise
     */
    @VisibleForTesting
    public static int getAcidVersionFromDataFile(Path dataFile, FileSystem fs) throws IOException {
      FileStatus fileStatus = fs.getFileStatus(dataFile);
      Reader orcReader = OrcFile.createReader(dataFile,
          OrcFile.readerOptions(fs.getConf())
              .filesystem(fs)
              //make sure to check for side file in case streaming ingest died
              .maxLength(getLogicalLength(fs, fileStatus)));
      if (orcReader.hasMetadataValue(ACID_VERSION_KEY)) {
        char[] versionChar = UTF8.decode(orcReader.getMetadataValue(ACID_VERSION_KEY)).array();
        String version = new String(versionChar);
        return Integer.valueOf(version);
      }
      return ORC_ACID_VERSION_DEFAULT;
    }
    /**
     * This creates a version file in {@code deltaOrBaseDir}
     * @param deltaOrBaseDir - where to create the version file
     */
    public static void writeVersionFile(Path deltaOrBaseDir, FileSystem fs)  throws IOException {
      Path formatFile = getVersionFilePath(deltaOrBaseDir);
      if(!fs.exists(formatFile)) {
        try (FSDataOutputStream strm = fs.create(formatFile, false)) {
          strm.write(UTF8.encode(String.valueOf(ORC_ACID_VERSION)).array());
        } catch (IOException ioe) {
          LOG.error("Failed to create " + formatFile + " due to: " + ioe.getMessage(), ioe);
          throw ioe;
        }
      }
    }
    public static Path getVersionFilePath(Path deltaOrBase) {
      return new Path(deltaOrBase, ACID_FORMAT);
    }
    @VisibleForTesting
    public static int getAcidVersionFromMetaFile(Path deltaOrBaseDir, FileSystem fs)
        throws IOException {
      Path formatFile = getVersionFilePath(deltaOrBaseDir);
      if(!fs.exists(formatFile)) {
        LOG.debug(formatFile + " not found, returning default: " + ORC_ACID_VERSION_DEFAULT);
        return ORC_ACID_VERSION_DEFAULT;
      }
      try (FSDataInputStream inputStream = fs.open(formatFile)) {
        byte[] bytes = new byte[1];
        int read = inputStream.read(bytes);
        if (read != -1) {
          String version = new String(bytes, UTF8);
          return Integer.valueOf(version);
        }
        return ORC_ACID_VERSION_DEFAULT;
      }
      catch(IOException ex) {
        LOG.error(formatFile + " is unreadable due to: " + ex.getMessage(), ex);
        throw ex;
      }
    }
  }

  public static List<FileStatus> getAcidFilesForStats(
      Table table, Path dir, Configuration jc, FileSystem fs) throws IOException {
    List<FileStatus> fileList = new ArrayList<>();
    ValidWriteIdList idList = AcidUtils.getTableValidWriteIdList(jc,
        AcidUtils.getFullTableName(table.getDbName(), table.getTableName()));
    if (idList == null) {
      LOG.warn("Cannot get ACID state for " + table.getDbName() + "." + table.getTableName()
          + " from " + jc.get(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY));
      return null;
    }
    Directory acidInfo = AcidUtils.getAcidState(fs, dir, jc, idList, null, false, null, true);
    // Assume that for an MM table, or if there's only the base directory, we are good.
    if (!acidInfo.getCurrentDirectories().isEmpty() && AcidUtils.isFullAcidTable(table)) {
      Utilities.FILE_OP_LOGGER.warn(
          "Computing stats for an ACID table; stats may be inaccurate");
    }
    if (fs == null) {
      fs = dir.getFileSystem(jc);
    }
    for (HdfsFileStatusWithId hfs : acidInfo.getOriginalFiles()) {
      fileList.add(hfs.getFileStatus());
    }
    for (ParsedDelta delta : acidInfo.getCurrentDirectories()) {
      for (FileStatus f : HiveStatsUtils.getFileStatusRecurse(delta.getPath(), -1, fs)) {
        fileList.add(f);
      }
    }
    if (acidInfo.getBaseDirectory() != null) {
      for (FileStatus f : HiveStatsUtils.getFileStatusRecurse(
          acidInfo.getBaseDirectory(), -1, fs)) {
        fileList.add(f);
      }
    }
    return fileList;
  }

  public static List<Path> getValidDataPaths(Path dataPath, Configuration conf, String validWriteIdStr)
          throws IOException {
    List<Path> pathList = new ArrayList<>();
    if ((validWriteIdStr == null) || validWriteIdStr.isEmpty()) {
      // If Non-Acid case, then all files would be in the base data path. So, just return it.
      pathList.add(dataPath);
      return pathList;
    }

    // If ACID/MM tables, then need to find the valid state wrt to given ValidWriteIdList.
    ValidWriteIdList validWriteIdList = new ValidReaderWriteIdList(validWriteIdStr);
    Directory acidInfo = AcidUtils.getAcidState(dataPath.getFileSystem(conf), dataPath, conf, validWriteIdList, null,
        false, null, false);

    for (HdfsFileStatusWithId hfs : acidInfo.getOriginalFiles()) {
      pathList.add(hfs.getFileStatus().getPath());
    }
    for (ParsedDelta delta : acidInfo.getCurrentDirectories()) {
      pathList.add(delta.getPath());
    }
    if (acidInfo.getBaseDirectory() != null) {
      pathList.add(acidInfo.getBaseDirectory());
    }
    return pathList;
  }

  public static String getAcidSubDir(Path dataPath) {
    String dataDir = dataPath.getName();
    if (dataDir.startsWith(AcidUtils.BASE_PREFIX)
            || dataDir.startsWith(AcidUtils.DELTA_PREFIX)
            || dataDir.startsWith(AcidUtils.DELETE_DELTA_PREFIX)) {
      return dataDir;
    }
    return null;
  }

  //Get the first level acid directory (if any) from a given path
  public static String getFirstLevelAcidDirPath(Path dataPath, FileSystem fileSystem) throws IOException {
    if (dataPath == null) {
      return null;
    }
    String firstLevelAcidDir = getAcidSubDir(dataPath);
    if (firstLevelAcidDir != null) {
      return firstLevelAcidDir;
    }

    String acidDirPath = getFirstLevelAcidDirPath(dataPath.getParent(), fileSystem);
    if (acidDirPath == null) {
      return null;
    }

    // We need the path for directory so no need to append file name
    if (fileSystem.isDirectory(dataPath)) {
      return acidDirPath + Path.SEPARATOR + dataPath.getName();
    }
    return acidDirPath;
  }

  public static boolean isAcidEnabled(HiveConf hiveConf) {
    String txnMgr = hiveConf.getVar(ConfVars.HIVE_TXN_MANAGER);
    boolean concurrency =  hiveConf.getBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY);
    String dbTxnMgr = "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager";
    if (txnMgr.equals(dbTxnMgr) && concurrency) {
      return true;
    }
    return false;
  }

  public static class AnyIdDirFilter implements PathFilter {
     @Override
     public boolean accept(Path path) {
       return extractWriteId(path) != null;
     }
  }

  public static class IdPathFilter implements PathFilter {
    private String baseDirName, deltaDirName;
    private final boolean isDeltaPrefix;

    public IdPathFilter(long writeId, int stmtId) {
      String deltaDirName = null;
      deltaDirName = DELTA_PREFIX + String.format(DELTA_DIGITS, writeId) + "_" +
              String.format(DELTA_DIGITS, writeId);
      isDeltaPrefix = (stmtId < 0);
      if (!isDeltaPrefix) {
        deltaDirName += "_" + String.format(STATEMENT_DIGITS, stmtId);
      }

      this.baseDirName = BASE_PREFIX + String.format(DELTA_DIGITS, writeId);
      this.deltaDirName = deltaDirName;
    }

    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      return name.equals(baseDirName) || (isDeltaPrefix && name.startsWith(deltaDirName))
          || (!isDeltaPrefix && name.equals(deltaDirName));
    }
  }


  public static Long extractWriteId(Path file) {
    String fileName = file.getName();
    if (!fileName.startsWith(DELTA_PREFIX) && !fileName.startsWith(BASE_PREFIX)) {
      LOG.trace("Cannot extract write ID for a MM table: {}", file);
      return null;
    }
    String[] parts = fileName.split("_", 4);  // e.g. delta_0000001_0000001_0000 or base_0000022
    if (parts.length < 2) {
      LOG.debug("Cannot extract write ID for a MM table: " + file
          + " (" + Arrays.toString(parts) + ")");
      return null;
    }
    long writeId = -1;
    try {
      writeId = Long.parseLong(parts[1]);
    } catch (NumberFormatException ex) {
      LOG.debug("Cannot extract write ID for a MM table: " + file
          + "; parsing " + parts[1] + " got " + ex.getMessage());
      return null;
    }
    return writeId;
  }

  public static void setNonTransactional(Map<String, String> tblProps) {
    tblProps.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "false");
    tblProps.remove(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES);
  }

  private static boolean needsLock(Entity entity) {
    switch (entity.getType()) {
    case TABLE:
      return isLockableTable(entity.getTable());
    case PARTITION:
      return isLockableTable(entity.getPartition().getTable());
    default:
      return true;
    }
  }

  private static Table getTable(WriteEntity we) {
    Table t = we.getTable();
    if (t == null) {
      throw new IllegalStateException("No table info for " + we);
    }
    return t;
  }

  private static boolean isLockableTable(Table t) {
    if (t.isTemporary()) {
      return false;
    }
    switch (t.getTableType()) {
    case MANAGED_TABLE:
    case MATERIALIZED_VIEW:
      return true;
    default:
      return false;
    }
  }

  /**
   * Create lock components from write/read entities.
   * @param outputs write entities
   * @param inputs read entities
   * @param conf
   * @return list with lock components
   */
  public static List<LockComponent> makeLockComponents(Set<WriteEntity> outputs, Set<ReadEntity> inputs,
      HiveConf conf) {
    List<LockComponent> lockComponents = new ArrayList<>();
    // For each source to read, get a shared lock
    for (ReadEntity input : inputs) {
      if (!input.needsLock() || input.isUpdateOrDelete() || !AcidUtils.needsLock(input)) {
        // We don't want to acquire read locks during update or delete as we'll be acquiring write
        // locks instead. Also, there's no need to lock temp tables since they're session wide
        continue;
      }
      LockComponentBuilder compBuilder = new LockComponentBuilder();
      compBuilder.setShared();
      compBuilder.setOperationType(DataOperationType.SELECT);

      Table t = null;
      switch (input.getType()) {
      case DATABASE:
        compBuilder.setDbName(input.getDatabase().getName());
        break;

      case TABLE:
        t = input.getTable();
        compBuilder.setDbName(t.getDbName());
        compBuilder.setTableName(t.getTableName());
        break;

      case PARTITION:
      case DUMMYPARTITION:
        compBuilder.setPartitionName(input.getPartition().getName());
        t = input.getPartition().getTable();
        compBuilder.setDbName(t.getDbName());
        compBuilder.setTableName(t.getTableName());
        break;

      default:
        // This is a file or something we don't hold locks for.
        continue;
      }
      if (t != null) {
        compBuilder.setIsTransactional(AcidUtils.isTransactionalTable(t));
      }
      LockComponent comp = compBuilder.build();
      LOG.debug("Adding lock component to lock request " + comp.toString());
      lockComponents.add(comp);
    }
    // For each source to write to, get the appropriate lock type.  If it's
    // an OVERWRITE, we need to get an exclusive lock.  If it's an insert (no
    // overwrite) than we need a shared.  If it's update or delete then we
    // need a SEMI-SHARED.
    for (WriteEntity output : outputs) {
      LOG.debug("output is null " + (output == null));
      if (output.getType() == Entity.Type.DFS_DIR || output.getType() == Entity.Type.LOCAL_DIR || !AcidUtils
          .needsLock(output)) {
        // We don't lock files or directories. We also skip locking temp tables.
        continue;
      }
      LockComponentBuilder compBuilder = new LockComponentBuilder();
      Table t = null;
      switch (output.getType()) {
      case DATABASE:
        compBuilder.setDbName(output.getDatabase().getName());
        break;

      case TABLE:
      case DUMMYPARTITION:   // in case of dynamic partitioning lock the table
        t = output.getTable();
        compBuilder.setDbName(t.getDbName());
        compBuilder.setTableName(t.getTableName());
        break;

      case PARTITION:
        compBuilder.setPartitionName(output.getPartition().getName());
        t = output.getPartition().getTable();
        compBuilder.setDbName(t.getDbName());
        compBuilder.setTableName(t.getTableName());
        break;

      default:
        // This is a file or something we don't hold locks for.
        continue;
      }
      switch (output.getWriteType()) {
        /* base this on HiveOperation instead?  this and DDL_NO_LOCK is peppered all over the code...
         Seems much cleaner if each stmt is identified as a particular HiveOperation (which I'd think
         makes sense everywhere).  This however would be problematic for merge...*/
      case DDL_EXCLUSIVE:
        compBuilder.setExclusive();
        compBuilder.setOperationType(DataOperationType.NO_TXN);
        break;
      case INSERT_OVERWRITE:
        t = AcidUtils.getTable(output);
        if (AcidUtils.isTransactionalTable(t)) {
          if (conf.getBoolVar(HiveConf.ConfVars.TXN_OVERWRITE_X_LOCK)) {
            compBuilder.setExclusive();
          } else {
            compBuilder.setSemiShared();
          }
          compBuilder.setOperationType(DataOperationType.UPDATE);
        } else {
          compBuilder.setExclusive();
          compBuilder.setOperationType(DataOperationType.NO_TXN);
        }
        break;
      case INSERT:
        assert t != null;
        if (AcidUtils.isTransactionalTable(t)) {
          compBuilder.setShared();
        } else if (MetaStoreUtils.isNonNativeTable(t.getTTable())) {
          final HiveStorageHandler storageHandler = Preconditions.checkNotNull(t.getStorageHandler(),
              "Thought all the non native tables have an instance of storage handler");
          LockType lockType = storageHandler.getLockType(output);
          if (null == LockType.findByValue(lockType.getValue())) {
            throw new IllegalArgumentException(String
                .format("Lock type [%s] for Database.Table [%s.%s] is unknown", lockType, t.getDbName(),
                    t.getTableName()));
          }
          compBuilder.setLock(lockType);
        } else {
          if (conf.getBoolVar(HiveConf.ConfVars.HIVE_TXN_STRICT_LOCKING_MODE)) {
            compBuilder.setExclusive();
          } else {  // this is backward compatible for non-ACID resources, w/o ACID semantics
            compBuilder.setShared();
          }
        }
        compBuilder.setOperationType(DataOperationType.INSERT);
        break;
      case DDL_SHARED:
        compBuilder.setShared();
        if (!output.isTxnAnalyze()) {
          // Analyze needs txn components to be present, otherwise an aborted analyze write ID
          // might be rolled under the watermark by compactor while stats written by it are
          // still present.
          compBuilder.setOperationType(DataOperationType.NO_TXN);
        }
        break;

      case UPDATE:
        compBuilder.setSemiShared();
        compBuilder.setOperationType(DataOperationType.UPDATE);
        break;
      case DELETE:
        compBuilder.setSemiShared();
        compBuilder.setOperationType(DataOperationType.DELETE);
        break;

      case DDL_NO_LOCK:
        continue; // No lock required here

      default:
        throw new RuntimeException("Unknown write type " + output.getWriteType().toString());
      }
      if (t != null) {
        compBuilder.setIsTransactional(AcidUtils.isTransactionalTable(t));
      }

      compBuilder.setIsDynamicPartitionWrite(output.isDynamicPartitionWrite());
      LockComponent comp = compBuilder.build();
      LOG.debug("Adding lock component to lock request " + comp.toString());
      lockComponents.add(comp);
    }
    return lockComponents;
  }

  /**
   * Safety check to make sure a file take from one acid table is not added into another acid table
   * since the ROW__IDs embedded as part a write to one table won't make sense in different
   * table/cluster.
   */
  public static void validateAcidFiles(Table table, FileStatus[] srcs, FileSystem fs) throws SemanticException {
    if (!AcidUtils.isFullAcidTable(table)) {
      return;
    }
    validateAcidFiles(srcs, fs);
  }

  private static void validateAcidFiles(FileStatus[] srcs, FileSystem fs) throws SemanticException {
    try {
      if (srcs == null) {
        return;
      }
      for (FileStatus oneSrc : srcs) {
        if (!AcidUtils.MetaDataFile.isRawFormatFile(oneSrc.getPath(), fs)) {
          throw new SemanticException(ErrorMsg.LOAD_DATA_ACID_FILE, oneSrc.getPath().toString());
        }
      }
    } catch (IOException ex) {
      throw new SemanticException(ex);
    }
  }

  /**
   * Safety check to make sure the given location is not the location of acid table and
   * all it's files  will be not added into another acid table
   */
  public static void validateAcidPartitionLocation(String location, Configuration conf) throws SemanticException {
    try {
      URI uri = new URI(location);
      FileSystem fs = FileSystem.get(uri, conf);
      FileStatus[] fileStatuses = LoadSemanticAnalyzer.matchFilesOrDir(fs, new Path(uri));
      validateAcidFiles(fileStatuses, fs);
    } catch (IOException | URISyntaxException ex) {
      throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(ex.getMessage()), ex);
    }
  }

  /**
   * Determines transaction type based on query AST.
   * @param tree AST
   */
  public static TxnType getTxnType(Configuration conf, ASTNode tree) {
    final ASTSearcher astSearcher = new ASTSearcher();

    return (HiveConf.getBoolVar(conf, ConfVars.HIVE_TXN_READONLY_ENABLED) &&
      tree.getToken().getType() == HiveParser.TOK_QUERY &&
      Stream.of(
        new int[]{HiveParser.TOK_INSERT_INTO},
        new int[]{HiveParser.TOK_INSERT, HiveParser.TOK_TAB})
        .noneMatch(pattern ->
            astSearcher.simpleBreadthFirstSearch(tree, pattern) != null)) ?
      TxnType.READ_ONLY : TxnType.DEFAULT;
  }
}
