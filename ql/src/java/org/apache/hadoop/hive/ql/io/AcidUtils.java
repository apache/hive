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

import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_PATH_SUFFIX;
import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_TABLE;
import static org.apache.hadoop.hive.common.FileUtils.HIDDEN_FILES_PATH_FILTER;
import static org.apache.hadoop.hive.metastore.TransactionalValidationListener.INSERTONLY_TRANSACTIONAL_PROPERTY;
import static org.apache.hadoop.hive.metastore.TransactionalValidationListener.DEFAULT_TRANSACTIONAL_PROPERTY;
import static org.apache.hadoop.hive.ql.exec.Utilities.COPY_KEYWORD;

import static org.apache.hadoop.hive.ql.parse.CalcitePlanner.ASTSearcher;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.FileUtils;

import com.google.common.base.Strings;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.common.AcidConstants;
import org.apache.hadoop.hive.common.AcidMetaDataFile;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.TransactionalValidationListener;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionState;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.table.create.CreateTableDesc;
import org.apache.hadoop.hive.ql.ddl.view.create.CreateMaterializedViewDesc;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.io.AcidInputFormat.DeltaFileMetaData;
import org.apache.hadoop.hive.ql.io.HdfsUtils.HdfsFileStatusWithoutId;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcRecordUpdater;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.concurrent.Immutable;
import java.nio.charset.Charset;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Utilities that are shared by all of the ACID input and output formats. They
 * are used by the compactor and cleaner and thus must be format agnostic.
 */
public class AcidUtils {
  // This key will be put in the conf file when planning an acid operation
  public static final String CONF_ACID_KEY = "hive.doing.acid";
  public static final String BASE_PREFIX = AcidConstants.BASE_PREFIX;
  public static final String COMPACTOR_TABLE_PROPERTY = "compactiontable";
  public static final PathFilter baseFileFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return path.getName().startsWith(BASE_PREFIX);
    }
  };
  public static final String DELTA_PREFIX = AcidConstants.DELTA_PREFIX;
  public static final String DELETE_DELTA_PREFIX = AcidConstants.DELETE_DELTA_PREFIX;
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
  public static final String BUCKET_DIGITS = AcidConstants.BUCKET_DIGITS;
  public static final String LEGACY_FILE_BUCKET_DIGITS = AcidConstants.LEGACY_FILE_BUCKET_DIGITS;
  public static final String DELTA_DIGITS = AcidConstants.DELTA_DIGITS;
  /**
   * 10K statements per tx.  Probably overkill ... since that many delta files
   * would not be good for performance
   */
  public static final String STATEMENT_DIGITS = AcidConstants.STATEMENT_DIGITS;
  /**
   * This must be in sync with {@link #STATEMENT_DIGITS}
   */
  public static final int MAX_STATEMENTS_PER_TXN = 10000;
  public static final Pattern LEGACY_BUCKET_DIGIT_PATTERN = Pattern.compile("^[0-9]{6}");
  public static final Pattern BUCKET_PATTERN = Pattern.compile("bucket_([0-9]+)(_[0-9]+)?$");
  private static final Set<Integer> READ_TXN_TOKENS = new HashSet<>();

  private static Cache<String, DirInfoValue> dirCache;
  private static AtomicBoolean dirCacheInited = new AtomicBoolean();

  static {
    READ_TXN_TOKENS.addAll(Arrays.asList(
      HiveParser.TOK_DESCDATABASE,
      HiveParser.TOK_DESCTABLE,
      HiveParser.TOK_EXPLAIN,
      HiveParser.TOK_EXPLAIN_SQ_REWRITE
    ));
  }

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

  public static final Pattern ORIGINAL_PATTERN =
      Pattern.compile("[0-9]+_[0-9]+");
  /**
   * @see org.apache.hadoop.hive.ql.exec.Utilities#COPY_KEYWORD
   */
  public static final Pattern ORIGINAL_PATTERN_COPY =
    Pattern.compile("[0-9]+_[0-9]+" + COPY_KEYWORD + "[0-9]+");

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
      return HIDDEN_FILES_PATH_FILTER.accept(p);
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

  public static final String VISIBILITY_PREFIX = AcidConstants.VISIBILITY_PREFIX;
  public static final Pattern VISIBILITY_PATTERN = AcidConstants.VISIBILITY_PATTERN;

  private static final HadoopShims SHIMS = ShimLoader.getHadoopShims();

  /**
   * Create the bucket filename in Acid format
   * @param subdir the subdirectory for the bucket.
   * @param bucket the bucket number
   * @return the filename
   */
  public static Path createBucketFile(Path subdir, int bucket) {
    return createBucketFile(subdir, bucket, null, true);
  }

  public static Path createBucketFile(Path subdir, int bucket, Integer attemptId) {
    return createBucketFile(subdir, bucket, attemptId, true);
  }

  /**
   * Create acid or original bucket name
   * @param subdir the subdirectory for the bucket.
   * @param bucket the bucket number
   * @return the filename
   */
  private static Path createBucketFile(Path subdir, int bucket, Integer attemptId, boolean isAcidSchema) {
    if(isAcidSchema) {
      String fileName = BUCKET_PREFIX + String.format(BUCKET_DIGITS, bucket);
      if (attemptId != null) {
        fileName = fileName + "_" + attemptId;
      }
      return new Path(subdir, fileName);
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
    return AcidConstants.baseDir(writeId);
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
      return createBucketFile(baseOrDeltaSubdirPath(directory, options), options.getBucketId(), options.getAttemptId());
    }
  }

  /**
   * Since Hive 4.0, compactor produces directories with {@link #VISIBILITY_PATTERN} suffix.
   * _v0 is equivalent to no suffix, for backwards compatibility.
   */
  public static String addVisibilitySuffix(String baseOrDeltaDir, long visibilityTxnId) {
    if(visibilityTxnId == 0) {
      return baseOrDeltaDir;
    }
    return baseOrDeltaDir + VISIBILITY_PREFIX
        + String.format(DELTA_DIGITS, visibilityTxnId);
  }

  public static boolean isLocklessReadsEnabled(Table table, HiveConf conf) {
    return HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED)
        && AcidUtils.isTransactionalTable(table);
  }

  public static boolean isTableSoftDeleteEnabled(Table table, HiveConf conf) {
    boolean isSoftDelete = HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_CREATE_TABLE_USE_SUFFIX)
      || HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED);
    return isSoftDelete && AcidUtils.isTransactionalTable(table)
      && Boolean.parseBoolean(table.getProperty(SOFT_DELETE_TABLE));
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
   * Determine if a table is used during query based compaction.
   * @param tblProperties table properties
   * @return true, if the tblProperties contains {@link AcidUtils#COMPACTOR_TABLE_PROPERTY}
   */
  public static boolean isCompactionTable(Properties tblProperties) {
    return tblProperties != null && isCompactionTable(Maps.fromProperties(tblProperties));
  }

  /**
   * Determine if a table is used during query based compaction.
   * @param parameters table properties map
   * @return true, if the parameters contains {@link AcidUtils#COMPACTOR_TABLE_PROPERTY}
   */
  public static boolean isCompactionTable(Map<String, String> parameters) {
    return StringUtils.isNotBlank(parameters.get(COMPACTOR_TABLE_PROPERTY));
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
      Matcher matcher = BUCKET_PATTERN.matcher(filename);
      if (matcher.matches()) {
        String bucketId = matcher.group(1);
        filename = filename.substring(0,matcher.end(1));
        if (Utilities.FILE_OP_LOGGER.isDebugEnabled()) {
          Utilities.FILE_OP_LOGGER.debug("Parsing bucket ID = " + bucketId + " from file name '" + filename + "'");
        }
        return Integer.parseInt(bucketId);
      }
    }
    return -1;
  }

  public static Integer parseAttemptId(Path bucketFile) {
    String filename = bucketFile.getName();
    Matcher matcher = BUCKET_PATTERN.matcher(filename);
    Integer attemptId = null;
    if (matcher.matches()) {
      attemptId = matcher.group(2) != null ? Integer.valueOf(matcher.group(2).substring(1)) : null;
    }
    if (Utilities.FILE_OP_LOGGER.isDebugEnabled()) {
      Utilities.FILE_OP_LOGGER.debug("Parsing attempt ID = " + attemptId + " from file name '" + bucketFile + "'");
    }
    return attemptId;
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
                                                   Configuration conf) {
    AcidOutputFormat.Options result = new AcidOutputFormat.Options(conf);
    String filename = bucketFile.getName();
    int bucket = parseBucketId(bucketFile);
    Integer attemptId = parseAttemptId(bucketFile);
    if (ORIGINAL_PATTERN.matcher(filename).matches() || ORIGINAL_PATTERN_COPY.matcher(filename).matches()) {
      long minWriteId = 0;
      long maxWriteId = 0;
      int statementId = -1;
      if (bucketFile.getParent().getName().startsWith(DELTA_PREFIX)) {
        ParsedDeltaLight parsedDelta = ParsedDeltaLight.parse(bucketFile.getParent());
        minWriteId = parsedDelta.getMinWriteId();
        maxWriteId = parsedDelta.getMaxWriteId();
        statementId = parsedDelta.getStatementId();
      }
      result
          .setOldStyle(true)
          .minimumWriteId(minWriteId)
          .maximumWriteId(maxWriteId)
          .statementId(statementId)
          .bucket(bucket)
          .writingBase(!bucketFile.getParent().getName().startsWith(DELTA_PREFIX));
    }
    else if (filename.startsWith(BUCKET_PREFIX)) {
      if (bucketFile.getParent().getName().startsWith(BASE_PREFIX)) {
        result
            .setOldStyle(false)
            .minimumWriteId(0)
            .maximumWriteId(ParsedBaseLight.parseBase(bucketFile.getParent()).getWriteId())
            .bucket(bucket)
            .writingBase(true);
      } else if (bucketFile.getParent().getName().startsWith(DELTA_PREFIX)) {
        ParsedDeltaLight parsedDelta = ParsedDeltaLight.parse(bucketFile.getParent());
        result
            .setOldStyle(false)
            .minimumWriteId(parsedDelta.minWriteId)
            .maximumWriteId(parsedDelta.maxWriteId)
            .statementId(parsedDelta.statementId)
            .bucket(bucket)
            .attemptId(attemptId);
      } else if (bucketFile.getParent().getName().startsWith(DELETE_DELTA_PREFIX)) {
        ParsedDeltaLight parsedDelta = ParsedDeltaLight.parse(bucketFile.getParent());
        result
            .setOldStyle(false)
            .minimumWriteId(parsedDelta.minWriteId)
            .maximumWriteId(parsedDelta.maxWriteId)
            .statementId(parsedDelta.statementId)
            .bucket(bucket);
      }
    } else {
      result.setOldStyle(true).bucket(bucket).minimumWriteId(0)
          .maximumWriteId(0);
    }
    return result;
  }

  /**
   * If the direct insert is on for ACID tables, the files will contain an "_attemptID" postfix.
   * In order to be able to read the files from the delete deltas, we need to know which
   * attemptId belongs to which delta. To make this lookup easy, this method created a map
   * to link the deltas to the attemptId.
   * @param pathToDeltaMetaData
   * @param deleteDeltaDirs
   * @param bucket
   * @return
   */
  public static Map<String, Integer> getDeltaToAttemptIdMap(
      Map<String, AcidInputFormat.DeltaMetaData> pathToDeltaMetaData, Path[] deleteDeltaDirs, int bucket) {
    Map<String, Integer> deltaToAttemptId = new HashMap<>();
    for (Path delta : deleteDeltaDirs) {
      AcidInputFormat.DeltaMetaData deltaMetaData = pathToDeltaMetaData.get(delta.getName());
      for (DeltaFileMetaData files : deltaMetaData.getDeltaFiles()) {
        if (bucket == files.getBucketId()) {
          deltaToAttemptId.put(delta.getName(), files.getAttemptId());
          break;
        }
      }
    }
    return deltaToAttemptId;
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
  public static class FileInfo {
    final private HdfsFileStatusWithId fileId;
    final private AcidBaseFileType acidBaseFileType;

    public FileInfo(HdfsFileStatusWithId fileId, AcidBaseFileType acidBaseFileType) {
      this.fileId = fileId;
      this.acidBaseFileType = acidBaseFileType;
    }

    public boolean isOriginal() {
      return this.acidBaseFileType == AcidBaseFileType.ORIGINAL_BASE;
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
    public static final int INSERT_ONLY_FETCH_BUCKET_ID_BIT = 0x08;
    public static final int FETCH_DELETED_ROWS_BIT = 0x10;
    public static final String INSERT_ONLY_STRING = "insert_only";
    public static final String INSERT_ONLY_FETCH_BUCKET_ID_STRING = "insert_only_fetch_bucket_id";
    public static final String FETCH_DELETED_ROWS_STRING = "fetch_deleted_rows";
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
      if ((properties & INSERT_ONLY_FETCH_BUCKET_ID_BIT) > 0) {
        obj.setInsertOnlyFetchBucketId(true);
      }
      if ((properties & FETCH_DELETED_ROWS_BIT) > 0) {
        obj.setFetchDeletedRows(true);
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
      return set(isSplitUpdate, SPLIT_UPDATE_BIT);
    }

    /**
     * Sets the hash-based merge property for ACID operations that combines delta files using
     * GRACE hash join based approach, when turned on. (Currently unimplemented!)
     * @param isHashBasedMerge a boolean property that turns on hash-based merge when true.
     * @return the acidOperationalProperties object.
     */
    public AcidOperationalProperties setHashBasedMerge(boolean isHashBasedMerge) {
      return set(isHashBasedMerge, HASH_BASED_MERGE_BIT);
    }

    public AcidOperationalProperties setInsertOnly(boolean isInsertOnly) {
      return set(isInsertOnly, INSERT_ONLY_BIT);
    }

    public AcidOperationalProperties setInsertOnlyFetchBucketId(boolean fetchBucketId) {
      return set(fetchBucketId, INSERT_ONLY_FETCH_BUCKET_ID_BIT);
    }

    public AcidOperationalProperties setFetchDeletedRows(boolean fetchDeletedRows) {
      return set(fetchDeletedRows, FETCH_DELETED_ROWS_BIT);
    }

    private AcidOperationalProperties set(boolean value, int bit) {
      description = (value ? (description | bit) : (description & ~bit));
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

    public boolean isFetchBucketId() {
      return (description & INSERT_ONLY_FETCH_BUCKET_ID_BIT) > 0;
    }

    public boolean isFetchDeletedRows() {
      return (description & FETCH_DELETED_ROWS_BIT) > 0;
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
      if (isFetchBucketId()) {
        str.append("|" + INSERT_ONLY_FETCH_BUCKET_ID_STRING);
      }
      if (isFetchBucketId()) {
        str.append("|" + FETCH_DELETED_ROWS_STRING);
      }
      return str.toString();
    }
  }

  public interface Directory {
    List<FileInfo> getFiles() throws IOException;
    FileSystem getFs();
    Path getPath();
    List<ParsedDelta> getDeleteDeltas();
  }

  public interface ParsedDirectory {
    public List<HdfsFileStatusWithId> getFiles(FileSystem fs, Ref<Boolean> useFileIds) throws IOException;
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
  public static class ParsedBaseLight {
    protected final long writeId;
    protected final long visibilityTxnId;
    protected final Path baseDirPath;

    ParsedBaseLight(long writeId, Path baseDirPath) {
      this(writeId, 0, baseDirPath);
    }
    ParsedBaseLight(long writeId, long visibilityTxnId, Path baseDirPath) {
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

    public static ParsedBaseLight parseBase(Path path) {
      String filename = path.getName();
      if (!filename.startsWith(BASE_PREFIX)) {
        throw new IllegalArgumentException(filename + " does not start with " + BASE_PREFIX);
      }
      int idxOfv = filename.indexOf(VISIBILITY_PREFIX);
      if (idxOfv < 0) {
        return new ParsedBaseLight(Long.parseLong(filename.substring(BASE_PREFIX.length())), path);
      }
      return new ParsedBaseLight(Long.parseLong(filename.substring(BASE_PREFIX.length(), idxOfv)),
          Long.parseLong(filename.substring(idxOfv + VISIBILITY_PREFIX.length())), path);
    }

    @Override
    public String toString() {
      return "Path: " + baseDirPath + "; writeId: " + writeId + "; visibilityTxnId: " + visibilityTxnId;
    }
  }
  /**
   * In addition to {@link ParsedBaseLight} this knows if the data is in raw format, i.e. doesn't
   * have acid metadata columns embedded in the files.  To determine this in some cases
   * requires looking at the footer of the data file which can be expensive so if this info is
   * not needed {@link ParsedBaseLight} should be used.
   */
  public static final class ParsedBase extends ParsedBaseLight implements ParsedDirectory {

    private boolean rawFormat;
    private List<HdfsFileStatusWithId> files;

    ParsedBase(ParsedBaseLight pb, List<HdfsFileStatusWithId> files) {
      super(pb.writeId, pb.visibilityTxnId, pb.baseDirPath);
      this.files = files;
    }

    public boolean isRawFormat() {
      return rawFormat;
    }

    public void setRawFormat(boolean rawFormat) {
      this.rawFormat = rawFormat;
    }

    /**
     * Returns the files from the base directory.
     * The list is either populated by AcidUtils or it will be listed through the provided FileSystem object.
     * If the list was not filled and no FS is provided, returns null.
     * @param fs FileSystem optional
     * @param useFileIds to use fileId based listing or not. Optional
     * @return list of files in the base directory
     * @throws IOException ex
     */
    public List<HdfsFileStatusWithId> getFiles(FileSystem fs, Ref<Boolean> useFileIds) throws IOException {
      // If the list was not populated before, do it now
      if (files == null && fs != null) {
        files = HdfsUtils.listFileStatusWithId(fs, baseDirPath, useFileIds, false, HIDDEN_FILES_PATH_FILTER);
      }
      return files;
    }

    public void setFiles(List<HdfsFileStatusWithId> files) {
      this.files = files;
    }
    @Override
    public String toString() {
      return super.toString() + "; rawFormat: " + rawFormat;
    }
  }

  /**
   * In addition to {@link ParsedDeltaLight} this knows if the data is in raw format, i.e. doesn't
   * have acid metadata columns embedded in the files.  To determine this in some cases
   * requires looking at the footer of the data file which can be expensive so if this info is
   * not needed {@link ParsedDeltaLight} should be used.
   */
  @Immutable
  public static final class ParsedDelta extends ParsedDeltaLight implements ParsedDirectory {
    private final boolean isRawFormat;
    private List<HdfsFileStatusWithId> files;

    private ParsedDelta(ParsedDeltaLight delta, boolean isRawFormat, List<HdfsFileStatusWithId> files) {
      super(delta.minWriteId, delta.maxWriteId, delta.path, delta.statementId, delta.isDeleteDelta, delta.visibilityTxnId);
      this.isRawFormat = isRawFormat;
      this.files = files;
    }
    /**
     * Files w/o Acid meta columns embedded in the file. See {@link AcidBaseFileType#ORIGINAL_BASE}
     */
    public boolean isRawFormat() {
      return isRawFormat;
    }

    /**
     * Returns the files from the delta directory.
     * The list is either populated by AcidUtils or it will be listed through the provided FileSystem object.
     * If the list was not filled and no FS is provided, returns null.
     * @param fs FileSystem optional
     * @param useFileIds to use fileId based listing or not. Optional
     * @return list of files in the delta directory
     * @throws IOException ex
     */
    public List<HdfsFileStatusWithId> getFiles(FileSystem fs, Ref<Boolean> useFileIds) throws IOException {
      // If the list was not populated before, do it now
      if (files == null && fs != null) {
        files = HdfsUtils.listFileStatusWithId(fs, path, useFileIds, false, isRawFormat() ? AcidUtils.originalBucketFilter : AcidUtils.bucketFileFilter);
      }
      return files;
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
     * use in {@link ParsedBaseLight}, {@link ParsedDelta}, {@link AcidInputFormat.Options}, AcidInputFormat.DeltaMetaData etc
     */
    final long visibilityTxnId;

    public static ParsedDeltaLight parse(Path deltaDir) {
      String filename = deltaDir.getName();
      int idxOfVis = filename.indexOf(VISIBILITY_PREFIX);
      long visibilityTxnId = 0; // visibilityTxnId:0 is always visible
      if (idxOfVis >= 0) {
        visibilityTxnId = Long.parseLong(filename.substring(idxOfVis + VISIBILITY_PREFIX.length()));
        filename = filename.substring(0, idxOfVis);
      }
      boolean isDeleteDelta = filename.startsWith(DELETE_DELTA_PREFIX);
      String rest = filename.substring((isDeleteDelta ? DELETE_DELTA_PREFIX : DELTA_PREFIX).length());
      int split = rest.indexOf('_');
      // split2 may be -1 if no statementId
      int split2 = rest.indexOf('_', split + 1);
      long min = Long.parseLong(rest.substring(0, split));
      long max =
          split2 == -1 ? Long.parseLong(rest.substring(split + 1)) : Long.parseLong(rest.substring(split + 1, split2));
      if (split2 == -1) {
        // pre 1.3.x delta files
        return new ParsedDeltaLight(min, max, deltaDir, -1, isDeleteDelta, visibilityTxnId);
      }
      int statementId = Integer.parseInt(rest.substring(split2 + 1));
      return new ParsedDeltaLight(min, max, deltaDir, statementId, isDeleteDelta, visibilityTxnId);
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

    public boolean hasStatementId() {
      return statementId >= 0;
    }

    public int getStatementId() {
      return hasStatementId() ? statementId : 0;
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
         * in a {@link AcidDirectory}
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
   * write id pairs.  Assumes {@code deltas} is sorted.
   * @param deltas sorted delete delta list
   * @param fs FileSystem
   * @return the list of write ids to serialize
   * @throws IOException ex
   */
  public static List<AcidInputFormat.DeltaMetaData> serializeDeleteDeltas(List<ParsedDelta> deltas, FileSystem fs) throws IOException {
    List<AcidInputFormat.DeltaMetaData> result = new ArrayList<>(deltas.size());
    AcidInputFormat.DeltaMetaData last = null;
    for (ParsedDelta parsedDelta : deltas) {
      assert parsedDelta.isDeleteDelta() : "expected delete_delta, got " + parsedDelta.getPath();
      final Integer stmtId = parsedDelta.statementId >= 0 ? parsedDelta.statementId : null;
      if ((last != null)
              && (last.getMinWriteId() == parsedDelta.getMinWriteId())
              && (last.getMaxWriteId() == parsedDelta.getMaxWriteId())) {
        if (stmtId != null) {
          last.getStmtIds().add(stmtId);
        }
        for (HadoopShims.HdfsFileStatusWithId fileStatus : parsedDelta.getFiles(fs, null)) {
          last.getDeltaFiles().add(new AcidInputFormat.DeltaFileMetaData(fileStatus, stmtId, parseBucketId(fileStatus.getFileStatus().getPath())));
        }
      } else {
        List<Integer> stmtIds = new ArrayList<>();
        if (stmtId != null) {
          stmtIds.add(stmtId);
        }
        last = new AcidInputFormat.DeltaMetaData(parsedDelta.getMinWriteId(), parsedDelta.getMaxWriteId(),
            stmtIds, parsedDelta.getVisibilityTxnId(), parsedDelta.getFiles(fs, null).stream()
            .map(file -> new AcidInputFormat.DeltaFileMetaData(file, stmtId, parseBucketId(file.getFileStatus().getPath())))
            .collect(Collectors.toList()));
        result.add(last);
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
  public static Path[] deserializeDeleteDeltas(Path root, final List<AcidInputFormat.DeltaMetaData> deleteDeltas,
      Map<String, AcidInputFormat.DeltaMetaData> pathToDeltaMetaData) {
    List<Path> results = new ArrayList<>(deleteDeltas.size());
    for (AcidInputFormat.DeltaMetaData dmd : deleteDeltas) {
      results.addAll(dmd.getPaths(root).stream().map(Pair::getLeft).collect(Collectors.toList()));
      if (pathToDeltaMetaData != null) {
        for (Pair<Path, Integer> pathPair : dmd.getPaths(root)) {
          pathToDeltaMetaData.put(pathPair.getLeft().getName(), dmd);
        }
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
    return parsedDelta(deltaDir, fs, null);
  }

  private static ParsedDelta parsedDelta(Path deltaDir, FileSystem fs, HdfsDirSnapshot dirSnapshot)
      throws IOException {
    ParsedDeltaLight deltaLight = ParsedDeltaLight.parse(deltaDir);
    //small optimization - delete delta can't be in raw format
    boolean isRawFormat = !deltaLight.isDeleteDelta && MetaDataFile.isRawFormat(deltaDir, fs, dirSnapshot);
    List<HdfsFileStatusWithId> files = null;
    if (dirSnapshot != null) {
      final PathFilter filter = isRawFormat ? AcidUtils.originalBucketFilter : AcidUtils.bucketFileFilter;
      // If we already know the files, store it for future use
      files = dirSnapshot.getFiles().stream()
          .filter(fileStatus -> filter.accept(fileStatus.getPath()))
          .map(HdfsFileStatusWithoutId::new)
          .collect(Collectors.toList());
    }
    return new ParsedDelta(deltaLight, isRawFormat, files);
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
        if (file.isDirectory()) {
          return true;
        }
      }
    }
    return false;
  }


  /**
   * Get the ACID state of the given directory. It finds the minimal set of
   * base and diff directories. Note that because major compactions don't
   * preserve the history, we can't use a base directory that includes a
   * write id that we must exclude.
   * @param fileSystem optional, it it is not provided, it will be derived from the candidateDirectory
   * @param candidateDirectory the partition directory to analyze
   * @param conf the configuration
   * @param writeIdList the list of write ids that we are reading
   * @param useFileIds It will be set to true, if the FileSystem supports listing with fileIds
   * @param ignoreEmptyFiles Ignore files with 0 length
   * @return the state of the directory
   * @throws IOException on filesystem errors
   */
  public static AcidDirectory getAcidState(FileSystem fileSystem, Path candidateDirectory, Configuration conf,
      ValidWriteIdList writeIdList, Ref<Boolean> useFileIds, boolean ignoreEmptyFiles) throws IOException {
    return getAcidState(fileSystem, candidateDirectory, conf, writeIdList, useFileIds, ignoreEmptyFiles, null);
  }

  /**
   * GetAcidState implementation which uses the provided dirSnapshot.
   * Generates a new one if needed and the provided one is null.
   * @param fileSystem optional, it it is not provided, it will be derived from the candidateDirectory
   * @param candidateDirectory the partition directory to analyze
   * @param conf the configuration
   * @param writeIdList the list of write ids that we are reading
   * @param useFileIds It will be set to true, if the FileSystem supports listing with fileIds
   * @param ignoreEmptyFiles Ignore files with 0 length
   * @param dirSnapshots The listed directory snapshot, if null new will be generated
   * @return the state of the directory
   * @throws IOException on filesystem errors
   */
  public static AcidDirectory getAcidState(FileSystem fileSystem, Path candidateDirectory, Configuration conf,
      ValidWriteIdList writeIdList, Ref<Boolean> useFileIds, boolean ignoreEmptyFiles, Map<Path,
      HdfsDirSnapshot> dirSnapshots) throws IOException {
    ValidTxnList validTxnList = getValidTxnList(conf);

    FileSystem fs = fileSystem == null ? candidateDirectory.getFileSystem(conf) : fileSystem;
    AcidDirectory directory = new AcidDirectory(candidateDirectory, fs, useFileIds);

    List<HdfsFileStatusWithId> childrenWithId = HdfsUtils.tryListLocatedHdfsStatus(useFileIds, fs, candidateDirectory, HIDDEN_FILES_PATH_FILTER);

    if (childrenWithId != null) {
      for (HdfsFileStatusWithId child : childrenWithId) {
        getChildState(directory, child, writeIdList,validTxnList, ignoreEmptyFiles);
      }
    } else {
      if (dirSnapshots == null) {
        dirSnapshots = getHdfsDirSnapshots(fs, candidateDirectory);
      }
      getChildState(directory, dirSnapshots, writeIdList, validTxnList, ignoreEmptyFiles);
    }
    // If we have a base, the original files are obsolete.
    if (directory.getBase() != null) {
      // Add original files to obsolete list if any
      for (HdfsFileStatusWithId fswid : directory.getOriginalFiles()) {
        directory.getObsolete().add(fswid.getFileStatus().getPath());
      }
      // Add original directories to obsolete list if any
      directory.getObsolete().addAll(directory.getOriginalDirectories());
      // remove the entries so we don't get confused later and think we should
      // use them.
      directory.getOriginalFiles().clear();
      directory.getOriginalDirectories().clear();
    } else {
      // Okay, we're going to need these originals.
      // Recurse through them and figure out what we really need.
      // If we already have the original list, do nothing
      // If childrenWithId != null, we would have already populated "original"
      if (childrenWithId != null) {
        for (Path origDir : directory.getOriginalDirectories()) {
          directory.getOriginalFiles().addAll(HdfsUtils.listFileStatusWithId(fs, origDir, useFileIds, true, null));
        }
      }
    }
    // Filter out all delta directories that are shadowed by others
    findBestWorkingDeltas(writeIdList, directory);

    if(directory.getOldestBase() != null && directory.getBase() == null &&
        isCompactedBase(directory.getOldestBase(), fs, dirSnapshots)) {
      /*
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
              minOpenWriteId, directory.getOldestBase().toString()));
    }

    Path basePath = directory.getBaseDirectory();
    if (basePath != null) {
      boolean isBaseInRawFormat = MetaDataFile.isRawFormat(basePath, fs, dirSnapshots != null ? dirSnapshots.get(basePath) : null);
      directory.getBase().setRawFormat(isBaseInRawFormat);
    }
    LOG.debug("in directory " + candidateDirectory.toUri().toString() + " base = " + basePath + " deltas = " +
        directory.getCurrentDirectories().size());
    /*
     * If this sort order is changed and there are tables that have been converted to transactional
     * and have had any update/delete/merge operations performed but not yet MAJOR compacted, it
     * may result in data loss since it may change how
     * {@link org.apache.hadoop.hive.ql.io.orc.OrcRawRecordMerger.OriginalReaderPair} assigns
     * {@link RecordIdentifier#rowId} for read (that have happened) and compaction (yet to happen).
     */
    // this does "Path.uri.compareTo(that.uri)"
    directory.getOriginalFiles().sort(Comparator.comparing(HdfsFileStatusWithId::getFileStatus));
    return directory;
  }

  public static AcidDirectory getAcidState(StorageDescriptor sd, ValidWriteIdList writeIds, HiveConf conf)
      throws IOException {
    Path location = new Path(sd.getLocation());
    FileSystem fs = location.getFileSystem(conf);
    return getAcidState(fs, location, conf, writeIds, Ref.from(false), false);
  }

  private static void findBestWorkingDeltas(ValidWriteIdList writeIdList, AcidDirectory directory) {
    Collections.sort(directory.getCurrentDirectories());
    //so now, 'current directories' should be sorted like delta_5_20 delta_5_10 delta_11_20 delta_51_60 for example
    //and we want to end up with the best set containing all relevant data: delta_5_20 delta_51_60,
    //subject to list of 'exceptions' in 'writeIdList' (not show in above example).
    List<ParsedDelta> deltas = new ArrayList<>();
    long current = directory.getBase() == null ? 0 : directory.getBase().getWriteId();
    int lastStmtId = -1;
    ParsedDelta prev = null;
    for(ParsedDelta next: directory.getCurrentDirectories()) {
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
        directory.getObsolete().add(next.path);
      }
    }
    directory.getCurrentDirectories().clear();
    directory.getCurrentDirectories().addAll(deltas);
  }

  private static ValidTxnList getValidTxnList(Configuration conf) {
    ValidTxnList validTxnList = null;
    String s = conf.get(ValidTxnList.VALID_TXNS_KEY);
    if(!Strings.isNullOrEmpty(s)) {
      /*
       * getAcidState() is sometimes called on non-transactional tables, e.g.
       * OrcInputFileFormat.FileGenerator.callInternal().  e.g. orc_merge3.q In that case
       * writeIdList is bogus - doesn't even have a table name.
       * see https://issues.apache.org/jira/browse/HIVE-20856.
       *
       * For now, assert that ValidTxnList.VALID_TXNS_KEY is set only if this is really a read
       * of a transactional table.
       */
      validTxnList = new ValidReadTxnList();
      validTxnList.readFromString(s);
    }
    return validTxnList;
  }


  /**
   * In case of the cleaner, we don't need to go into file level, it is enough to collect base/delta/deletedelta directories.
   *
   * @param fs the filesystem used for the directory lookup
   * @param path the path of the table or partition needs to be cleaned
   * @return The listed directory snapshot needs to be checked for cleaning
   * @throws IOException on filesystem errors
   */
  public static Map<Path, HdfsDirSnapshot> getHdfsDirSnapshotsForCleaner(final FileSystem fs, final Path path)
          throws IOException {
    Map<Path, HdfsDirSnapshot> dirToSnapshots = new HashMap<>();
    Deque<RemoteIterator<FileStatus>> stack = new ArrayDeque<>();
    stack.push(FileUtils.listStatusIterator(fs, path, acidHiddenFileFilter));
    while (!stack.isEmpty()) {
      RemoteIterator<FileStatus> itr = stack.pop();
      while (itr.hasNext()) {
        FileStatus fStatus = itr.next();
        Path fPath = fStatus.getPath();
        if (baseFileFilter.accept(fPath) ||
                deltaFileFilter.accept(fPath) ||
                deleteEventDeltaDirFilter.accept(fPath)) {
          addToSnapshot(dirToSnapshots, fPath);
        } else {
          if (fStatus.isDirectory()) {
            stack.push(FileUtils.listStatusIterator(fs, fPath, acidHiddenFileFilter));
          } else {
            // Found an original file
            HdfsDirSnapshot hdfsDirSnapshot = addToSnapshot(dirToSnapshots, fPath.getParent());
            hdfsDirSnapshot.addFile(fStatus);
          }
        }
      }
    }
    return dirToSnapshots;
  }

  private static HdfsDirSnapshot addToSnapshot(Map<Path, HdfsDirSnapshot> dirToSnapshots, Path fPath) {
    HdfsDirSnapshot dirSnapshot = dirToSnapshots.get(fPath);
    if (dirSnapshot == null) {
      dirSnapshot = new HdfsDirSnapshotImpl(fPath);
      dirToSnapshots.put(fPath, dirSnapshot);
    }
    return dirSnapshot;
  }

  public static Map<Path, HdfsDirSnapshot> getHdfsDirSnapshots(final FileSystem fs, final Path path)
      throws IOException {
    Map<Path, HdfsDirSnapshot> dirToSnapshots = new HashMap<>();
    Deque<RemoteIterator<FileStatus>> stack = new ArrayDeque<>();
    stack.push(FileUtils.listStatusIterator(fs, path, acidHiddenFileFilter));
    while (!stack.isEmpty()) {
      RemoteIterator<FileStatus> itr = stack.pop();
      while (itr.hasNext()) {
        FileStatus fStatus = itr.next();
        Path fPath = fStatus.getPath();
        if (fStatus.isDirectory()) {
          stack.push(FileUtils.listStatusIterator(fs, fPath, acidHiddenFileFilter));
        } else {
          Path parentDirPath = fPath.getParent();
          if (acidTempDirFilter.accept(parentDirPath)) {
            while (isChildOfDelta(parentDirPath, path)) {
              // Some cases there are other directory layers between the delta and the datafiles
              // (export-import mm table, insert with union all to mm table, skewed tables).
              // But it does not matter for the AcidState, we just need the deltas and the data files
              // So build the snapshot with the files inside the delta directory
              parentDirPath = parentDirPath.getParent();
            }
            HdfsDirSnapshot dirSnapshot = addToSnapshot(dirToSnapshots, parentDirPath);
            // We're not filtering out the metadata file and acid format file,
            // as they represent parts of a valid snapshot
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
  }

  public static boolean isChildOfDelta(Path childDir, Path rootPath) {
    if (childDir.toUri().toString().length() <= rootPath.toUri().toString().length()) {
      return false;
    }
    // We do not want to look outside the original directory
    String fullName = childDir.toUri().toString().substring(rootPath.toUri().toString().length() + 1);
    String dirName = childDir.getName();
    return !dirName.startsWith(BASE_PREFIX) && !dirName.startsWith(DELTA_PREFIX) && !dirName.startsWith(DELETE_DELTA_PREFIX)
          && (fullName.contains(BASE_PREFIX) || fullName.contains(DELTA_PREFIX) || fullName.contains(DELETE_DELTA_PREFIX));
  }

  /**
   * DFS dir listing.
   * Captures a dir and the corresponding list of files it contains,
   * with additional properties about the dir (like isBase etc)
   *
   */
  public interface HdfsDirSnapshot {
    public Path getPath();

    public void addOrcAcidFormatFile(FileStatus fStatus);

    public FileStatus getOrcAcidFormatFile();

    public void addMetadataFile(FileStatus fStatus);

    public FileStatus getMetadataFile();

    // Get the list of files if any within this directory
    public List<FileStatus> getFiles();

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
    private FileStatus metadataFStatus = null;
    private FileStatus orcAcidFormatFStatus = null;
    private List<FileStatus> files = new ArrayList<FileStatus>();
    private Long fileId = null;
    private Boolean isRawFormat = null;
    private Boolean isBase = null;
    private Boolean isValidBase = null;
    private Boolean isCompactedBase = null;

    public HdfsDirSnapshotImpl(Path path, List<FileStatus> files) {
      this.dirPath = path;
      this.files = files;
    }

    public HdfsDirSnapshotImpl(Path path) {
      this.dirPath = path;
    }

    @Override
    public Path getPath() {
      return dirPath;
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
    public FileStatus getMetadataFile() {
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
      for (FileStatus fStatus : files) {
        sb.append(fStatus);
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
  private static boolean isValidBase(ParsedBaseLight parsedBase, ValidWriteIdList writeIdList, FileSystem fs,
      HdfsDirSnapshot dirSnapshot) throws IOException {
    boolean isValidBase;
    if (dirSnapshot != null && dirSnapshot.isValidBase() != null) {
      isValidBase = dirSnapshot.isValidBase();
    } else {
      if (parsedBase.getWriteId() == Long.MIN_VALUE) {
        //such base is created by 1st compaction in case of non-acid to acid table conversion
        //By definition there are no open txns with id < 1.
        isValidBase = true;
      } else if (writeIdList.getMinOpenWriteId() != null && parsedBase.getWriteId() <= writeIdList
          .getMinOpenWriteId()) {
        isValidBase = true;
      } else if (isCompactedBase(parsedBase, fs, dirSnapshot)) {
        isValidBase = writeIdList.isValidBase(parsedBase.getWriteId());
      } else {
        // if here, it's a result of IOW
        isValidBase = writeIdList.isWriteIdValid(parsedBase.getWriteId());
      }
      if (dirSnapshot != null) {
        dirSnapshot.setIsValidBase(isValidBase);
      }
    }
    return isValidBase;
  }

  /**
   * Returns {@code true} if {@code parsedBase} was created by compaction.
   * As of Hive 4.0 we can tell if a directory is a result of compaction based on the
   * presence of {@link AcidUtils#VISIBILITY_PATTERN} suffix.  Base directories written prior to
   * that, have to rely on the {@link MetaDataFile} in the directory. So look at the filename first
   * since that is the cheaper test.*/
  private static boolean isCompactedBase(ParsedBaseLight parsedBase, FileSystem fs,
      Map<Path, HdfsDirSnapshot> snapshotMap) throws IOException {
    return isCompactedBase(parsedBase, fs, snapshotMap != null ? snapshotMap.get(parsedBase.getBaseDirPath()) : null);
  }

  private static boolean isCompactedBase(ParsedBaseLight parsedBase, FileSystem fs,
      HdfsDirSnapshot snapshot) throws IOException {
    return parsedBase.getVisibilityTxnId() > 0 || MetaDataFile.isCompacted(parsedBase.getBaseDirPath(), fs, snapshot);
  }

  private static void getChildState(AcidDirectory directory, HdfsFileStatusWithId childWithId, ValidWriteIdList writeIdList,
      ValidTxnList validTxnList, boolean ignoreEmptyFiles) throws IOException {
    Path childPath = childWithId.getFileStatus().getPath();
    String fn = childPath.getName();
    if (!childWithId.getFileStatus().isDirectory()) {
      if (!ignoreEmptyFiles || childWithId.getFileStatus().getLen() != 0) {
        directory.getOriginalFiles().add(childWithId);
      }
    } else if (fn.startsWith(BASE_PREFIX)) {
      processBaseDir(childPath, writeIdList, validTxnList, directory, null);
    } else if (fn.startsWith(DELTA_PREFIX) || fn.startsWith(DELETE_DELTA_PREFIX)) {
      processDeltaDir(childPath, writeIdList, validTxnList, directory, null);
    } else {
      // This is just the directory.  We need to recurse and find the actual files.  But don't
      // do this until we have determined there is no base.  This saves time.  Plus,
      // it is possible that the cleaner is running and removing these original files,
      // in which case recursing through them could cause us to get an error.
      directory.getOriginalDirectories().add(childPath);
    }
  }

  private static void getChildState(AcidDirectory directory, Map<Path, HdfsDirSnapshot> dirSnapshots,
      ValidWriteIdList writeIdList, ValidTxnList validTxnList , boolean ignoreEmptyFiles) throws IOException {
    for (HdfsDirSnapshot dirSnapshot : dirSnapshots.values()) {
      Path dirPath = dirSnapshot.getPath();
      String dirName = dirPath.getName();
      // dirPath may contains the filesystem prefix
      if (dirPath.toString().endsWith(directory.getPath().toString())) {
        // if the candidateDirectory is itself a delta directory, we need to add originals in that directory
        // and return. This is the case when compaction thread calls getChildState.
        for (FileStatus fileStatus : dirSnapshot.getFiles()) {
          if (!ignoreEmptyFiles || fileStatus.getLen() != 0) {
            directory.getOriginalFiles().add(new HdfsFileStatusWithoutId(fileStatus));
          }
        }
      } else if (dirName.startsWith(BASE_PREFIX)) {
        processBaseDir(dirPath, writeIdList, validTxnList, directory, dirSnapshot);
      } else if (dirName.startsWith(DELTA_PREFIX) || dirName.startsWith(DELETE_DELTA_PREFIX)) {
        processDeltaDir(dirPath, writeIdList, validTxnList, directory, dirSnapshot);
      } else {
        directory.getOriginalDirectories().add(dirPath);
        for (FileStatus stat : dirSnapshot.getFiles()) {
          if ((!ignoreEmptyFiles) || (stat.getLen() != 0)) {
            directory.getOriginalFiles().add(new HdfsFileStatusWithoutId(stat));
          }
        }
      }
    }
  }

  private static void processBaseDir(Path baseDir, ValidWriteIdList writeIdList, ValidTxnList validTxnList,
      AcidDirectory directory, AcidUtils.HdfsDirSnapshot dirSnapshot) throws IOException {
    ParsedBaseLight parsedBase = ParsedBaseLight.parseBase(baseDir);
    if (!isDirUsable(baseDir, parsedBase.getVisibilityTxnId(), directory.getAbortedDirectories(), validTxnList)) {
      return;
    }
    final long writeId = parsedBase.getWriteId();
    if (directory.getOldestBase() == null || directory.getOldestBase().writeId > writeId) {
      // keep track for error reporting
      directory.setOldestBase(parsedBase);
    }
    boolean isCompactedBase = isCompactedBase(parsedBase, directory.getFs(), dirSnapshot);
    // Handle aborted IOW base.
    if (writeIdList.isWriteIdAborted(writeId) && !isCompactedBase) {
      directory.getAbortedDirectories().add(baseDir);
      directory.getAbortedWriteIds().add(parsedBase.writeId);
      return;
    }
    if (directory.getBase() == null || directory.getBase().getWriteId() < writeId
      // If there are two competing versions of a particular write-id, one from the compactor and another from IOW,
      // always pick the compactor one once it is committed.
      || directory.getBase().getWriteId() == writeId &&
          isCompactedBase && validTxnList.isTxnValid(parsedBase.getVisibilityTxnId())) {

      if (isValidBase(parsedBase, writeIdList, directory.getFs(), dirSnapshot)) {
        List<HdfsFileStatusWithId> files = null;
        if (dirSnapshot != null) {
          files = dirSnapshot.getFiles().stream().map(HdfsFileStatusWithoutId::new).collect(Collectors.toList());
        }
        if (directory.getBase() != null) {
          directory.getObsolete().add(directory.getBase().getBaseDirPath());
        }
        directory.setBase(new ParsedBase(parsedBase, files));
      }
    } else {
      directory.getObsolete().add(baseDir);
    }
  }

  private static void processDeltaDir(Path deltadir, ValidWriteIdList writeIdList, ValidTxnList validTxnList, AcidDirectory directory, AcidUtils.HdfsDirSnapshot dirSnapshot)
      throws IOException {
    ParsedDelta delta = parsedDelta(deltadir, directory.getFs(), dirSnapshot);
    if (!isDirUsable(deltadir, delta.getVisibilityTxnId(), directory.getAbortedDirectories(), validTxnList)) {
      return;
    }
    ValidWriteIdList.RangeResponse abortRange = writeIdList.isWriteIdRangeAborted(delta.minWriteId, delta.maxWriteId);
    if (ValidWriteIdList.RangeResponse.ALL == abortRange) {
      directory.getAbortedDirectories().add(deltadir);
      directory.getAbortedWriteIds().addAll(LongStream.rangeClosed(delta.minWriteId, delta.maxWriteId)
          .boxed().collect(Collectors.toList()));
    } else {
      if (ValidWriteIdList.RangeResponse.SOME == abortRange) {
        // This means this delta contains aborted writes but can not be cleaned
        // This is important for Cleaner to not remove metadata belonging to this transaction
        directory.setUnCompactedAborts(true);
      }
      if (writeIdList.isWriteIdRangeValid(delta.minWriteId, delta.maxWriteId) != ValidWriteIdList.RangeResponse.NONE) {
        directory.getCurrentDirectories().add(delta);
      }
    }
  }

  /**
   * checks {@code visibilityTxnId} to see if {@code child} is committed in current snapshot
   */
  private static boolean isDirUsable(Path child, long visibilityTxnId, List<Path> aborted, ValidTxnList validTxnList) {
    if (validTxnList == null) {
      throw new IllegalArgumentException("No ValidTxnList for " + child);
    }
    if (!validTxnList.isTxnValid(visibilityTxnId)) {
      boolean isAborted = validTxnList.isTxnAborted(visibilityTxnId);
      if (isAborted) {
        aborted.add(child);// so we can clean it up
      }
      LOG.debug("getChildState() ignoring(" + aborted + ") " + child);
      return false;
    }
    return true;
  }

  public static boolean isTablePropertyTransactional(Properties props) {
    return isTablePropertyTransactional(Maps.fromProperties(props));
  }

  public static boolean isTablePropertyTransactional(Map<String, String> parameters) {
    String resultStr = parameters.get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    if (resultStr == null) {
      resultStr = parameters.get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL.toUpperCase());
    }
    return Boolean.parseBoolean(resultStr);
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

  public static boolean isTransactionalTable(Table table) {
    return table != null && isTransactionalTable(table.getTTable());
  }

  public static boolean isTransactionalTable(org.apache.hadoop.hive.metastore.api.Table table) {
    return table != null && isTransactionalTable(table.getParameters());
  }

  public static boolean isTransactionalTable(Map<String, String> props) {
    return props != null && isTablePropertyTransactional(props);
  }

  public static boolean isTransactionalView(CreateMaterializedViewDesc view) {
    if (view == null || view.getTblProps() == null) {
      return false;
    }
    return isTransactionalTable(view.getTblProps());
  }

  public static boolean isFullAcidTable(CreateTableDesc td) {
    if (td == null || td.getTblProps() == null) {
      return false;
    }
    return isFullAcidTable(td.getTblProps());
  }
  /**
   * Should produce the same result as
   * {@link org.apache.hadoop.hive.metastore.txn.TxnUtils#isAcidTable(org.apache.hadoop.hive.metastore.api.Table)}
   */
  public static boolean isFullAcidTable(Table table) {
    return table != null && isFullAcidTable(table.getTTable());
  }

  /**
   * Should produce the same result as
   * {@link org.apache.hadoop.hive.metastore.txn.TxnUtils#isAcidTable(org.apache.hadoop.hive.metastore.api.Table)}
   */
  public static boolean isFullAcidTable(org.apache.hadoop.hive.metastore.api.Table table) {
    return table != null && isFullAcidTable(table.getParameters());
  }

  public static boolean isFullAcidTable(Map<String, String> params) {
    return isTransactionalTable(params) && !isInsertOnlyTable(params);
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

  public static boolean isInsertOnlyFetchBucketId(Configuration conf) {
    if (!HiveConf.getBoolVar(conf, ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN)) {
      return false;
    }
    int propInt = conf.getInt(ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname, -1);
    if (propInt == -1) {
      return false;
    }
    AcidOperationalProperties props = AcidOperationalProperties.parseInt(propInt);
    if (!props.isInsertOnly()) {
      return false;
    }
    return props.isFetchBucketId();
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
   * by {@link #getAcidState(FileSystem, Path, Configuration, ValidWriteIdList, Ref, boolean)}
   * and so won't be read at all.
   * @param file - data file to read/compute splits on
   */
  public static long getLogicalLength(FileSystem fs, FileStatus file) throws IOException {
    Path acidDir = file.getPath().getParent(); //should be base_x or delta_x_y_
    if (AcidUtils.isInsertDelta(acidDir)) {
      ParsedDeltaLight pd = ParsedDeltaLight.parse(acidDir);
      if(!pd.mayContainSideFile()) {
        return file.getLen();
      }
    }
    else {
      return file.getLen();
    }
    return getLastFlushLength(fs, file);
  }

  /**
   * Read the side file to get the last flush length, or file length if no side file found.
   * @param fs the file system to use
   * @param deltaFile the delta file
   * @return length as stored in the side file, or file length if no side file found
   * @throws IOException if problems reading the side file
   */
  private static long getLastFlushLength(FileSystem fs, FileStatus deltaFile) throws IOException {
    Path sideFile = OrcAcidUtils.getSideFile(deltaFile.getPath());

    try (FSDataInputStream stream = fs.open(sideFile)) {
      long result = -1;
      while (stream.available() > 0) {
        result = stream.readLong();
      }
      if (result < 0) {
        /* side file is there but we couldn't read it. We want to avoid a read where
         * (file.getLen() < 'value from side file' which may happen if file is not closed)
         * because this means some committed data from 'file' would be skipped. This should be very
         * unusual.
         */
        throw new IOException(sideFile + " found but is not readable.  Consider waiting or "
            + "orcfiledump --recover");
      }
      return result;

    } catch (FileNotFoundException e) {
      return deltaFile.getLen();
    }
  }


  /**
   * Checks if a table is a transactional table that only supports INSERT, but not UPDATE/DELETE
   * @param params table properties
   * @return true if table is an INSERT_ONLY table, false otherwise
   */
  public static boolean isInsertOnlyTable(Map<String, String> params) {
    String transactionalProp = params.get(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES);
    return INSERTONLY_TRANSACTIONAL_PROPERTY.equalsIgnoreCase(transactionalProp);
  }

  public static boolean isInsertOnlyTable(Table table) {
    return isTransactionalTable(table) && getAcidOperationalProperties(table).isInsertOnly();
  }

  public static boolean isInsertOnlyTable(Properties params) {
    return isInsertOnlyTable(Maps.fromProperties(params));
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
    boolean isSetToTxn = Boolean.parseBoolean(transactional);
    if (transactionalProp == null) {
      if (isSetToTxn || tbl == null) return false; // Assume the full ACID table.
      throw new RuntimeException("Cannot change '" + hive_metastoreConstants.TABLE_IS_TRANSACTIONAL
          + "' without '" + hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES + "'");
    }
    if (!INSERTONLY_TRANSACTIONAL_PROPERTY.equalsIgnoreCase(transactionalProp)) {
      return false; // Not MM.
    }
    if (!isSetToTxn) {
      if (tbl == null) return true; // No table information yet; looks like it could be valid.
      throw new RuntimeException("Cannot set '"
          + hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES + "' to 'insert_only' without "
          + "setting '" + hive_metastoreConstants.TABLE_IS_TRANSACTIONAL + "' to 'true'");
    }
    return true;
  }

  public static Boolean isToFullAcid(Table table, Map<String, String> props) {
    if (AcidUtils.isTransactionalTable(table)) {
      String transactionalProp = props.get(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES);

      if (DEFAULT_TRANSACTIONAL_PROPERTY.equalsIgnoreCase(transactionalProp)) {
        return canBeMadeAcid(table.getTableName(), table.getSd());
      }
    }
    return false;
  }

  public static boolean canBeMadeAcid(String fullTableName, StorageDescriptor sd) {
    return isAcidInputOutputFormat(fullTableName, sd) && sd.getSortColsSize() <= 0;
  }

  private static boolean isAcidInputOutputFormat(String fullTableName, StorageDescriptor sd) {
    if (sd.getInputFormat() == null || sd.getOutputFormat() == null) {
      return false;
    }
    try {
      return Class.forName(Constants.ORC_INPUT_FORMAT)
                  .isAssignableFrom(Class.forName(sd.getInputFormat()))
            && Class.forName(Constants.ORC_OUTPUT_FORMAT)
                  .isAssignableFrom(Class.forName(sd.getOutputFormat()));

    } catch (ClassNotFoundException e) {
      //if a table is using some custom I/O format and it's not in the classpath, we won't mark
      //the table for Acid, but today OrcInput/OutputFormat is the only Acid format
      LOG.error("Could not determine if " + fullTableName + " can be made Acid due to: " + e.getMessage(), e);
      return false;
    }
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
    return new ValidTxnWriteIdList(txnString);
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

    if (SessionState.get() != null) {
      HiveTxnManager sessionTxnMgr = SessionState.get().getTxnMgr();
      String fullTableName = getFullTableName(dbName, tblName);
      if (sessionTxnMgr != null && sessionTxnMgr.getCurrentTxnId() > 0) {
        validWriteIdList = getTableValidWriteIdList(conf, fullTableName);
        if (isStatsUpdater) {
          writeId = sessionTxnMgr.getAllocatedTableWriteId(dbName, tblName);
          if (writeId < 1) {
            // TODO: this is not ideal... stats updater that doesn't have write ID is currently
            //       "create table"; writeId would be 0/-1 here. No need to call this w/true.
            LOG.debug("Stats updater for {}.{} doesn't have a write ID ({})", dbName, tblName, writeId);
          }
        }

        if (HiveConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST) && conf.get(ValidTxnList.VALID_TXNS_KEY) == null) {
          return null;
        }
        if (validWriteIdList == null) {
          validWriteIdList = getTableValidWriteIdListWithTxnList(conf, dbName, tblName);
        }
        if (validWriteIdList == null) {
          throw new AssertionError("Cannot find valid write ID list for " + tblName);
        }
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
    String validTxnList = conf.get(ValidTxnList.VALID_TXNS_KEY);
    List<String> tablesInput = new ArrayList<>();
    String fullTableName = getFullTableName(dbName, tableName);
    tablesInput.add(fullTableName);

    ValidTxnWriteIdList validTxnWriteIdList = sessionTxnMgr.getValidWriteIds(tablesInput, validTxnList);
    return validTxnWriteIdList != null ?
        validTxnWriteIdList.getTableValidWriteIdList(fullTableName) : null;
  }

  public static String getFullTableName(String dbName, String tableName) {
    return TableName.fromString(tableName, null, dbName).getNotEmptyDbTable().toLowerCase();
  }

  /**
   * General facility to place a metadata file into a dir created by acid/compactor write.
   *
   * Load Data commands against Acid tables write {@link AcidBaseFileType#ORIGINAL_BASE} type files
   * into delta_x_x/ (or base_x in case there is Overwrite clause).  {@link MetaDataFile} is a
   * small JSON file in this directory that indicates that these files don't have Acid metadata
   * columns and so the values for these columns need to be assigned at read time/compaction.
   */
  public static class MetaDataFile extends AcidMetaDataFile {

    static boolean isCompacted(Path baseOrDeltaDir, FileSystem fs, HdfsDirSnapshot dirSnapshot) throws IOException {
      /**
       * this file was written by Hive versions before 4.0 into a base_x/ dir
       * created by compactor so that it can be distinguished from the one
       * created by Insert Overwrite
       */
      if (dirSnapshot != null && dirSnapshot.getMetadataFile() == null) {
        return false;
      }
      Path formatFile = new Path(baseOrDeltaDir, METADATA_FILE);
      try (FSDataInputStream strm = fs.open(formatFile)) {
        Map<String, String> metaData = new ObjectMapper().readValue((InputStream)strm, Map.class);
        if (!CURRENT_VERSION.equalsIgnoreCase(metaData.get(Field.VERSION.toString()))) {
          throw new IllegalStateException("Unexpected Meta Data version: " + metaData.get(Field.VERSION));
        }
        String dataFormat = metaData.getOrDefault(Field.DATA_FORMAT.toString(), "null");
        DataFormat format = DataFormat.valueOf(dataFormat.toUpperCase());
        return DataFormat.COMPACTED == format;
      } catch (FileNotFoundException e) {
        return false;
      } catch (IOException e) {
        String msg = "Failed to read " + baseOrDeltaDir + "/" + METADATA_FILE + ": " + e.getMessage();
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
        dataFiles = fs.listStatus(baseOrDeltaDir, originalBucketFilter);
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
        if(isCompactedBase(ParsedBaseLight.parseBase(baseOrDeltaDir), fs, dirSnapshot)) {
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
      try (Reader reader = OrcFile.createReader(dataFile, OrcFile.readerOptions(fs.getConf()))) {
        /*
          acid file would have schema like <op, owid, writerId, rowid, cwid, <f1, ... fn>> so could
          check it this way once/if OrcRecordUpdater.ACID_KEY_INDEX_NAME is removed
          TypeDescription schema = reader.getSchema();
          List<String> columns = schema.getFieldNames();
         */
        return OrcInputFormat.isOriginal(reader);
      } catch (FileFormatException | InvalidProtocolBufferException ex) {
        //We may be parsing a delta for Insert-only table which may not even be an ORC file so
        //cannot have ROW_IDs in it.
        LOG.debug("isRawFormat() called on " + dataFile + " which is not an ORC file: " +
            ex.getMessage());
        return true;
      } catch (FileNotFoundException ex) {
        //Fallback in case file was already removed and used Snapshot is outdated
        return false;
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
   *
   * Methods for getting/reading the version from files were moved to test class TestTxnCommands
   * which is the only place they are used, in order to keep devs out of temptation, since they
   * access the FileSystem which is expensive.
   */
  public static final class OrcAcidVersion {
    public static final String ACID_VERSION_KEY = "hive.acid.version";
    public static final String ACID_FORMAT = "_orc_acid_version";
    private static final Charset UTF8 = Charset.forName("UTF-8");
    /**
     * 2 is the version of Acid released in Hive 3.0.
     */
    public static final int ORC_ACID_VERSION = 2;
    /**
     * Include current acid version in file footer.
     * @param writer - file written
     */
    public static void setAcidVersionInDataFile(Writer writer) {
      //so that we know which version wrote the file
      writer.addUserMetadata(ACID_VERSION_KEY, UTF8.encode(String.valueOf(ORC_ACID_VERSION)));
    }

    /**
     * This creates a version file in {@code deltaOrBaseDir}
     * @param deltaOrBaseDir - where to create the version file
     */
    public static void writeVersionFile(Path deltaOrBaseDir, FileSystem fs)  throws IOException {
      Path formatFile = getVersionFilePath(deltaOrBaseDir);
      if(!fs.isFile(formatFile)) {
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
    if (fs == null) {
      fs = dir.getFileSystem(jc);
    }
    // Collect the all of the files/dirs
    Map<Path, HdfsDirSnapshot> hdfsDirSnapshots = AcidUtils.getHdfsDirSnapshots(fs, dir);
    AcidDirectory acidInfo = AcidUtils.getAcidState(fs, dir, jc, idList, null, false, hdfsDirSnapshots);
    // Assume that for an MM table, or if there's only the base directory, we are good.
    if (!acidInfo.getCurrentDirectories().isEmpty() && AcidUtils.isFullAcidTable(table)) {
      Utilities.FILE_OP_LOGGER.warn(
          "Computing stats for an ACID table; stats may be inaccurate");
    }
    for (HdfsFileStatusWithId hfs : acidInfo.getOriginalFiles()) {
      fileList.add(hfs.getFileStatus());
    }
    for (ParsedDelta delta : acidInfo.getCurrentDirectories()) {
      fileList.addAll(hdfsDirSnapshots.get(delta.getPath()).getFiles());
    }
    if (acidInfo.getBaseDirectory() != null) {
      fileList.addAll(hdfsDirSnapshots.get(acidInfo.getBaseDirectory()).getFiles());
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
    AcidDirectory acidInfo = AcidUtils.getAcidState(dataPath.getFileSystem(conf), dataPath, conf, validWriteIdList, null,
        false);

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
    private String baseDirName, deltaDirName, deleteDeltaDirName;
    private final boolean isDeltaPrefix;
    private final Set<String> dpSpecs;
    private final int dpLevel;

    public IdPathFilter(long writeId, int stmtId) {
      this(writeId, stmtId, null, 0);
    }

    public IdPathFilter(long writeId, int stmtId, Set<String> dpSpecs, int dpLevel) {
      String deltaDirName = DELTA_PREFIX + String.format(DELTA_DIGITS, writeId) + "_" +
          String.format(DELTA_DIGITS, writeId);
      String deleteDeltaDirName = DELETE_DELTA_PREFIX + String.format(DELTA_DIGITS, writeId) + "_" +
          String.format(DELTA_DIGITS, writeId);
      isDeltaPrefix = (stmtId < 0);
      if (!isDeltaPrefix) {
        deltaDirName += "_" + String.format(STATEMENT_DIGITS, stmtId);
        deleteDeltaDirName += "_" + String.format(STATEMENT_DIGITS, stmtId);
      }

      this.baseDirName = BASE_PREFIX + String.format(DELTA_DIGITS, writeId);
      this.deltaDirName = deltaDirName;
      this.deleteDeltaDirName = deleteDeltaDirName;
      this.dpSpecs = dpSpecs;
      this.dpLevel = dpLevel;
    }

    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      // Extending the path filter with optional dynamic partition specifications.
      // This is needed for the use case when doing multi-statement insert overwrite with
      // dynamic partitioning with direct insert or with insert-only tables.
      // In this use-case, each FileSinkOperator should only clean-up the directories written
      // by the same FileSinkOperator and do not clean-up the partition directories
      // written by the other FileSinkOperators. (For further details please see HIVE-23114.)
      if (dpLevel > 0 && dpSpecs != null && !dpSpecs.isEmpty()) {
        Path parent = path.getParent();
        String partitionSpec = parent.getName();
        for (int i = 1; i < dpLevel; i++) {
          parent = parent.getParent();
          partitionSpec = parent.getName() + "/" + partitionSpec;
        }
        return (name.equals(baseDirName) && dpSpecs.contains(partitionSpec));
      }
      else {
        return name.equals(baseDirName)
            || (isDeltaPrefix && (name.startsWith(deltaDirName) || name.startsWith(deleteDeltaDirName)))
            || (!isDeltaPrefix && (name.equals(deltaDirName) || name.equals(deleteDeltaDirName)));
      }
    }
  }

  /**
   * Full recursive PathFilter version of IdPathFilter (filtering files for a given writeId and stmtId).
   * This can be used by recursive filelisting, when we want to match the delta / base pattern on the bucketFiles.
   */
  public static class IdFullPathFiler extends IdPathFilter {
    private final Path basePath;

    public IdFullPathFiler(long writeId, int stmtId, Path basePath) {
      super(writeId, stmtId);
      this.basePath = basePath;
    }
    @Override
    public boolean accept(Path path) {
      do {
        if (super.accept(path)) {
          return true;
        }
        path = path.getParent();
      } while (path != null && !path.equals(basePath));
      return false;
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

  private static boolean needsLock(Entity entity, boolean isExternalEnabled) {
    return needsLock(entity, isExternalEnabled, false);
  }

  private static boolean needsLock(Entity entity, boolean isExternalEnabled, boolean isLocklessReads) {
    switch (entity.getType()) {
      case TABLE:
        return isLockableTable(entity.getTable(), isExternalEnabled, isLocklessReads);
      case PARTITION:
        return isLockableTable(entity.getPartition().getTable(), isExternalEnabled, isLocklessReads);
      default:
        return true;
    }
  }

  private static boolean isLockableTable(Table t, boolean isExternalEnabled, boolean isLocklessReads) {
    if (t.isTemporary()) {
      return false;
    }
    switch (t.getTableType()) {
      case MANAGED_TABLE:
      case MATERIALIZED_VIEW:
        return !(isLocklessReads && isTransactionalTable(t));
      case EXTERNAL_TABLE:
        return isExternalEnabled;
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
      Context.Operation operation, HiveConf conf) {

    List<LockComponent> lockComponents = new ArrayList<>();
    boolean isLocklessReadsEnabled = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED);
    boolean skipReadLock = !conf.getBoolVar(ConfVars.HIVE_TXN_READ_LOCKS);
    boolean skipNonAcidReadLock = !conf.getBoolVar(ConfVars.HIVE_TXN_NONACID_READ_LOCKS);

    boolean sharedWrite = !conf.getBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK);
    boolean isExternalEnabled = conf.getBoolVar(HiveConf.ConfVars.HIVE_TXN_EXT_LOCKING_ENABLED);
    boolean isMerge = operation == Context.Operation.MERGE;

    // We don't want to acquire read locks during update or delete as we'll be acquiring write
    // locks instead. Also, there's no need to lock temp tables since they're session wide
    List<ReadEntity> readEntities = inputs.stream()
      .filter(input -> !input.isDummy()
        && input.needsLock()
        && !input.isUpdateOrDelete()
        && AcidUtils.needsLock(input, isExternalEnabled, isLocklessReadsEnabled)
        && !skipReadLock)
      .collect(Collectors.toList());

    Set<Table> fullTableLock = getFullTableLock(readEntities, conf);

    // For each source to read, get a shared_read lock
    for (ReadEntity input : readEntities) {
      LockComponentBuilder compBuilder = new LockComponentBuilder();
      compBuilder.setSharedRead();
      compBuilder.setOperationType(DataOperationType.SELECT);

      Table t = null;
      switch (input.getType()) {
        case DATABASE:
          compBuilder.setDbName(input.getDatabase().getName());
          break;

        case TABLE:
          t = input.getTable();
          if (!fullTableLock.contains(t)) {
            continue;
          }
          compBuilder.setDbName(t.getDbName());
          compBuilder.setTableName(t.getTableName());
          break;

        case PARTITION:
        case DUMMYPARTITION:
          compBuilder.setPartitionName(input.getPartition().getName());
          t = input.getPartition().getTable();
          if (fullTableLock.contains(t)) {
            continue;
          }
          compBuilder.setDbName(t.getDbName());
          compBuilder.setTableName(t.getTableName());
          break;

        default:
          // This is a file or something we don't hold locks for.
          continue;
      }
      if (skipNonAcidReadLock && !AcidUtils.isTransactionalTable(t)) {
        // skip read-locks for non-transactional tables
        // read-locks don't protect non-transactional tables data consistency
        continue;
      }
      if (t != null) {
        compBuilder.setIsTransactional(AcidUtils.isTransactionalTable(t));
      }
      LockComponent comp = compBuilder.build();
      LOG.debug("Adding lock component to lock request {} ", comp);
      lockComponents.add(comp);
    }
    // For each source to write to, get the appropriate lock type.  If it's
    // an OVERWRITE, we need to get an exclusive lock.  If it's an insert (no
    // overwrite) than we need a shared.  If it's update or delete then we
    // need a SHARED_WRITE.
    for (WriteEntity output : outputs) {
      if (output.getType() == Entity.Type.DFS_DIR || output.getType() == Entity.Type.LOCAL_DIR
          || !AcidUtils.needsLock(output, isExternalEnabled)) {
        // We don't lock files or directories. We also skip locking temp tables.
        continue;
      }
      LockComponentBuilder compBuilder = new LockComponentBuilder();
      Table t = null;
      /**
       * For any insert/updates set dir cache to read-only mode, where it wouldn't
       * add any new entry to cache.
       * When updates are executed, delta folders are created only at the end of the statement
       * and at the time of acquiring locks, there would not be any delta folders. This can cause wrong data to be reported
       * when "insert" followed by "update" statements are executed. In such cases, use the cache as read only mode.
       */
      HiveConf.setIntVar(conf, ConfVars.HIVE_TXN_ACID_DIR_CACHE_DURATION, 0);
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

      case DDL_EXCL_WRITE:
        compBuilder.setExclWrite();
        compBuilder.setOperationType(DataOperationType.NO_TXN);
        break;

      case CTAS:
        assert t != null;
        if (AcidUtils.isTransactionalTable(t)) {
          compBuilder.setExclWrite();
          compBuilder.setOperationType(DataOperationType.INSERT);
        } else {
          compBuilder.setExclusive();
          compBuilder.setOperationType(DataOperationType.NO_TXN);
        }
        break;

      case INSERT_OVERWRITE:
        assert t != null;
        if (AcidUtils.isTransactionalTable(t)) {
          if (conf.getBoolVar(HiveConf.ConfVars.TXN_OVERWRITE_X_LOCK) && !sharedWrite
                  && !isLocklessReadsEnabled) {
            compBuilder.setExclusive();
          } else {
            compBuilder.setExclWrite();
          }
          compBuilder.setOperationType(DataOperationType.UPDATE);
        } else if (MetaStoreUtils.isNonNativeTable(t.getTTable())) {
          compBuilder.setLock(getLockTypeFromStorageHandler(output, t));
          compBuilder.setOperationType(DataOperationType.UPDATE);
        } else {
          compBuilder.setExclusive();
          compBuilder.setOperationType(DataOperationType.NO_TXN);
        }
        break;

      case INSERT:
        assert t != null;
        if (AcidUtils.isTransactionalTable(t)) {
          boolean isExclMergeInsert = conf.getBoolVar(ConfVars.TXN_MERGE_INSERT_X_LOCK) && isMerge;
          compBuilder.setSharedRead();

          if (sharedWrite) {
            compBuilder.setSharedWrite();
          } else {
            if (isExclMergeInsert) {
              compBuilder.setExclWrite();

            } else if (isLocklessReadsEnabled) {
              compBuilder.setSharedWrite();
            }
          }
          if (isExclMergeInsert) {
            compBuilder.setOperationType(DataOperationType.UPDATE);
            break;
          }
        } else if (MetaStoreUtils.isNonNativeTable(t.getTTable())) {
          compBuilder.setLock(getLockTypeFromStorageHandler(output, t));
        } else {
          if (conf.getBoolVar(HiveConf.ConfVars.HIVE_TXN_STRICT_LOCKING_MODE)) {
            compBuilder.setExclusive();
          } else {  // this is backward compatible for non-ACID resources, w/o ACID semantics
            compBuilder.setSharedRead();
          }
        }
        compBuilder.setOperationType(DataOperationType.INSERT);
        break;

      case DDL_SHARED:
        compBuilder.setSharedRead();
        if (output.isTxnAnalyze()) {
          // Analyze needs txn components to be present, otherwise an aborted analyze write ID
          // might be rolled under the watermark by compactor while stats written by it are
          // still present.
          continue;
        }
        compBuilder.setOperationType(DataOperationType.NO_TXN);
        break;

      case UPDATE:
      case DELETE:
        assert t != null;
        if (AcidUtils.isTransactionalTable(t) && sharedWrite) {
          compBuilder.setSharedWrite();
        } else if (MetaStoreUtils.isNonNativeTable(t.getTTable())) {
          compBuilder.setLock(getLockTypeFromStorageHandler(output, t));
        } else {
          compBuilder.setExclWrite();
        }
        compBuilder.setOperationType(DataOperationType.valueOf(
            output.getWriteType().name()));
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

  private static LockType getLockTypeFromStorageHandler(WriteEntity output, Table t) {
    final HiveStorageHandler storageHandler = Preconditions.checkNotNull(t.getStorageHandler(),
        "Non-native tables must have an instance of storage handler.");
    LockType lockType = storageHandler.getLockType(output);
    if (null == lockType) {
      throw new IllegalArgumentException(
              String.format("Lock type for Database.Table [%s.%s] is null", t.getDbName(), t.getTableName()));
    }
    return lockType;
  }

  public static boolean isExclusiveCTASEnabled(Configuration conf) {
    return HiveConf.getBoolVar(conf, ConfVars.TXN_CTAS_X_LOCK);
  }

  public static boolean isExclusiveCTAS(Set<WriteEntity> outputs, HiveConf conf) {
    return outputs.stream().anyMatch(we -> we.getWriteType().equals(WriteType.CTAS) && isExclusiveCTASEnabled(conf));
  }

  private static Set<Table> getFullTableLock(List<ReadEntity> readEntities, HiveConf conf) {
    int partLocksThreshold = conf.getIntVar(HiveConf.ConfVars.HIVE_LOCKS_PARTITION_THRESHOLD);

    Map<Table, Long> partLocksPerTable = readEntities.stream()
      .filter(input -> input.getType() == Entity.Type.PARTITION)
      .map(Entity::getPartition)
      .collect(Collectors.groupingBy(Partition::getTable, Collectors.counting()));

    return readEntities.stream()
      .filter(input -> input.getType() == Entity.Type.TABLE)
      .map(Entity::getTable)
      .filter(t -> !partLocksPerTable.containsKey(t)
        || (partLocksThreshold > 0 && partLocksThreshold <= partLocksPerTable.get(t)))
      .collect(Collectors.toSet());
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
    // check if read-only txn
    if (HiveConf.getBoolVar(conf, ConfVars.HIVE_TXN_READONLY_ENABLED) && isReadOnlyTxn(tree)) {
      return TxnType.READ_ONLY;
    }
    // check if txn has a materialized view rebuild
    if (tree.getToken().getType() == HiveParser.TOK_ALTER_MATERIALIZED_VIEW_REBUILD) {
      return TxnType.MATER_VIEW_REBUILD;
    }
    // check if soft delete txn
    if (isSoftDeleteTxn(conf, tree))  {
      return TxnType.SOFT_DELETE;
    }
    return TxnType.DEFAULT;
  }

  private static boolean isReadOnlyTxn(ASTNode tree) {
    final ASTSearcher astSearcher = new ASTSearcher();
    return READ_TXN_TOKENS.contains(tree.getToken().getType())
      || (tree.getToken().getType() == HiveParser.TOK_QUERY && Stream.of(
          new int[]{HiveParser.TOK_INSERT_INTO},
          new int[]{HiveParser.TOK_INSERT, HiveParser.TOK_TAB})
      .noneMatch(pattern -> astSearcher.simpleBreadthFirstSearch(tree, pattern) != null));
  }

  private static boolean isSoftDeleteTxn(Configuration conf, ASTNode tree) {
    boolean locklessReadsEnabled = HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED);

    switch (tree.getToken().getType()) {
      case HiveParser.TOK_DROPDATABASE:
      case HiveParser.TOK_DROPTABLE:
      case HiveParser.TOK_DROP_MATERIALIZED_VIEW:
        return locklessReadsEnabled
          || HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_CREATE_TABLE_USE_SUFFIX);

      case HiveParser.TOK_ALTERTABLE: {
        boolean isDropParts = tree.getFirstChildWithType(HiveParser.TOK_ALTERTABLE_DROPPARTS) != null;
        if (isDropParts) {
          return locklessReadsEnabled
            || HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_DROP_PARTITION_USE_BASE);
        }
        boolean isRenamePart = tree.getFirstChildWithType(HiveParser.TOK_ALTERTABLE_RENAMEPART) != null;
        if (isRenamePart) {
          return locklessReadsEnabled
            || HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_RENAME_PARTITION_MAKE_COPY);
        }
      }
    }
    return false;
  }

  public static String getPathSuffix(long txnId) {
    return (SOFT_DELETE_PATH_SUFFIX + String.format(DELTA_DIGITS, txnId));
  }

  @VisibleForTesting
  public static void initDirCache(int durationInMts) {
    if (dirCacheInited.get()) {
      LOG.debug("DirCache got initialized already");
      return;
    }
    dirCache = CacheBuilder.newBuilder()
        .expireAfterWrite(durationInMts, TimeUnit.MINUTES)
        .softValues()
        .build();
    dirCacheInited.set(true);
  }

  private static void printDirCacheEntries() {
    if (dirCache != null) {
      LOG.debug("Cache entries: {}", Arrays.toString(dirCache.asMap().keySet().toArray()));
    }
  }

  /**
   * Tries to get directory details from cache. For now, cache is valid only
   * when base directory is available and no deltas are present. This should
   * be used only in BI strategy and for ACID tables.
   *
   * @param fileSystem file system supplier
   * @param candidateDirectory the partition directory to analyze
   * @param conf the configuration
   * @param writeIdList the list of write ids that we are reading
   * @param useFileIds It will be set to true, if the FileSystem supports listing with fileIds
   * @param ignoreEmptyFiles Ignore files with 0 length
   * @return directory state
   * @throws IOException on errors
   */
  public static AcidDirectory getAcidStateFromCache(Supplier<FileSystem> fileSystem,
      Path candidateDirectory, Configuration conf,
      ValidWriteIdList writeIdList, Ref<Boolean> useFileIds, boolean ignoreEmptyFiles) throws IOException {

    int dirCacheDuration = HiveConf.getIntVar(conf,
        ConfVars.HIVE_TXN_ACID_DIR_CACHE_DURATION);

    if (dirCacheDuration < 0) {
      LOG.debug("dirCache is not enabled");
      return getAcidState(fileSystem.get(), candidateDirectory, conf, writeIdList,
          useFileIds, ignoreEmptyFiles);
    } else {
      initDirCache(dirCacheDuration);
    }

    /*
     * Cache for single case, where base directory is there without deltas.
     * In case of changes, cache would get invalidated based on
     * open/aborted list
     */
    //dbName + tableName + dir
    String key = writeIdList.getTableName() + "_" + candidateDirectory.toString();
    DirInfoValue value = dirCache.getIfPresent(key);

    // in case of open/aborted txns, recompute dirInfo
    long[] exceptions = writeIdList.getInvalidWriteIds();
    boolean recompute = (exceptions != null && exceptions.length > 0);

    if (recompute) {
      LOG.info("invalidating cache entry for key: {}", key);
      dirCache.invalidate(key);
      value = null;
    }

    if (value != null) {
      // double check writeIds
      if (!value.getTxnString().equalsIgnoreCase(writeIdList.writeToString())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("writeIdList: {} from cache: {} is not matching for key: {}",
              writeIdList.writeToString(), value.getTxnString(), key);
        }
        recompute = true;
      }
    }

    // compute and add to cache
    if (recompute || (value == null)) {
      AcidDirectory dirInfo = getAcidState(fileSystem.get(), candidateDirectory, conf,
          writeIdList, useFileIds, ignoreEmptyFiles);
      value = new DirInfoValue(writeIdList.writeToString(), dirInfo);

      if (value.dirInfo != null && value.dirInfo.getBaseDirectory() != null
          && value.dirInfo.getCurrentDirectories().isEmpty()) {
        if (dirCacheDuration > 0) {
          dirCache.put(key, value);
        } else {
          LOG.info("Not populating cache for {}, as duration is set to 0", key);
        }
      }
    } else {
      LOG.info("Got {} from cache, cache size: {}", key, dirCache.size());
    }
    if (LOG.isDebugEnabled()) {
      printDirCacheEntries();
    }
    return value.getDirInfo();
  }

  public static void tryInvalidateDirCache(org.apache.hadoop.hive.metastore.api.Table table) {
    if (dirCacheInited.get()) {
      String key = getFullTableName(table.getDbName(), table.getTableName()) + "_" + table.getSd().getLocation();
      boolean partitioned = table.getPartitionKeys() != null && !table.getPartitionKeys().isEmpty();
      if (!partitioned) {
        dirCache.invalidate(key);
      } else {
        // Invalidate all partitions as the difference in the key is only the partition part at the end of the path.
        dirCache.invalidateAll(
          dirCache.asMap().keySet().stream().filter(k -> k.startsWith(key)).collect(Collectors.toSet()));
      }
    }
  }

  public static boolean isNonNativeAcidTable(Table table) {
    return table != null && table.getStorageHandler() != null &&
        table.getStorageHandler().supportsAcidOperations() != HiveStorageHandler.AcidSupportType.NONE;
  }

  /**
   * Returns the virtual columns needed for update queries. For ACID queries it is a single ROW__ID, for non-native
   * tables the list is provided by the {@link HiveStorageHandler#acidVirtualColumns()}.
   * @param table The table for which we run the query
   * @return The list of virtual columns used
   */
  public static List<VirtualColumn> getAcidVirtualColumns(Table table) {
    if (isTransactionalTable(table)) {
      return Lists.newArrayList(VirtualColumn.ROWID);
    } else {
      if (isNonNativeAcidTable(table)) {
        return table.getStorageHandler().acidVirtualColumns();
      }
    }
    return Collections.emptyList();
  }

  public static boolean acidTableWithoutTransactions(Table table) {
    return table != null && table.getStorageHandler() != null &&
        table.getStorageHandler().supportsAcidOperations() ==
            HiveStorageHandler.AcidSupportType.WITHOUT_TRANSACTIONS;
  }

  static class DirInfoValue {
    private String txnString;
    private AcidDirectory dirInfo;

    DirInfoValue(String txnString, AcidDirectory dirInfo) {
      this.txnString = txnString;
      this.dirInfo = dirInfo;
    }

    String getTxnString() {
      return txnString;
    }

    AcidDirectory getDirInfo() {
      return dirInfo;
    }
  }

  public static String getPartitionName(Map<String, String> partitionSpec) throws SemanticException {
    String partitionName = null;
    if (partitionSpec != null) {
      try {
        partitionName = Warehouse.makePartName(partitionSpec, false);
      } catch (MetaException e) {
        throw new SemanticException("partition " + partitionSpec.toString() + " not found");
      }
    }
    return partitionName;
  }

  public static CompactionType compactionTypeStr2ThriftType(String inputValue) throws SemanticException {
    try {
      return CompactionType.valueOf(inputValue.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new SemanticException("Unexpected compaction type " + inputValue);
    }
  }

  public static CompactionState compactionStateStr2Enum(String inputValue) throws SemanticException {
    try {
      return CompactionState.fromString(inputValue);
    } catch (IllegalArgumentException e) {
      throw new SemanticException("Unexpected compaction state " + inputValue);
    }
  }
}
