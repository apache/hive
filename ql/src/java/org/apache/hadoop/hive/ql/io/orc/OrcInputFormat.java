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

import org.apache.hadoop.hive.common.NoDynamicValuesException;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Metastore;
import org.apache.hadoop.hive.metastore.Metastore.SplitInfos;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSupport;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils.AcidBaseFileInfo;
import org.apache.hadoop.hive.ql.io.AcidUtils.AcidOperationalProperties;
import org.apache.hadoop.hive.ql.io.AcidUtils.Directory;
import org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDelta;
import org.apache.hadoop.hive.ql.io.BatchToRowInputFormat;
import org.apache.hadoop.hive.ql.io.BatchToRowReader;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.HdfsUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.hive.ql.io.LlapWrappableInputFormatInterface;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.SelfDescribingInputFormatInterface;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordReader;
import org.apache.hadoop.hive.ql.io.SyntheticFileId;
import org.apache.hadoop.hive.ql.io.orc.ExternalCache.ExternalFooterCachesByConf;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf.Operator;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileStatusWithId;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.Ref;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.FileFormatException;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcProto.Footer;
import org.apache.orc.OrcProto.Type;
import org.apache.orc.OrcUtils;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.SchemaEvolution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.CodedInputStream;
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
 * originalWriteId, bucket, and rowId are the unique identifier for the row.
 * The operation and currentWriteId are the operation and the table write id within current txn
 * that added this event. Insert and update events include the entire row, while
 * delete events have null for row.
 */
public class OrcInputFormat implements InputFormat<NullWritable, OrcStruct>,
  InputFormatChecker, VectorizedInputFormatInterface, LlapWrappableInputFormatInterface,
  SelfDescribingInputFormatInterface, AcidInputFormat<NullWritable, OrcStruct>,
  CombineHiveInputFormat.AvoidSplitCombination, BatchToRowInputFormat {

  @Override
  public VectorizedSupport.Support[] getSupportedFeatures() {
    return new VectorizedSupport.Support[] {VectorizedSupport.Support.DECIMAL_64};
  }

  static enum SplitStrategyKind {
    HYBRID,
    BI,
    ETL
  }

  private static final Logger LOG = LoggerFactory.getLogger(OrcInputFormat.class);
  static final HadoopShims SHIMS = ShimLoader.getHadoopShims();

  private static final long DEFAULT_MIN_SPLIT_SIZE = 16 * 1024 * 1024;
  private static final long DEFAULT_MAX_SPLIT_SIZE = 256 * 1024 * 1024;
  private static final int DEFAULT_ETL_FILE_THRESHOLD = 100;

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


  /**
   * We can derive if a split is ACID or not from the flags encoded in OrcSplit.
   * If the file split is not instance of OrcSplit then its definitely not ACID.
   * If file split is instance of OrcSplit and the flags contain hasBase or deltas then it's
   * definitely ACID.
   * Else fallback to configuration object/table property.
   * @param conf
   * @param inputSplit
   * @return
   */
  public boolean isFullAcidRead(Configuration conf, InputSplit inputSplit) {
    if (!(inputSplit instanceof OrcSplit)) {
      return false;
    }

    /*
     * If OrcSplit.isAcid returns true, we know for sure it is ACID.
     */
    // if (((OrcSplit) inputSplit).isAcid()) {
    //   return true;
    // }

    /*
     * Fallback for the case when OrcSplit flags do not contain hasBase and deltas
     */
    return AcidUtils.isFullAcidScan(conf);
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
  public static int getRootColumn(boolean isOriginal) {
    return isOriginal ? 0 : (OrcRecordUpdater.ROW + 1);
  }

  public static void raiseAcidTablesMustBeReadWithAcidReaderException(Configuration conf)
      throws IOException {
    String hiveInputFormat = HiveConf.getVar(conf, ConfVars.HIVEINPUTFORMAT);
    if (hiveInputFormat.equals(HiveInputFormat.class.getName())) {
      throw new IOException(ErrorMsg.ACID_TABLES_MUST_BE_READ_WITH_ACID_READER.getErrorCodedMsg());
    } else {
      throw new IOException(ErrorMsg.ACID_TABLES_MUST_BE_READ_WITH_HIVEINPUTFORMAT.getErrorCodedMsg());
    }
  }

  public static RecordReader createReaderFromFile(Reader file,
                                                  Configuration conf,
                                                  long offset, long length
                                                  ) throws IOException {
    if (AcidUtils.isFullAcidScan(conf)) {
      raiseAcidTablesMustBeReadWithAcidReaderException(conf);
    }

    /**
     * Do we have schema on read in the configuration variables?
     */
    TypeDescription schema = getDesiredRowTypeDescr(conf, false, Integer.MAX_VALUE);

    Reader.Options options = new Reader.Options(conf).range(offset, length);
    options.schema(schema);
    boolean isOriginal = isOriginal(file);
    if (schema == null) {
      schema = file.getSchema();
    }
    List<OrcProto.Type> types = OrcUtils.getOrcTypes(schema);
    options.include(genIncludedColumns(schema, conf));
    setSearchArgument(options, types, conf, isOriginal);
    return file.rowsOptions(options, conf);
  }

  public static boolean isOriginal(Reader file) {
    return !file.hasMetadataValue(OrcRecordUpdater.ACID_KEY_INDEX_NAME);
  }

  public static boolean isOriginal(Footer footer) {
    for(OrcProto.UserMetadataItem item: footer.getMetadataList()) {
      if (item.hasName() && item.getName().equals(OrcRecordUpdater.ACID_KEY_INDEX_NAME)) {
        return true;
      }
    }
    return false;
  }


  public static boolean[] genIncludedColumns(TypeDescription readerSchema,
                                             List<Integer> included) {
    return genIncludedColumns(readerSchema, included, null);
  }

  public static boolean[] genIncludedColumns(TypeDescription readerSchema,
                                             List<Integer> included,
                                             Integer recursiveStruct) {
    boolean[] result = new boolean[readerSchema.getMaximumId() + 1];
    if (included == null) {
      Arrays.fill(result, true);
      return result;
    }
    result[0] = true;
    List<TypeDescription> children = readerSchema.getChildren();
    for (int columnNumber = 0; columnNumber < children.size(); ++columnNumber) {
      if (included.contains(columnNumber)) {
        addColumnToIncludes(children.get(columnNumber), result);
      } else if (recursiveStruct != null && recursiveStruct == columnNumber) {
        // This assumes all struct cols immediately follow struct
        List<TypeDescription> nestedChildren = children.get(columnNumber).getChildren();
        for (int columnNumberDelta = 0; columnNumberDelta < nestedChildren.size(); ++columnNumberDelta) {
          int columnNumberNested = columnNumber + 1 + columnNumberDelta;
          if (included.contains(columnNumberNested)) {
            addColumnToIncludes(nestedChildren.get(columnNumberDelta), result);
          }
        }
      }
    }

    return result;
  }

  // Mostly dup of genIncludedColumns
  public static TypeDescription[] genIncludedTypes(TypeDescription fileSchema,
      List<Integer> included, Integer recursiveStruct) {
    TypeDescription[] result = new TypeDescription[included.size()];
    List<TypeDescription> children = fileSchema.getChildren();
    for (int columnNumber = 0; columnNumber < children.size(); ++columnNumber) {
      int indexInBatchCols = included.indexOf(columnNumber);
      if (indexInBatchCols >= 0) {
        result[indexInBatchCols] = children.get(columnNumber);
      } else if (recursiveStruct != null && recursiveStruct == columnNumber) {
        // This assumes all struct cols immediately follow struct
        List<TypeDescription> nestedChildren = children.get(columnNumber).getChildren();
        for (int columnNumberDelta = 0; columnNumberDelta < nestedChildren.size(); ++columnNumberDelta) {
          int columnNumberNested = columnNumber + 1 + columnNumberDelta;
          int nestedIxInBatchCols = included.indexOf(columnNumberNested);
          if (nestedIxInBatchCols >= 0) {
            result[nestedIxInBatchCols] = nestedChildren.get(columnNumberDelta);
          }
        }
      }
    }
    return result;
  }


  private static void addColumnToIncludes(TypeDescription child, boolean[] result) {
    for(int col = child.getId(); col <= child.getMaximumId(); ++col) {
      result[col] = true;
    }
  }

  /**
   * Reverses genIncludedColumns; produces the table columns indexes from ORC included columns.
   * @param readerSchema The ORC reader schema for the table.
   * @param included The included ORC columns.
   * @param isFullColumnMatch Whether full column match should be enforced (i.e. whether to expect
   *          that all the sub-columns or a complex type column should be included or excluded
   *          together in the included array. If false, any sub-column being included for a complex
   *          type is sufficient for the entire complex column to be included in the result.
   * @return The list of table column indexes.
   */
  public static List<Integer> genIncludedColumnsReverse(
      TypeDescription readerSchema, boolean[] included, boolean isFullColumnMatch) {
    assert included != null;
    List<Integer> result = new ArrayList<>();
    List<TypeDescription> children = readerSchema.getChildren();
    for (int columnNumber = 0; columnNumber < children.size(); ++columnNumber) {
      TypeDescription child = children.get(columnNumber);
      int id = child.getId();
      int maxId = child.getMaximumId();
      if (id >= included.length || maxId >= included.length) {
        throw new AssertionError("Inconsistent includes: " + included.length
            + " elements; found column ID " + id);
      }
      boolean isIncluded = included[id];
      for (int col = id + 1; col <= maxId; ++col) {
        if (isFullColumnMatch && included[col] != isIncluded) {
          throw new AssertionError("Inconsistent includes: root column IDs are [" + id + ", "
              + maxId + "]; included[" + col + "] = " + included[col] + ", which is different "
              + " from the previous IDs of the same root column.");
        }
        isIncluded = isIncluded || included[col];
      }
      if (isIncluded) {
        result.add(columnNumber);
      }
    }
    return result;
  }

  /**
   * Take the configuration and figure out which columns we need to include.
   * @param readerSchema the types for the reader
   * @param conf the configuration
   */
  static boolean[] genIncludedColumns(TypeDescription readerSchema,
                                             Configuration conf) {
     if (!ColumnProjectionUtils.isReadAllColumns(conf)) {
      List<Integer> included = ColumnProjectionUtils.getReadColumnIDs(conf);
      return genIncludedColumns(readerSchema, included);
    } else {
      return null;
    }
  }

  private static String[] getSargColumnNames(String[] originalColumnNames,
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

    if (LOG.isInfoEnabled()) {
      LOG.info("ORC pushdown predicate: " + sarg);
    }
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
    String colNames = getNeededColumnNamesString(conf);
    if (colNames == null) {
      return null;
    }
    return extractNeededColNames(types, colNames, include, isOriginal);
  }

  private static String[] extractNeededColNames(
      List<OrcProto.Type> types, String columnNamesString, boolean[] include, boolean isOriginal) {
    return getSargColumnNames(columnNamesString.split(","), types, include, isOriginal);
  }

  static String getNeededColumnNamesString(Configuration conf) {
    return conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
  }

  static String getSargColumnIDsString(Configuration conf) {
    return conf.getBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, true) ? null
        : conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR);
  }

  @Override
  public boolean validateInput(FileSystem fs, HiveConf conf,
                               List<FileStatus> files
                              ) throws IOException {

    if (Utilities.getIsVectorized(conf)) {
      return new VectorizedOrcInputFormat().validateInput(fs, conf, files);
    }

    if (files.size() <= 0) {
      return false;
    }
    for (FileStatus file : files) {
      if (!HiveConf.getVar(conf, ConfVars.HIVE_EXECUTION_ENGINE).equals("mr")) {
        // 0 length files cannot be ORC files, not valid for MR.
        if (file.getLen() == 0) {
          return false;
        }
      }
      try {
        OrcFile.createReader(file.getPath(),
            OrcFile.readerOptions(conf).filesystem(fs).maxLength(file.getLen()));
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

    // We store all caches in variables to change the main one based on config.
    // This is not thread safe between different split generations (and wasn't anyway).
    private FooterCache footerCache;
    private static LocalCache localCache;
    private static ExternalCache metaCache;
    static ExecutorService threadPool = null;
    private final int numBuckets;
    private final int splitStrategyBatchMs;
    private final long maxSize;
    private final long minSize;
    private final int etlFileThreshold;
    private final boolean footerInSplits;
    private final boolean cacheStripeDetails;
    private final boolean forceThreadpool;
    private final AtomicInteger cacheHitCounter = new AtomicInteger(0);
    private final AtomicInteger numFilesCounter = new AtomicInteger(0);
    private final ValidWriteIdList writeIdList;
    private SplitStrategyKind splitStrategyKind;
    private final SearchArgument sarg;
    private final AcidOperationalProperties acidOperationalProperties;

    Context(Configuration conf) throws IOException {
      this(conf, 1, null);
    }

    Context(Configuration conf, final int minSplits) throws IOException {
      this(conf, minSplits, null);
    }

    @VisibleForTesting
    Context(Configuration conf, final int minSplits, ExternalFooterCachesByConf efc)
        throws IOException {
      this.conf = conf;
      this.forceThreadpool = HiveConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST);
      this.sarg = ConvertAstToSearchArg.createFromConf(conf);
      minSize = HiveConf.getLongVar(conf, ConfVars.MAPREDMINSPLITSIZE, DEFAULT_MIN_SPLIT_SIZE);
      maxSize = HiveConf.getLongVar(conf, ConfVars.MAPREDMAXSPLITSIZE, DEFAULT_MAX_SPLIT_SIZE);
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
      splitStrategyBatchMs = HiveConf.getIntVar(conf, ConfVars.HIVE_ORC_SPLIT_DIRECTORY_BATCH_MS);
      LOG.debug("Number of buckets specified by conf file is " + numBuckets);
      long cacheMemSize = HiveConf.getSizeVar(
          conf, ConfVars.HIVE_ORC_CACHE_STRIPE_DETAILS_MEMORY_SIZE);
      int numThreads = HiveConf.getIntVar(conf, ConfVars.HIVE_ORC_COMPUTE_SPLITS_NUM_THREADS);
      boolean useSoftReference = HiveConf.getBoolVar(
          conf, ConfVars.HIVE_ORC_CACHE_USE_SOFT_REFERENCES);

      cacheStripeDetails = (cacheMemSize > 0);

      this.etlFileThreshold = minSplits <= 0 ? DEFAULT_ETL_FILE_THRESHOLD : minSplits;

      synchronized (Context.class) {
        if (threadPool == null) {
          threadPool = Executors.newFixedThreadPool(numThreads,
              new ThreadFactoryBuilder().setDaemon(true)
                  .setNameFormat("ORC_GET_SPLITS #%d").build());
        }

        // TODO: local cache is created once, so the configs for future queries will not be honored.
        if (cacheStripeDetails) {
          // Note that there's no FS check here; we implicitly only use metastore cache for
          // HDFS, because only HDFS would return fileIds for us. If fileId is extended using
          // size/mod time/etc. for other FSes, we might need to check FSes explicitly because
          // using such an aggregate fileId cache is not bulletproof and should be disable-able.
          boolean useExternalCache = HiveConf.getBoolVar(
              conf, HiveConf.ConfVars.HIVE_ORC_MS_FOOTER_CACHE_ENABLED);
          if (useExternalCache) {
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                "Turning off hive.orc.splits.ms.footer.cache.enabled since it is not fully supported yet");
            }
            useExternalCache = false;
          }
          if (localCache == null) {
            localCache = new LocalCache(numThreads, cacheMemSize, useSoftReference);
          }
          if (useExternalCache) {
            if (metaCache == null) {
              metaCache = new ExternalCache(localCache,
                  efc == null ? new MetastoreExternalCachesByConf() : efc);
            }
            assert conf instanceof HiveConf;
            metaCache.configure((HiveConf)conf);
          }
          // Set footer cache for current split generation. See field comment - not thread safe.
          // TODO: we should be able to enable caches separately
          footerCache = useExternalCache ? metaCache : localCache;
        }
      }

      // Determine the transactional_properties of the table from the job conf stored in context.
      // The table properties are copied to job conf at HiveInputFormat::addSplitsForGroup(),
      // & therefore we should be able to retrieve them here and determine appropriate behavior.
      // Note that this will be meaningless for non-acid tables & will be set to null.
      //this is set by Utilities.copyTablePropertiesToConf()
      boolean isTxnTable = conf.getBoolean(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, false);
      String txnProperties = conf.get(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES);
      this.acidOperationalProperties = isTxnTable
          ? AcidOperationalProperties.parseString(txnProperties) : null;

      String value = conf.get(ValidWriteIdList.VALID_WRITEIDS_KEY);
      writeIdList = value == null ? new ValidReaderWriteIdList() : new ValidReaderWriteIdList(value);
      LOG.debug("Context:: Read ValidWriteIdList: " + writeIdList.toString()
              + " isTransactionalTable: " + isTxnTable + " properties: " + txnProperties);
    }

    @VisibleForTesting
    static int getCurrentThreadPoolSize() {
      synchronized (Context.class) {
        return (threadPool instanceof ThreadPoolExecutor)
            ? ((ThreadPoolExecutor)threadPool).getPoolSize() : ((threadPool == null) ? 0 : -1);
      }
    }

    @VisibleForTesting
    public static void resetThreadPool() {
      synchronized (Context.class) {
        threadPool = null;
      }
    }

    @VisibleForTesting
    public static void clearLocalCache() {
      if (localCache == null) return;
      localCache.clear();
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
    AcidDirInfo(FileSystem fs, Path splitPath, Directory acidInfo,
        List<AcidBaseFileInfo> baseFiles,
        List<ParsedDelta> deleteEvents) {
      this.splitPath = splitPath;
      this.acidInfo = acidInfo;
      this.baseFiles = baseFiles;
      this.fs = fs;
      this.deleteEvents = deleteEvents;
    }

    final FileSystem fs;
    final Path splitPath;
    final AcidUtils.Directory acidInfo;
    final List<AcidBaseFileInfo> baseFiles;
    final List<ParsedDelta> deleteEvents;

    /**
     * No (qualifying) data files found in {@link #splitPath}
     * @return
     */
    boolean isEmpty() {
      return (baseFiles == null || baseFiles.isEmpty());
    }
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
    private final OrcTail orcTail;
    private final List<OrcProto.Type> readerTypes;
    private final boolean isOriginal;
    private final List<DeltaMetaData> deltas;
    private final boolean hasBase;
    private final ByteBuffer ppdResult;

    SplitInfo(Context context, FileSystem fs, HdfsFileStatusWithId fileWithId, OrcTail orcTail,
        List<OrcProto.Type> readerTypes, boolean isOriginal, List<DeltaMetaData> deltas,
        boolean hasBase, Path dir, boolean[] covered, ByteBuffer ppdResult) throws IOException {
      super(dir, context.numBuckets, deltas, covered, context.acidOperationalProperties);
      this.context = context;
      this.fs = fs;
      this.fileWithId = fileWithId;
      this.orcTail = orcTail;
      this.readerTypes = readerTypes;
      this.isOriginal = isOriginal;
      this.deltas = deltas;
      this.hasBase = hasBase;
      this.ppdResult = ppdResult;
    }

    @VisibleForTesting
    public SplitInfo(Context context, FileSystem fs, FileStatus fileStatus, OrcTail orcTail,
        List<OrcProto.Type> readerTypes,  boolean isOriginal, ArrayList<DeltaMetaData> deltas,
        boolean hasBase, Path dir, boolean[] covered) throws IOException {
      this(context, fs, AcidUtils.createOriginalObj(null, fileStatus),
          orcTail, readerTypes, isOriginal, deltas, hasBase, dir, covered, null);
    }
  }

  /**
   * ETL strategy is used when spending little more time in split generation is acceptable
   * (split generation reads and caches file footers).
   */
  static final class ETLSplitStrategy implements SplitStrategy<SplitInfo>, Callable<Void> {
    private static final int ETL_COMBINE_FILE_LIMIT = 500;

    private static class ETLDir {
      public ETLDir(Path dir, FileSystem fs, int fileCount) {
        this.dir = dir;
        this.fs = fs;
        this.fileCount = fileCount;
      }
      private final int fileCount;
      private final Path dir;
      private final FileSystem fs;
    }

    Context context;
    final List<ETLDir> dirs;
    List<HdfsFileStatusWithId> files;
    private final List<DeltaMetaData> deltas;
    private final boolean[] covered;
    final boolean isOriginal;
    final List<OrcProto.Type> readerTypes;
    // References to external fields for async SplitInfo generation.
    private List<Future<List<OrcSplit>>> splitFuturesRef = null;
    private List<OrcSplit> splitsRef = null;
    private final UserGroupInformation ugi;
    private final boolean allowSyntheticFileIds;
    private final boolean isDefaultFs;

    public ETLSplitStrategy(Context context, FileSystem fs, Path dir,
        List<HdfsFileStatusWithId> children, List<OrcProto.Type> readerTypes, boolean isOriginal,
        List<DeltaMetaData> deltas, boolean[] covered, UserGroupInformation ugi,
        boolean allowSyntheticFileIds, boolean isDefaultFs) {
      assert !children.isEmpty();
      this.context = context;
      this.dirs = Lists.newArrayList(new ETLDir(dir, fs, children.size()));
      this.files = children;
      this.isOriginal = isOriginal;
      this.readerTypes = readerTypes;
      this.deltas = deltas;
      this.covered = covered;
      this.ugi = ugi;
      this.allowSyntheticFileIds = allowSyntheticFileIds;
      this.isDefaultFs = isDefaultFs;
    }

    @Override
    public List<SplitInfo> getSplits() throws IOException {
      List<SplitInfo> result = new ArrayList<>(files.size());
      // Force local cache if we have deltas.
      FooterCache cache = context.cacheStripeDetails ? ((deltas == null || deltas.isEmpty())
          ? context.footerCache : Context.localCache) : null;
      if (cache != null) {
        OrcTail[] orcTails = new OrcTail[files.size()];
        ByteBuffer[] ppdResults = null;
        if (cache.hasPpd()) {
          ppdResults = new ByteBuffer[files.size()];
        }
        try {
          cache.getAndValidate(files, isOriginal, orcTails, ppdResults);
        } catch (HiveException e) {
          throw new IOException(e);
        }
        int dirIx = -1, fileInDirIx = -1, filesInDirCount = 0;
        ETLDir dir = null;
        for (int i = 0; i < files.size(); ++i) {
          if ((++fileInDirIx) == filesInDirCount) {
            dir = dirs.get(++dirIx);
            filesInDirCount = dir.fileCount;
          }
          OrcTail orcTail = orcTails[i];
          ByteBuffer ppdResult = ppdResults == null ? null : ppdResults[i];
          HdfsFileStatusWithId file = files.get(i);
          if (orcTail != null) {
            // Cached copy is valid
            context.cacheHitCounter.incrementAndGet();
          }
          // Ignore files eliminated by PPD, or of 0 length.
          if (ppdResult != FooterCache.NO_SPLIT_AFTER_PPD && file.getFileStatus().getLen() > 0) {
            result.add(new SplitInfo(context, dir.fs, file, orcTail, readerTypes,
                isOriginal, deltas, true, dir.dir, covered, ppdResult));
          }
        }
      } else {
        int dirIx = -1, fileInDirIx = -1, filesInDirCount = 0;
        ETLDir dir = null;
        for (HdfsFileStatusWithId file : files) {
          if ((++fileInDirIx) == filesInDirCount) {
            dir = dirs.get(++dirIx);
            filesInDirCount = dir.fileCount;
          }
          // ignore files of 0 length
          if (file.getFileStatus().getLen() > 0) {
            result.add(new SplitInfo(context, dir.fs, file, null, readerTypes,
                isOriginal, deltas, true, dir.dir, covered, null));
          }
        }
      }
      return result;
    }

    @Override
    public String toString() {
      if (dirs.size() == 1) {
        return ETLSplitStrategy.class.getSimpleName() + " strategy for " + dirs.get(0).dir;
      } else {
        StringBuilder sb = new StringBuilder(ETLSplitStrategy.class.getSimpleName()
            + " strategy for ");
        boolean isFirst = true;
        for (ETLDir dir : dirs) {
          if (!isFirst) sb.append(", ");
          isFirst = false;
          sb.append(dir.dir);
        }
        return sb.toString();
      }
    }

    enum CombineResult {
      YES, // Combined, all good.
      NO_AND_CONTINUE, // Don't combine with that, but may combine with others.
      NO_AND_SWAP // Don't combine with with that, and make that a base for new combines.
      // We may add NO_AND_STOP in future where combine is impossible and other should not be base.
    }

    public CombineResult combineWith(FileSystem fs, Path dir,
        List<HdfsFileStatusWithId> otherFiles, boolean isOriginal) {
      if ((files.size() + otherFiles.size()) > ETL_COMBINE_FILE_LIMIT
        || this.isOriginal != isOriginal) {//todo: what is this checking????
        return (files.size() > otherFiles.size())
            ? CombineResult.NO_AND_SWAP : CombineResult.NO_AND_CONTINUE;
      }
      // All good, combine the base/original only ETL strategies.
      files.addAll(otherFiles);
      dirs.add(new ETLDir(dir, fs, otherFiles.size()));
      return CombineResult.YES;
    }

    public Future<Void> generateSplitWork(Context context,
        List<Future<List<OrcSplit>>> splitFutures, List<OrcSplit> splits) throws IOException {
      if ((context.cacheStripeDetails && context.footerCache.isBlocking())
          || context.forceThreadpool) {
        this.splitFuturesRef = splitFutures;
        this.splitsRef = splits;
        return Context.threadPool.submit(this);
      } else {
        runGetSplitsSync(splitFutures, splits, null);
        return null;
      }
    }

    @Override
    public Void call() throws IOException {
      if (ugi == null) {
        runGetSplitsSync(splitFuturesRef, splitsRef, null);
        return null;
      }
      try {
        return ugi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            runGetSplitsSync(splitFuturesRef, splitsRef, ugi);
            return null;
          }
        });
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    private void runGetSplitsSync(List<Future<List<OrcSplit>>> splitFutures,
        List<OrcSplit> splits, UserGroupInformation ugi) throws IOException {
      UserGroupInformation tpUgi = ugi == null ? UserGroupInformation.getCurrentUser() : ugi;
      List<SplitInfo> splitInfos = getSplits();
      List<Future<List<OrcSplit>>> localListF = null;
      List<OrcSplit> localListS = null;
      for (SplitInfo splitInfo : splitInfos) {
        SplitGenerator sg = new SplitGenerator(
            splitInfo, tpUgi, allowSyntheticFileIds, isDefaultFs);
        if (!sg.isBlocking()) {
          if (localListS == null) {
            localListS = new ArrayList<>(splits.size());
          }
          // Already called in doAs, so no need to doAs here.
          localListS.addAll(sg.call());
        } else {
          if (localListF == null) {
            localListF = new ArrayList<>(splits.size());
          }
          localListF.add(Context.threadPool.submit(sg));
        }
      }
      if (localListS != null) {
        synchronized (splits) {
          splits.addAll(localListS);
        }
      }
      if (localListF != null) {
        synchronized (splitFutures) {
          splitFutures.addAll(localListF);
        }
       }
     }
   }

  /**
   * BI strategy is used when the requirement is to spend less time in split generation
   * as opposed to query execution (split generation does not read or cache file footers).
   */
  static final class BISplitStrategy extends ACIDSplitStrategy {
    private final List<HdfsFileStatusWithId> fileStatuses;
    private final boolean isOriginal;
    private final List<DeltaMetaData> deltas;
    private final FileSystem fs;
    private final Path dir;
    private final boolean allowSyntheticFileIds;
    private final boolean isDefaultFs;

    /**
     * @param dir - root of partition dir
     */
    public BISplitStrategy(Context context, FileSystem fs, Path dir,
        List<HdfsFileStatusWithId> fileStatuses, boolean isOriginal, List<DeltaMetaData> deltas,
        boolean[] covered, boolean allowSyntheticFileIds, boolean isDefaultFs) {
      super(dir, context.numBuckets, deltas, covered, context.acidOperationalProperties);
      this.fileStatuses = fileStatuses;
      this.isOriginal = isOriginal;
      this.deltas = deltas;
      this.fs = fs;
      this.dir = dir;
      this.allowSyntheticFileIds = allowSyntheticFileIds;
      this.isDefaultFs = isDefaultFs;
    }

    @Override
    public List<OrcSplit> getSplits() throws IOException {
      List<OrcSplit> splits = Lists.newArrayList();
      for (HdfsFileStatusWithId file : fileStatuses) {
        FileStatus fileStatus = file.getFileStatus();
        long logicalLen = AcidUtils.getLogicalLength(fs, fileStatus);
        if (logicalLen != 0) {
          Object fileKey = isDefaultFs ? file.getFileId() : null;
          if (fileKey == null && allowSyntheticFileIds) {
            fileKey = new SyntheticFileId(fileStatus);
          }
          TreeMap<Long, BlockLocation> blockOffsets = SHIMS.getLocationsWithOffset(fs, fileStatus);
          for (Map.Entry<Long, BlockLocation> entry : blockOffsets.entrySet()) {
            if(entry.getKey() + entry.getValue().getLength() > logicalLen) {
              //don't create splits for anything past logical EOF
              continue;
            }
            OrcSplit orcSplit = new OrcSplit(fileStatus.getPath(), fileKey, entry.getKey(),
                entry.getValue().getLength(), entry.getValue().getHosts(), null, isOriginal, true,
                deltas, -1, logicalLen, dir);
            splits.add(orcSplit);
          }
        }
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
    private List<DeltaMetaData> deltas;
    private AcidOperationalProperties acidOperationalProperties;
    /**
     * @param dir root of partition dir
     */
    ACIDSplitStrategy(Path dir, int numBuckets, List<DeltaMetaData> deltas, boolean[] covered,
        AcidOperationalProperties acidOperationalProperties) {
      this.dir = dir;
      this.deltas = deltas;
      this.acidOperationalProperties = acidOperationalProperties;
    }

    @Override
    public List<OrcSplit> getSplits() throws IOException {
      List<OrcSplit> splits = Lists.newArrayList();

      // When split-update is enabled, we do not need to account for buckets that aren't covered.
      // This is a huge performance benefit of split-update. And the reason why we are able to
      // do so is because the 'deltas' here are actually only the delete_deltas. All the insert_deltas
      // with valid user payload data has already been considered as base for the covered buckets.
      // Hence, the uncovered buckets do not have any relevant data and we can just ignore them.
      if (acidOperationalProperties != null && acidOperationalProperties.isSplitUpdate()) {
        return Collections.emptyList();
      }

      // Generate a split for any buckets that weren't covered.
      // This happens in the case where a bucket just has deltas and no
      // base.
      if (!deltas.isEmpty()) {
        //since HIVE-17089 if here, then it's not an acid table so there should never be any deltas
        throw new IllegalStateException("Found unexpected deltas: " + deltas + " in " + dir);
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
    /**
     * For plain or acid tables this is the root of the partition (or table if not partitioned).
     * For MM table this is delta/ or base/ dir.  In MM case applying of the ValidTxnList that
     * {@link AcidUtils#getAcidState(Path, Configuration, ValidWriteIdList)} normally does has already
     * been done in {@link HiveInputFormat#processPathsForMmRead(List, JobConf, ValidWriteIdList)}.
     */
    private final Path dir;
    private final Ref<Boolean> useFileIds;
    private final UserGroupInformation ugi;

    @VisibleForTesting
    FileGenerator(Context context, FileSystem fs, Path dir, boolean useFileIds,
        UserGroupInformation ugi) {
      this(context, fs, dir, Ref.from(useFileIds), ugi);
    }

    FileGenerator(Context context, FileSystem fs, Path dir, Ref<Boolean> useFileIds,
        UserGroupInformation ugi) {
      this.context = context;
      this.fs = fs;
      this.dir = dir;
      this.useFileIds = useFileIds;
      this.ugi = ugi;
    }

    @Override
    public AcidDirInfo call() throws IOException {
      if (ugi == null) {
        return callInternal();
      }
      try {
        return ugi.doAs(new PrivilegedExceptionAction<AcidDirInfo>() {
          @Override
          public AcidDirInfo run() throws Exception {
            return callInternal();
          }
        });
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    private AcidDirInfo callInternal() throws IOException {
      if (context.acidOperationalProperties != null
          && context.acidOperationalProperties.isInsertOnly()) {
        // See the class comment - HIF handles MM for all input formats, so if we try to handle it
        // again, in particular for the non-recursive originals-only getSplits call, we will just
        // get confused. This bypass was not necessary when MM tables didn't support originals. Now
        // that they do, we use this path for anything MM table related, although everything except
        // the originals could still be handled by AcidUtils like a regular non-txn table.
        boolean isRecursive = context.conf.getBoolean(FileInputFormat.INPUT_DIR_RECURSIVE,
            context.conf.getBoolean("mapred.input.dir.recursive", false));
        List<HdfsFileStatusWithId> originals = new ArrayList<>();
        List<AcidBaseFileInfo> baseFiles = new ArrayList<>();
        AcidUtils.findOriginals(fs, fs.getFileStatus(dir), originals, useFileIds, true, isRecursive);
        for (HdfsFileStatusWithId fileId : originals) {
          baseFiles.add(new AcidBaseFileInfo(fileId, AcidUtils.AcidBaseFileType.ORIGINAL_BASE));
        }
        return new AcidDirInfo(fs, dir, new AcidUtils.DirectoryImpl(Lists.newArrayList(), true, originals,
            Lists.newArrayList(), Lists.newArrayList(), null), baseFiles, new ArrayList<>());
      }
      //todo: shouldn't ignoreEmptyFiles be set based on ExecutionEngine?
      AcidUtils.Directory dirInfo = AcidUtils.getAcidState(
          dir, context.conf, context.writeIdList, useFileIds, true, null);
      // find the base files (original or new style)
      List<AcidBaseFileInfo> baseFiles = new ArrayList<>();
      if (dirInfo.getBaseDirectory() == null) {
        // For non-acid tables (or paths), all data files are in getOriginalFiles() list
        for (HdfsFileStatusWithId fileId : dirInfo.getOriginalFiles()) {
          baseFiles.add(new AcidBaseFileInfo(fileId, AcidUtils.AcidBaseFileType.ORIGINAL_BASE));
        }
      } else {
        List<HdfsFileStatusWithId> compactedBaseFiles = findBaseFiles(dirInfo.getBaseDirectory(), useFileIds);
        for (HdfsFileStatusWithId fileId : compactedBaseFiles) {
          baseFiles.add(new AcidBaseFileInfo(fileId, dirInfo.isBaseInRawFormat() ?
            AcidUtils.AcidBaseFileType.ORIGINAL_BASE : AcidUtils.AcidBaseFileType.ACID_SCHEMA));
        }
      }

      // Find the parsed deltas- some of them containing only the insert delta events
      // may get treated as base if split-update is enabled for ACID. (See HIVE-14035 for details)
      List<ParsedDelta> parsedDeltas = new ArrayList<>();
      if (context.acidOperationalProperties != null &&
          context.acidOperationalProperties.isSplitUpdate()) {
        // If we have split-update turned on for this table, then the delta events have already been
        // split into two directories- delta_x_y/ and delete_delta_x_y/.
        // When you have split-update turned on, the insert events go to delta_x_y/ directory and all
        // the delete events go to delete_x_y/. An update event will generate two events-
        // a delete event for the old record that is put into delete_delta_x_y/,
        // followed by an insert event for the updated record put into the usual delta_x_y/.
        // Therefore, everything inside delta_x_y/ is an insert event and all the files in delta_x_y/
        // can be treated like base files. Hence, each of these are added to baseOrOriginalFiles list.

        for (ParsedDelta parsedDelta : dirInfo.getCurrentDirectories()) {
          if (parsedDelta.isDeleteDelta()) {
            parsedDeltas.add(parsedDelta);
          } else {
            AcidUtils.AcidBaseFileType deltaType = parsedDelta.isRawFormat() ?
              AcidUtils.AcidBaseFileType.ORIGINAL_BASE : AcidUtils.AcidBaseFileType.ACID_SCHEMA;
            PathFilter bucketFilter = parsedDelta.isRawFormat() ?
              AcidUtils.originalBucketFilter : AcidUtils.bucketFileFilter;
            if (parsedDelta.isRawFormat() && parsedDelta.getMinWriteId() != parsedDelta.getMaxWriteId()) {
              //delta/ with files in raw format are a result of Load Data (as opposed to compaction
              //or streaming ingest so must have interval length == 1.
              throw new IllegalStateException("Delta in " + AcidUtils.AcidBaseFileType.ORIGINAL_BASE
               + " format but txnIds are out of range: " + parsedDelta.getPath());
            }
            // This is a normal insert delta, which only has insert events and hence all the files
            // in this delta directory can be considered as a base.
            Boolean val = useFileIds.value;
            if (val == null || val) {
              try {
                List<HdfsFileStatusWithId> insertDeltaFiles =
                    SHIMS.listLocatedHdfsStatus(fs, parsedDelta.getPath(), bucketFilter);
                for (HdfsFileStatusWithId fileId : insertDeltaFiles) {
                  baseFiles.add(new AcidBaseFileInfo(fileId, deltaType));
                }
                if (val == null) {
                  useFileIds.value = true; // The call succeeded, so presumably the API is there.
                }
                continue; // move on to process to the next parsedDelta.
              } catch (Throwable t) {
                LOG.error("Failed to get files with ID; using regular API: " + t.getMessage());
                if (val == null && t instanceof UnsupportedOperationException) {
                  useFileIds.value = false;
                }
              }
            }
            // Fall back to regular API and create statuses without ID.
            List<FileStatus> children = HdfsUtils.listLocatedStatus(fs, parsedDelta.getPath(), bucketFilter);
            for (FileStatus child : children) {
              HdfsFileStatusWithId fileId = AcidUtils.createOriginalObj(null, child);
              baseFiles.add(new AcidBaseFileInfo(fileId, deltaType));
            }
          }
        }

      } else {
        /*
        We already handled all delete deltas above and there should not be any other deltas for
        any table type.  (this was acid 1.0 code path).
         */
        assert dirInfo.getCurrentDirectories().isEmpty() :
            "Non empty curDir list?!: " + dirInfo.getCurrentDirectories();
        // When split-update is not enabled, then all the deltas in the current directories
        // should be considered as usual.
        parsedDeltas.addAll(dirInfo.getCurrentDirectories());
      }
      return new AcidDirInfo(fs, dir, dirInfo, baseFiles, parsedDeltas);
    }

    private List<HdfsFileStatusWithId> findBaseFiles(
        Path base, Ref<Boolean> useFileIds) throws IOException {
      Boolean val = useFileIds.value;
      if (val == null || val) {
        try {
          List<HdfsFileStatusWithId> result = SHIMS.listLocatedHdfsStatus(
              fs, base, AcidUtils.hiddenFileFilter);
          if (val == null) {
            useFileIds.value = true; // The call succeeded, so presumably the API is there.
          }
          return result;
        } catch (Throwable t) {
          LOG.error("Failed to get files with ID; using regular API: " + t.getMessage());
          if (val == null && t instanceof UnsupportedOperationException) {
            useFileIds.value = false;
          }
        }
      }

      // Fall back to regular API and create states without ID.
      List<FileStatus> children = HdfsUtils.listLocatedStatus(fs, base, AcidUtils.hiddenFileFilter);
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
    private final FileStatus file;
    private final Long fsFileId;
    private final long blockSize;
    private final TreeMap<Long, BlockLocation> locations;
    private OrcTail orcTail;
    private List<OrcProto.Type> readerTypes;
    private List<StripeInformation> stripes;
    private List<StripeStatistics> stripeStats;
    private List<OrcProto.Type> fileTypes;
    private boolean[] readerIncluded;    // The included columns of the reader / file schema that
                                         // include ACID columns if present.
    private final boolean isOriginal;
    private final List<DeltaMetaData> deltas;
    private final boolean hasBase;
    private OrcFile.WriterVersion writerVersion;
    private long projColsUncompressedSize;
    private final List<OrcSplit> deltaSplits;
    private final ByteBuffer ppdResult;
    private final UserGroupInformation ugi;
    private final boolean allowSyntheticFileIds;
    private SchemaEvolution evolution;
    //this is the root of the partition in which the 'file' is located
    private final Path rootDir;

    public SplitGenerator(SplitInfo splitInfo, UserGroupInformation ugi,
        boolean allowSyntheticFileIds, boolean isDefaultFs) throws IOException {
      this.ugi = ugi;
      this.context = splitInfo.context;
      this.fs = splitInfo.fs;
      this.file = splitInfo.fileWithId.getFileStatus();
      this.fsFileId = isDefaultFs ? splitInfo.fileWithId.getFileId() : null;
      this.blockSize = this.file.getBlockSize();
      this.orcTail = splitInfo.orcTail;
      this.readerTypes = splitInfo.readerTypes;
      // TODO: potential DFS call
      this.locations = SHIMS.getLocationsWithOffset(fs, file);
      this.isOriginal = splitInfo.isOriginal;
      this.deltas = splitInfo.deltas;
      this.hasBase = splitInfo.hasBase;
      this.rootDir = splitInfo.dir;
      this.projColsUncompressedSize = -1;
      this.deltaSplits = splitInfo.getSplits();
      this.allowSyntheticFileIds = allowSyntheticFileIds;
      this.ppdResult = splitInfo.ppdResult;
    }

    public boolean isBlocking() {
      return true;
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
     * @param orcTail orc tail
     * @throws IOException
     */
    OrcSplit createSplit(long offset, long length, OrcTail orcTail) throws IOException {
      String[] hosts;
      Map.Entry<Long, BlockLocation> startEntry = locations.floorEntry(offset);
      BlockLocation start = startEntry.getValue();
      if (offset + length <= start.getOffset() + start.getLength()) {
        // handle the single block case
        hosts = start.getHosts();
      } else {
        Map.Entry<Long, BlockLocation> endEntry = locations.floorEntry(offset + length);
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

      // scale the raw data size to split level based on ratio of split wrt to file length
      final long fileLen = file.getLen();
      final double splitRatio = (double) length / (double) fileLen;
      final long scaledProjSize = projColsUncompressedSize > 0 ?
          (long) (splitRatio * projColsUncompressedSize) : fileLen;
      Object fileKey = fsFileId;
      if (fileKey == null && allowSyntheticFileIds) {
        fileKey = new SyntheticFileId(file);
      }
      return new OrcSplit(file.getPath(), fileKey, offset, length, hosts,
          orcTail, isOriginal, hasBase, deltas, scaledProjSize, fileLen, rootDir);
    }

    private static final class OffsetAndLength { // Java cruft; pair of long.
      public OffsetAndLength() {
        this.offset = -1;
        this.length = 0;
      }

      long offset, length;

      @Override
      public String toString() {
        return "[offset=" + offset + ", length=" + length + "]";
      }
    }

    /**
     * Divide the adjacent stripes in the file into input splits based on the
     * block size and the configured minimum and maximum sizes.
     */
    @Override
    public List<OrcSplit> call() throws IOException {
      if (ugi == null) {
        return callInternal();
      }
      try {
        return ugi.doAs(new PrivilegedExceptionAction<List<OrcSplit>>() {
          @Override
          public List<OrcSplit> run() throws Exception {
            return callInternal();
          }
        });
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    private List<OrcSplit> callInternal() throws IOException {
      // Figure out which stripes we need to read.
      if (ppdResult != null) {
        assert deltaSplits.isEmpty();
        assert ppdResult.hasArray();

        // TODO: when PB is upgraded to 2.6, newInstance(ByteBuffer) method should be used here.
        CodedInputStream cis = CodedInputStream.newInstance(
            ppdResult.array(), ppdResult.arrayOffset(), ppdResult.remaining());
        cis.setSizeLimit(InStream.PROTOBUF_MESSAGE_MAX_LIMIT);
        return generateSplitsFromPpd(SplitInfos.parseFrom(cis));
      } else {
        populateAndCacheStripeDetails();
        boolean[] includeStripe = null;
        // We can't eliminate stripes if there are deltas because the
        // deltas may change the rows making them match the predicate. todo: See HIVE-14516.
        if ((deltas == null || deltas.isEmpty()) && context.sarg != null) {
          String[] colNames =
              extractNeededColNames((readerTypes == null ? fileTypes : readerTypes),
                  context.conf, readerIncluded, isOriginal);
          if (colNames == null) {
            LOG.warn("Skipping split elimination for {} as column names is null", file.getPath());
          } else {
            includeStripe = pickStripes(context.sarg, writerVersion,
                stripeStats, stripes.size(), file.getPath(), evolution);
          }
        }
        return generateSplitsFromStripes(includeStripe);
      }
    }

    private List<OrcSplit> generateSplitsFromPpd(SplitInfos ppdResult) throws IOException {
      OffsetAndLength current = new OffsetAndLength();
      List<OrcSplit> splits = new ArrayList<>(ppdResult.getInfosCount());
      int lastIdx = -1;
      for (Metastore.SplitInfo si : ppdResult.getInfosList()) {
        int index = si.getIndex();
        if (lastIdx >= 0 && lastIdx + 1 != index && current.offset != -1) {
          // Create split for the previous unfinished stripe.
          splits.add(createSplit(current.offset, current.length, orcTail));
          current.offset = -1;
        }
        lastIdx = index;
        String debugStr = null;
        if (LOG.isDebugEnabled()) {
          debugStr = current.toString();
        }
        current = generateOrUpdateSplit(splits, current, si.getOffset(), si.getLength(), null);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Updated split from {" + index + ": " + si.getOffset() + ", "
              + si.getLength() + "} and "+ debugStr + " to " + current);
        }
      }
      generateLastSplit(splits, current, null);
      return splits;
    }

    private List<OrcSplit> generateSplitsFromStripes(boolean[] includeStripe) throws IOException {
      List<OrcSplit> splits = new ArrayList<>(stripes.size());
      // if we didn't have predicate pushdown, read everything
      if (includeStripe == null) {
        includeStripe = new boolean[stripes.size()];
        Arrays.fill(includeStripe, true);
      }

      OffsetAndLength current = new OffsetAndLength();
      int idx = -1;
      for (StripeInformation stripe : stripes) {
        idx++;

        if (!includeStripe[idx]) {
          // create split for the previous unfinished stripe
          if (current.offset != -1) {
            splits.add(createSplit(current.offset, current.length, orcTail));
            current.offset = -1;
          }
          continue;
        }

        current = generateOrUpdateSplit(
            splits, current, stripe.getOffset(), stripe.getLength(), orcTail);
      }
      generateLastSplit(splits, current, orcTail);

      // Add uncovered ACID delta splits.
      splits.addAll(deltaSplits);
      return splits;
    }

    private OffsetAndLength generateOrUpdateSplit(
        List<OrcSplit> splits, OffsetAndLength current, long offset,
        long length, OrcTail orcTail) throws IOException {
      // if we are working on a stripe, over the min stripe size, and
      // crossed a block boundary, cut the input split here.
      if (current.offset != -1 && current.length > context.minSize &&
          (current.offset / blockSize != offset / blockSize)) {
        splits.add(createSplit(current.offset, current.length, orcTail));
        current.offset = -1;
      }
      // if we aren't building a split, start a new one.
      if (current.offset == -1) {
        current.offset = offset;
        current.length = length;
      } else {
        current.length = (offset + length) - current.offset;
      }
      if (current.length >= context.maxSize) {
        splits.add(createSplit(current.offset, current.length, orcTail));
        current.offset = -1;
      }
      return current;
    }

    private void generateLastSplit(List<OrcSplit> splits, OffsetAndLength current,
        OrcTail orcTail) throws IOException {
      if (current.offset == -1) return;
      splits.add(createSplit(current.offset, current.length, orcTail));
    }

    private void populateAndCacheStripeDetails() throws IOException {
      // When reading the file for first time we get the orc tail from the orc reader and cache it
      // in the footer cache. Subsequent requests will get the orc tail from the cache (if file
      // length and modification time is not changed) and populate the split info. If the split info
      // object contains the orc tail from the cache then we can skip creating orc reader avoiding
      // filesystem calls.
      if (orcTail == null) {
        Reader orcReader = OrcFile.createReader(file.getPath(),
            OrcFile.readerOptions(context.conf)
                .filesystem(fs)
                .maxLength(AcidUtils.getLogicalLength(fs, file)));
        orcTail = new OrcTail(orcReader.getFileTail(), orcReader.getSerializedFileFooter(),
            file.getModificationTime());
        if (context.cacheStripeDetails) {
          context.footerCache.put(new FooterCacheKey(fsFileId, file.getPath()), orcTail);
        }
      }
      stripes = orcTail.getStripes();
      stripeStats = orcTail.getStripeStatistics();
      fileTypes = orcTail.getTypes();
      TypeDescription fileSchema = OrcUtils.convertTypeFromProtobuf(fileTypes, 0);
      Reader.Options readerOptions = new Reader.Options(context.conf);
      if (readerTypes == null) {
        readerIncluded = genIncludedColumns(fileSchema, context.conf);
        evolution = new SchemaEvolution(fileSchema, null, readerOptions.include(readerIncluded));
      } else {
        // The reader schema always comes in without ACID columns.
        TypeDescription readerSchema = OrcUtils.convertTypeFromProtobuf(readerTypes, 0);
        readerIncluded = genIncludedColumns(readerSchema, context.conf);
        evolution = new SchemaEvolution(fileSchema, readerSchema, readerOptions.include(readerIncluded));
        if (!isOriginal) {
          // The SchemaEvolution class has added the ACID metadata columns.  Let's update our
          // readerTypes so PPD code will work correctly.
          readerTypes = OrcUtils.getOrcTypes(evolution.getReaderSchema());
        }
      }
      writerVersion = orcTail.getWriterVersion();
      List<OrcProto.ColumnStatistics> fileColStats = orcTail.getFooter().getStatisticsList();
      boolean[] fileIncluded;
      if (readerTypes == null) {
        fileIncluded = readerIncluded;
      } else {
        fileIncluded = new boolean[fileTypes.size()];
        final int readerSchemaSize = readerTypes.size();
        for (int i = 0; i < readerSchemaSize; i++) {
          TypeDescription fileType = evolution.getFileType(i);
          if (fileType != null) {
            fileIncluded[fileType.getId()] = true;
          }
        }
      }
      projColsUncompressedSize = computeProjectionSize(fileTypes, fileColStats, fileIncluded);
      if (!context.footerInSplits) {
        orcTail = null;
      }
    }

    private long computeProjectionSize(List<OrcProto.Type> fileTypes,
        List<OrcProto.ColumnStatistics> stats, boolean[] fileIncluded) {
      List<Integer> internalColIds = Lists.newArrayList();
      if (fileIncluded == null) {
        // Add all.
        for (int i = 0; i < fileTypes.size(); i++) {
          internalColIds.add(i);
        }
      } else {
        for (int i = 0; i < fileIncluded.length; i++) {
          if (fileIncluded[i]) {
            internalColIds.add(i);
          }
        }
      }
      return ReaderImpl.getRawDataSizeFromColIndices(internalColIds, fileTypes, stats);
    }
  }

  public static boolean[] shiftReaderIncludedForAcid(boolean[] included) {
    // We always need the base row
    included[0] = true;
    boolean[] newIncluded = new boolean[included.length + OrcRecordUpdater.FIELDS];
    Arrays.fill(newIncluded, 0, OrcRecordUpdater.FIELDS, true);
    for (int i = 0; i < included.length; ++i) {
      newIncluded[i + OrcRecordUpdater.FIELDS] = included[i];
    }
    return newIncluded;
  }

  /** Class intended to update two values from methods... Java-related cruft. */
  @VisibleForTesting
  static final class CombinedCtx {
    ETLSplitStrategy combined;
    long combineStartUs;
  }

  static List<OrcSplit> generateSplitsInfo(Configuration conf, Context context)
      throws IOException {
    if (LOG.isInfoEnabled()) {
      LOG.info("ORC pushdown predicate: " + context.sarg);
    }
    boolean useFileIdsConfig = HiveConf.getBoolVar(
        conf, ConfVars.HIVE_ORC_INCLUDE_FILE_ID_IN_SPLITS);
    // Sharing this state assumes splits will succeed or fail to get it together (same FS).
    // We also start with null and only set it to true on the first call, so we would only do
    // the global-disable thing on the first failure w/the API error, not any random failure.
    Ref<Boolean> useFileIds = Ref.from(useFileIdsConfig ? null : false);
    boolean allowSyntheticFileIds = useFileIdsConfig && HiveConf.getBoolVar(
        conf, ConfVars.HIVE_ORC_ALLOW_SYNTHETIC_FILE_ID_IN_SPLITS);
    List<OrcSplit> splits = Lists.newArrayList();
    List<Future<AcidDirInfo>> pathFutures = Lists.newArrayList();
    List<Future<Void>> strategyFutures = Lists.newArrayList();
    final List<Future<List<OrcSplit>>> splitFutures = Lists.newArrayList();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    // multi-threaded file statuses and split strategy
    Path[] paths = getInputPaths(conf);
    CompletionService<AcidDirInfo> ecs = new ExecutorCompletionService<>(Context.threadPool);
    for (Path dir : paths) {
      FileSystem fs = dir.getFileSystem(conf);
      FileGenerator fileGenerator = new FileGenerator(context, fs, dir, useFileIds, ugi);
      pathFutures.add(ecs.submit(fileGenerator));
    }

    boolean isAcidTableScan = AcidUtils.isFullAcidScan(conf);
    boolean isSchemaEvolution = HiveConf.getBoolVar(conf, ConfVars.HIVE_SCHEMA_EVOLUTION);
    TypeDescription readerSchema =
        OrcInputFormat.getDesiredRowTypeDescr(conf, isAcidTableScan, Integer.MAX_VALUE);
    List<OrcProto.Type> readerTypes = null;
    if (readerSchema != null) {
      readerTypes = OrcUtils.getOrcTypes(readerSchema);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Generate splits schema evolution property " + isSchemaEvolution +
        " reader schema " + (readerSchema == null ? "NULL" : readerSchema.toString()) +
        " ACID scan property " + isAcidTableScan);
    }

    // complete path futures and schedule split generation
    try {
      CombinedCtx combinedCtx = (context.splitStrategyBatchMs > 0) ? new CombinedCtx() : null;
      long maxWaitUs = context.splitStrategyBatchMs * 1000000;
      int resultsLeft = paths.length;
      while (resultsLeft > 0) {
        AcidDirInfo adi = null;
        if (combinedCtx != null && combinedCtx.combined != null) {
          long waitTimeUs = combinedCtx.combineStartUs + maxWaitUs - System.nanoTime();
          if (waitTimeUs >= 0) {
            Future<AcidDirInfo> f = ecs.poll(waitTimeUs, TimeUnit.NANOSECONDS);
            adi = (f == null) ? null : f.get();
          }
        } else {
          adi = ecs.take().get();
        }

        if (adi == null) {
          // We were combining SS-es and the time has expired.
          assert combinedCtx.combined != null;
          scheduleSplits(combinedCtx.combined, context, splitFutures, strategyFutures, splits);
          combinedCtx.combined = null;
          continue;
        }

        // We have received a new directory information, make split strategies.
        --resultsLeft;
        if(adi.isEmpty()) {
          //no files found, for example empty table/partition
          continue;
        }
        // The reason why we can get a list of split strategies here is because for ACID split-update
        // case when we have a mix of original base files & insert deltas, we will produce two
        // independent split strategies for them. There is a global flag 'isOriginal' that is set
        // on a per split strategy basis and it has to be same for all the files in that strategy.
        List<SplitStrategy<?>> splitStrategies = determineSplitStrategies(combinedCtx, context, adi.fs,
            adi.splitPath, adi.baseFiles, adi.deleteEvents, readerTypes, ugi,
            allowSyntheticFileIds);

        for (SplitStrategy<?> splitStrategy : splitStrategies) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Split strategy: {}", splitStrategy);
          }

          // Hack note - different split strategies return differently typed lists, yay Java.
          // This works purely by magic, because we know which strategy produces which type.
          if (splitStrategy instanceof ETLSplitStrategy) {
            scheduleSplits((ETLSplitStrategy)splitStrategy,
                context, splitFutures, strategyFutures, splits);
          } else {
            @SuppressWarnings("unchecked")
            List<OrcSplit> readySplits = (List<OrcSplit>)splitStrategy.getSplits();
            splits.addAll(readySplits);
          }
        }
      }

      // Run the last combined strategy, if any.
      if (combinedCtx != null && combinedCtx.combined != null) {
        scheduleSplits(combinedCtx.combined, context, splitFutures, strategyFutures, splits);
        combinedCtx.combined = null;
      }

      // complete split futures
      for (Future<Void> ssFuture : strategyFutures) {
         ssFuture.get(); // Make sure we get exceptions strategies might have thrown.
      }
      // All the split strategies are done, so it must be safe to access splitFutures.
      for (Future<List<OrcSplit>> splitFuture : splitFutures) {
        splits.addAll(splitFuture.get());
      }
    } catch (Exception e) {
      cancelFutures(pathFutures);
      cancelFutures(strategyFutures);
      cancelFutures(splitFutures);
      throw new RuntimeException("ORC split generation failed with exception: " + e.getMessage(), e);
    }

    if (context.cacheStripeDetails) {
      LOG.info("FooterCacheHitRatio: " + context.cacheHitCounter.get() + "/"
          + context.numFilesCounter.get());
    }

    if (LOG.isDebugEnabled()) {
      for (OrcSplit split : splits) {
        LOG.debug(split + " projected_columns_uncompressed_size: "
            + split.getColumnarProjectionSize());
      }
    }
    return splits;
  }

  @VisibleForTesting
  // We could have this as a protected method w/no class, but half of Hive is static, so there.
  public static class ContextFactory {
    public Context create(Configuration conf, int numSplits) throws IOException {
      return new Context(conf, numSplits);
    }
  }

  private static void scheduleSplits(ETLSplitStrategy splitStrategy, Context context,
      List<Future<List<OrcSplit>>> splitFutures, List<Future<Void>> strategyFutures,
      List<OrcSplit> splits) throws IOException {
    Future<Void> ssFuture = splitStrategy.generateSplitWork(context, splitFutures, splits);
    if (ssFuture == null) return;
    strategyFutures.add(ssFuture);
  }

  private static <T> void cancelFutures(List<Future<T>> futures) {
    for (Future<T> future : futures) {
      future.cancel(true);
    }
  }

  private static SplitStrategy<?> combineOrCreateETLStrategy(CombinedCtx combinedCtx,
      Context context, FileSystem fs, Path dir, List<HdfsFileStatusWithId> files,
      List<DeltaMetaData> deltas, boolean[] covered, List<OrcProto.Type> readerTypes,
      boolean isOriginal, UserGroupInformation ugi, boolean allowSyntheticFileIds,
      boolean isDefaultFs) {
    if (!deltas.isEmpty() || combinedCtx == null) {
      //why is this checking for deltas.isEmpty() - HIVE-18110
      return new ETLSplitStrategy(
          context, fs, dir, files, readerTypes, isOriginal, deltas, covered, ugi,
          allowSyntheticFileIds, isDefaultFs);
    } else if (combinedCtx.combined == null) {
      combinedCtx.combined = new ETLSplitStrategy(
          context, fs, dir, files, readerTypes, isOriginal, deltas, covered, ugi,
          allowSyntheticFileIds, isDefaultFs);
      combinedCtx.combineStartUs = System.nanoTime();
      return null;
    } else {
      ETLSplitStrategy.CombineResult r =
          combinedCtx.combined.combineWith(fs, dir, files, isOriginal);
      switch (r) {
      case YES: return null;
      case NO_AND_CONTINUE:
        return new ETLSplitStrategy(
            context, fs, dir, files, readerTypes, isOriginal, deltas, covered, ugi,
            allowSyntheticFileIds, isDefaultFs);
      case NO_AND_SWAP: {
        ETLSplitStrategy oldBase = combinedCtx.combined;
        combinedCtx.combined = new ETLSplitStrategy(
            context, fs, dir, files, readerTypes, isOriginal, deltas, covered, ugi,
            allowSyntheticFileIds, isDefaultFs);
        combinedCtx.combineStartUs = System.nanoTime();
        return oldBase;
      }
      default: throw new AssertionError("Unknown result " + r);
      }
    }
  }

  @Override
  public InputSplit[] getSplits(JobConf job,
                                int numSplits) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("getSplits started");
    }
    Configuration conf = job;
    if (HiveConf.getBoolVar(job, HiveConf.ConfVars.HIVE_ORC_MS_FOOTER_CACHE_ENABLED)) {
      // Create HiveConf once, since this is expensive.
      conf = new HiveConf(conf, OrcInputFormat.class);
    }
    List<OrcSplit> result = generateSplitsInfo(conf,
        new Context(conf, numSplits, createExternalCaches()));
    if (LOG.isDebugEnabled()) {
      LOG.debug("getSplits finished");
    }
    return result.toArray(new InputSplit[result.size()]);
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
    //CombineHiveInputFormat may produce FileSplit that is not OrcSplit
    boolean vectorMode = Utilities.getIsVectorized(conf);
    boolean isAcidRead = isFullAcidRead(conf, inputSplit);
    if (!isAcidRead) {
      if (vectorMode) {
        return createVectorizedReader(inputSplit, conf, reporter);
      } else {
        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf);
        if (inputSplit instanceof OrcSplit) {
          OrcSplit split = (OrcSplit) inputSplit;
          readerOptions.maxLength(split.getFileLength()).orcTail(split.getOrcTail());
        }
        return new OrcRecordReader(OrcFile.createReader(
            ((FileSplit) inputSplit).getPath(),
            readerOptions),
            conf, (FileSplit) inputSplit);
      }
    }

    reporter.setStatus(inputSplit.toString());
    //if here we are now doing an Acid read so must have OrcSplit.  CombineHiveInputFormat is
    // disabled for acid path
    OrcSplit split = (OrcSplit) inputSplit;

    if (vectorMode) {
      return (org.apache.hadoop.mapred.RecordReader)
          new VectorizedOrcAcidRowBatchReader(split, conf, reporter);
    }

    Options options = new Options(conf).reporter(reporter);
    final RowReader<OrcStruct> inner = getReader(inputSplit, options);
    // Non-vectorized regular ACID reader.
    return new NullKeyRecordReader(inner, conf);
  }

  /**
   * Return a RecordReader that is compatible with the Hive 0.12 reader
   * with NullWritable for the key instead of RecordIdentifier.
   */
  public static final class NullKeyRecordReader implements AcidRecordReader<NullWritable, OrcStruct> {
    private final RecordIdentifier id;
    private final RowReader<OrcStruct> inner;

    @Override
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
                                        Options options)
                                            throws IOException {

    final OrcSplit split = (OrcSplit) inputSplit;

    // Retrieve the acidOperationalProperties for the table, initialized in HiveInputFormat.
    AcidUtils.AcidOperationalProperties acidOperationalProperties
            = AcidUtils.getAcidOperationalProperties(options.getConfiguration());
    if(!acidOperationalProperties.isSplitUpdate()) {
      throw new IllegalStateException("Expected SpliUpdate table: " + split.getPath());
    }

    final Path[] deltas = VectorizedOrcAcidRowBatchReader.getDeleteDeltaDirsFromSplit(split);
    final Configuration conf = options.getConfiguration();

    final Reader reader = OrcInputFormat.createOrcReaderForSplit(conf, split);
    OrcRawRecordMerger.Options mergerOptions = new OrcRawRecordMerger.Options().isCompacting(false);
    mergerOptions.rootPath(split.getRootDir());
    mergerOptions.bucketPath(split.getPath());
    final int bucket;
    if (split.hasBase()) {
      AcidOutputFormat.Options acidIOOptions =
        AcidUtils.parseBaseOrDeltaBucketFilename(split.getPath(), conf);
      if(acidIOOptions.getBucketId() < 0) {
        LOG.warn("Can't determine bucket ID for " + split.getPath() + "; ignoring");
      }
      bucket = acidIOOptions.getBucketId();
      if(split.isOriginal()) {
        mergerOptions.copyIndex(acidIOOptions.getCopyNumber()).bucketPath(split.getPath());
      }
    } else {
      bucket = (int) split.getStart();
      assert false : "We should never have a split w/o base in acid 2.0 for full acid: " + split.getPath();
    }
    //todo: createOptionsForReader() assumes it's !isOriginal.... why?
    final Reader.Options readOptions = OrcInputFormat.createOptionsForReader(conf);
    readOptions.range(split.getStart(), split.getLength());

    String txnString = conf.get(ValidWriteIdList.VALID_WRITEIDS_KEY);
    ValidWriteIdList validWriteIdList
            = (txnString == null) ? new ValidReaderWriteIdList() : new ValidReaderWriteIdList(txnString);
    if (LOG.isDebugEnabled()) {
      LOG.debug("getReader:: Read ValidWriteIdList: " + validWriteIdList.toString()
            + " isTransactionalTable: " + HiveConf.getBoolVar(conf, ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN));
      LOG.debug("Creating merger for {} and {}", split.getPath(), Arrays.toString(deltas));
    }
    final OrcRawRecordMerger records =
        new OrcRawRecordMerger(conf, true, reader, split.isOriginal(), bucket,
            validWriteIdList, readOptions, deltas, mergerOptions);
    return new RowReader<OrcStruct>() {
      OrcStruct innerRecord = records.createValue();

      @Override
      public ObjectInspector getObjectInspector() {
        return OrcStruct.createObjectInspector(0, OrcUtils.getOrcTypes(readOptions.getSchema()));
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

  static Reader.Options createOptionsForReader(Configuration conf) {
    /**
     * Do we have schema on read in the configuration variables?
     */
    TypeDescription schema =
        OrcInputFormat.getDesiredRowTypeDescr(conf, true, Integer.MAX_VALUE);
    Reader.Options readerOptions = new Reader.Options(conf).schema(schema);
    // TODO: Convert genIncludedColumns and setSearchArgument to use TypeDescription.
    final List<OrcProto.Type> schemaTypes = OrcUtils.getOrcTypes(schema);
    readerOptions.include(OrcInputFormat.genIncludedColumns(schema, conf));
    //todo: last param is bogus. why is this hardcoded?
    OrcInputFormat.setSearchArgument(readerOptions, schemaTypes, conf, true);
    return readerOptions;
  }

  static Reader createOrcReaderForSplit(Configuration conf, OrcSplit orcSplit) throws IOException {
    Path path = orcSplit.getPath();
    Reader reader;
    if (orcSplit.hasBase()) {
      OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf);
      readerOptions.maxLength(orcSplit.getFileLength());
      if (orcSplit.hasFooter()) {
        readerOptions.orcTail(orcSplit.getOrcTail());
      }
      reader = OrcFile.createReader(path, readerOptions);
    } else {
      reader = null;
    }
    return reader;
  }

  public static boolean[] pickStripesViaTranslatedSarg(SearchArgument sarg,
      OrcFile.WriterVersion writerVersion, List<OrcProto.Type> types,
      List<StripeStatistics> stripeStats, int stripeCount) throws FileFormatException {
    LOG.info("Translated ORC pushdown predicate: " + sarg);
    assert sarg != null;
    if (stripeStats == null || writerVersion == OrcFile.WriterVersion.ORIGINAL) {
      return null; // only do split pruning if HIVE-8732 has been fixed in the writer
    }
    // eliminate stripes that doesn't satisfy the predicate condition
    List<PredicateLeaf> sargLeaves = sarg.getLeaves();
    int[] filterColumns = RecordReaderImpl.mapTranslatedSargColumns(types, sargLeaves);
    TypeDescription schema = OrcUtils.convertTypeFromProtobuf(types, 0);
    SchemaEvolution evolution = new SchemaEvolution(schema, null);
    return pickStripesInternal(sarg, filterColumns, stripeStats, stripeCount, null, evolution);
  }

  private static boolean[] pickStripes(SearchArgument sarg,
                                       OrcFile.WriterVersion writerVersion,
                                       List<StripeStatistics> stripeStats,
      int stripeCount, Path filePath, final SchemaEvolution evolution) {
    if (sarg == null || stripeStats == null || writerVersion == OrcFile.WriterVersion.ORIGINAL) {
      return null; // only do split pruning if HIVE-8732 has been fixed in the writer
    }
    // eliminate stripes that doesn't satisfy the predicate condition
    List<PredicateLeaf> sargLeaves = sarg.getLeaves();
    int[] filterColumns = RecordReaderImpl.mapSargColumnsToOrcInternalColIdx(sargLeaves,
        evolution);
    return pickStripesInternal(sarg, filterColumns, stripeStats, stripeCount, filePath, evolution);
  }

  private static boolean[] pickStripesInternal(SearchArgument sarg, int[] filterColumns,
      List<StripeStatistics> stripeStats, int stripeCount, Path filePath,
      final SchemaEvolution evolution) {
    boolean[] includeStripe = new boolean[stripeCount];
    for (int i = 0; i < includeStripe.length; ++i) {
      includeStripe[i] = (i >= stripeStats.size()) ||
          isStripeSatisfyPredicate(stripeStats.get(i), sarg, filterColumns, evolution);
      if (LOG.isDebugEnabled() && !includeStripe[i]) {
        LOG.debug("Eliminating ORC stripe-" + i + " of file '" + filePath
            + "'  as it did not satisfy predicate condition.");
      }
    }
    return includeStripe;
  }

  private static boolean isStripeSatisfyPredicate(
      StripeStatistics stripeStatistics, SearchArgument sarg, int[] filterColumns,
      final SchemaEvolution evolution) {
    List<PredicateLeaf> predLeaves = sarg.getLeaves();
    TruthValue[] truthValues = new TruthValue[predLeaves.size()];
    for (int pred = 0; pred < truthValues.length; pred++) {
      if (filterColumns[pred] != -1) {
        if (evolution != null && !evolution.isPPDSafeConversion(filterColumns[pred])) {
          truthValues[pred] = TruthValue.YES_NO_NULL;
        } else {
          // column statistics at index 0 contains only the number of rows
          ColumnStatistics stats = stripeStatistics.getColumnStatistics()[filterColumns[pred]];
          // if row count is 0 and where there are no nulls it means index is disabled and we don't have stats
          if (stats.getNumberOfValues() == 0 && !stats.hasNull()) {
            truthValues[pred] = TruthValue.YES_NO_NULL;
            continue;
          }
          PredicateLeaf leaf = predLeaves.get(pred);
          try {
            truthValues[pred] = RecordReaderImpl.evaluatePredicate(stats, leaf, null);
          } catch (NoDynamicValuesException dve) {
            LOG.debug("Dynamic values are not available here {}", dve.getMessage());
            boolean hasNulls = stats.hasNull() || leaf.getOperator() != Operator.NULL_SAFE_EQUALS;
            truthValues[pred] = hasNulls ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
          }
        }
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
  static List<SplitStrategy<?>> determineSplitStrategies(CombinedCtx combinedCtx, Context context,
      FileSystem fs, Path dir,
      List<AcidBaseFileInfo> baseFiles,
      List<ParsedDelta> parsedDeltas,
      List<OrcProto.Type> readerTypes,
      UserGroupInformation ugi, boolean allowSyntheticFileIds) throws IOException {
    List<SplitStrategy<?>> splitStrategies = new ArrayList<SplitStrategy<?>>();
    SplitStrategy<?> splitStrategy;

    boolean checkDefaultFs = HiveConf.getBoolVar(
        context.conf, ConfVars.LLAP_CACHE_DEFAULT_FS_FILE_ID);
    boolean isDefaultFs = (!checkDefaultFs) || ((fs instanceof DistributedFileSystem)
            && HdfsUtils.isDefaultFs((DistributedFileSystem) fs));

    if (baseFiles.isEmpty()) {
      assert false : "acid 2.0 no base?!: " + dir;
      splitStrategy = determineSplitStrategy(combinedCtx, context, fs, dir, Collections.emptyList(),
          false, parsedDeltas, readerTypes, ugi, allowSyntheticFileIds, isDefaultFs);
      if (splitStrategy != null) {
        splitStrategies.add(splitStrategy);
      }
      return splitStrategies;
    }

    List<HdfsFileStatusWithId> acidSchemaFiles = new ArrayList<>();
    List<HdfsFileStatusWithId> originalSchemaFiles = new ArrayList<HdfsFileStatusWithId>();
    // Separate the base files into acid schema and non-acid(original) schema files.
    for (AcidBaseFileInfo acidBaseFileInfo : baseFiles) {
      if (acidBaseFileInfo.isOriginal()) {
        originalSchemaFiles.add(acidBaseFileInfo.getHdfsFileStatusWithId());
      } else {
        assert acidBaseFileInfo.isAcidSchema();
        acidSchemaFiles.add(acidBaseFileInfo.getHdfsFileStatusWithId());
      }
    }

    // Generate split strategy for non-acid schema original files, if any.
    if (!originalSchemaFiles.isEmpty()) {
      splitStrategy = determineSplitStrategy(combinedCtx, context, fs, dir, originalSchemaFiles,
          true, parsedDeltas, readerTypes, ugi, allowSyntheticFileIds, isDefaultFs);
      if (splitStrategy != null) {
        splitStrategies.add(splitStrategy);
      }
    }

    // Generate split strategy for acid schema files, if any.
    if (!acidSchemaFiles.isEmpty()) {
      splitStrategy = determineSplitStrategy(combinedCtx, context, fs, dir, acidSchemaFiles,
          false, parsedDeltas, readerTypes, ugi, allowSyntheticFileIds, isDefaultFs);
      if (splitStrategy != null) {
        splitStrategies.add(splitStrategy);
      }
    }

    return splitStrategies;
  }

  private static SplitStrategy<?> determineSplitStrategy(CombinedCtx combinedCtx, Context context,
      FileSystem fs, Path dir,
      List<HdfsFileStatusWithId> baseFiles,
      boolean isOriginal,
      List<ParsedDelta> parsedDeltas,
      List<OrcProto.Type> readerTypes,
      UserGroupInformation ugi, boolean allowSyntheticFileIds, boolean isDefaultFs) {
    List<DeltaMetaData> deltas = AcidUtils.serializeDeltas(parsedDeltas);
    boolean[] covered = new boolean[context.numBuckets];

    // if we have a base to work from
    if (!baseFiles.isEmpty()) {
      long totalFileSize = 0;
      for (HdfsFileStatusWithId child : baseFiles) {
        totalFileSize += child.getFileStatus().getLen();
        int b = AcidUtils.parseBucketId(child.getFileStatus().getPath());
        // If the bucket is in the valid range, mark it as covered.
        // I wish Hive actually enforced bucketing all of the time.
        if (b >= 0 && b < covered.length) {
          covered[b] = true;
        }
      }

      int numFiles = baseFiles.size();
      long avgFileSize = totalFileSize / numFiles;
      int totalFiles = context.numFilesCounter.addAndGet(numFiles);
      switch(context.splitStrategyKind) {
        case BI:
          // BI strategy requested through config
          return new BISplitStrategy(context, fs, dir, baseFiles,
              isOriginal, deltas, covered, allowSyntheticFileIds, isDefaultFs);
        case ETL:
          // ETL strategy requested through config
          return combineOrCreateETLStrategy(combinedCtx, context, fs, dir, baseFiles,
              deltas, covered, readerTypes, isOriginal, ugi, allowSyntheticFileIds, isDefaultFs);
        default:
          // HYBRID strategy
          if (avgFileSize > context.maxSize || totalFiles <= context.etlFileThreshold) {
            return combineOrCreateETLStrategy(combinedCtx, context, fs, dir, baseFiles,
                deltas, covered, readerTypes, isOriginal, ugi, allowSyntheticFileIds, isDefaultFs);
          } else {
            return new BISplitStrategy(context, fs, dir, baseFiles,
                isOriginal, deltas, covered, allowSyntheticFileIds, isDefaultFs);
          }
      }
    } else {
      // no base, only deltas
      return new ACIDSplitStrategy(dir, context.numBuckets, deltas, covered,
          context.acidOperationalProperties);
    }
  }

  /**
   *
   * @param bucket bucket/writer ID for this split of the compaction job
   */
  @Override
  public RawReader<OrcStruct> getRawReader(Configuration conf,
                                           boolean collapseEvents,
                                           int bucket,
                                           ValidWriteIdList validWriteIdList,
                                           Path baseDirectory,
                                           Path[] deltaDirectory
                                           ) throws IOException {
    boolean isOriginal = false;
    OrcRawRecordMerger.Options mergerOptions = new OrcRawRecordMerger.Options().isCompacting(true)
      .isMajorCompaction(collapseEvents);
    if (baseDirectory != null) {//this is NULL for minor compaction
      //it may also be null if there is no base - only deltas
      mergerOptions.baseDir(baseDirectory);
      if (baseDirectory.getName().startsWith(AcidUtils.BASE_PREFIX)) {
        isOriginal = AcidUtils.MetaDataFile.isRawFormat(baseDirectory, baseDirectory.getFileSystem(conf));
        mergerOptions.rootPath(baseDirectory.getParent());
      } else {
        isOriginal = true;
        mergerOptions.rootPath(baseDirectory);
      }
    }
    else {
      //since we have no base, there must be at least 1 delta which must a result of acid write
      //so it must be immediate child of the partition
      mergerOptions.rootPath(deltaDirectory[0].getParent());
    }
    return new OrcRawRecordMerger(conf, collapseEvents, null, isOriginal,
        bucket, validWriteIdList, new Reader.Options(conf), deltaDirectory, mergerOptions);
  }

  /**
   * Represents footer cache.
   */
  public interface FooterCache {
    ByteBuffer NO_SPLIT_AFTER_PPD = ByteBuffer.wrap(new byte[0]);

    void getAndValidate(List<HdfsFileStatusWithId> files, boolean isOriginal,
        OrcTail[] result, ByteBuffer[] ppdResult) throws IOException, HiveException;
    boolean hasPpd();
    boolean isBlocking();
    void put(FooterCacheKey cacheKey, OrcTail orcTail) throws IOException;
  }

  public static class FooterCacheKey {
    Long fileId; // used by external cache
    Path path; // used by local cache

    FooterCacheKey(Long fileId, Path path) {
      this.fileId = fileId;
      this.path = path;
    }

    public Long getFileId() {
      return fileId;
    }

    public Path getPath() {
      return path;
    }
  }
  /**
   * Convert a Hive type property string that contains separated type names into a list of
   * TypeDescription objects.
   * @param hiveTypeProperty the desired types from hive
   * @param maxColumns the maximum number of desired columns
   * @return the list of TypeDescription objects.
   */
  public static ArrayList<TypeDescription>
      typeDescriptionsFromHiveTypeProperty(String hiveTypeProperty,
                                           int maxColumns) {

    // CONSDIER: We need a type name parser for TypeDescription.

    ArrayList<TypeInfo> typeInfoList = TypeInfoUtils.getTypeInfosFromTypeString(hiveTypeProperty);
    ArrayList<TypeDescription> typeDescrList =new ArrayList<TypeDescription>(typeInfoList.size());
    for (TypeInfo typeInfo : typeInfoList) {
      typeDescrList.add(convertTypeInfo(typeInfo));
      if (typeDescrList.size() >= maxColumns) {
        break;
      }
    }
    return typeDescrList;
  }

  public static TypeDescription convertTypeInfo(TypeInfo info) {
    switch (info.getCategory()) {
      case PRIMITIVE: {
        PrimitiveTypeInfo pinfo = (PrimitiveTypeInfo) info;
        switch (pinfo.getPrimitiveCategory()) {
          case BOOLEAN:
            return TypeDescription.createBoolean();
          case BYTE:
            return TypeDescription.createByte();
          case SHORT:
            return TypeDescription.createShort();
          case INT:
            return TypeDescription.createInt();
          case LONG:
            return TypeDescription.createLong();
          case FLOAT:
            return TypeDescription.createFloat();
          case DOUBLE:
            return TypeDescription.createDouble();
          case STRING:
            return TypeDescription.createString();
          case DATE:
            return TypeDescription.createDate();
          case TIMESTAMP:
            return TypeDescription.createTimestamp();
          case BINARY:
            return TypeDescription.createBinary();
          case DECIMAL: {
            DecimalTypeInfo dinfo = (DecimalTypeInfo) pinfo;
            return TypeDescription.createDecimal()
                .withScale(dinfo.getScale())
                .withPrecision(dinfo.getPrecision());
          }
          case VARCHAR: {
            BaseCharTypeInfo cinfo = (BaseCharTypeInfo) pinfo;
            return TypeDescription.createVarchar()
                .withMaxLength(cinfo.getLength());
          }
          case CHAR: {
            BaseCharTypeInfo cinfo = (BaseCharTypeInfo) pinfo;
            return TypeDescription.createChar()
                .withMaxLength(cinfo.getLength());
          }
          default:
            throw new IllegalArgumentException("ORC doesn't handle primitive" +
                " category " + pinfo.getPrimitiveCategory());
        }
      }
      case LIST: {
        ListTypeInfo linfo = (ListTypeInfo) info;
        return TypeDescription.createList
            (convertTypeInfo(linfo.getListElementTypeInfo()));
      }
      case MAP: {
        MapTypeInfo minfo = (MapTypeInfo) info;
        return TypeDescription.createMap
            (convertTypeInfo(minfo.getMapKeyTypeInfo()),
                convertTypeInfo(minfo.getMapValueTypeInfo()));
      }
      case UNION: {
        UnionTypeInfo minfo = (UnionTypeInfo) info;
        TypeDescription result = TypeDescription.createUnion();
        for (TypeInfo child: minfo.getAllUnionObjectTypeInfos()) {
          result.addUnionChild(convertTypeInfo(child));
        }
        return result;
      }
      case STRUCT: {
        StructTypeInfo sinfo = (StructTypeInfo) info;
        TypeDescription result = TypeDescription.createStruct();
        for(String fieldName: sinfo.getAllStructFieldNames()) {
          result.addField(fieldName,
              convertTypeInfo(sinfo.getStructFieldTypeInfo(fieldName)));
        }
        return result;
      }
      default:
        throw new IllegalArgumentException("ORC doesn't handle " +
            info.getCategory());
    }
  }

  /**
   * Generate the desired schema for reading the file.
   * @param conf the configuration
   * @param isAcidRead is this an acid format?
   * @param dataColumns the desired number of data columns for vectorized read
   * @return the desired schema or null if schema evolution isn't enabled
   * @throws IllegalArgumentException
   */
  public static TypeDescription getDesiredRowTypeDescr(Configuration conf,
                                                       boolean isAcidRead,
                                                       int dataColumns) {

    String columnNameProperty = null;
    String columnTypeProperty = null;

    ArrayList<String> schemaEvolutionColumnNames = null;
    ArrayList<TypeDescription> schemaEvolutionTypeDescrs = null;

    boolean haveSchemaEvolutionProperties = false;
    if (isAcidRead || HiveConf.getBoolVar(conf, ConfVars.HIVE_SCHEMA_EVOLUTION) ) {

      columnNameProperty = conf.get(IOConstants.SCHEMA_EVOLUTION_COLUMNS);
      columnTypeProperty = conf.get(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES);

      haveSchemaEvolutionProperties =
          (columnNameProperty != null && columnTypeProperty != null);

      if (haveSchemaEvolutionProperties) {
        schemaEvolutionColumnNames = Lists.newArrayList(columnNameProperty.split(","));
        if (schemaEvolutionColumnNames.size() == 0) {
          haveSchemaEvolutionProperties = false;
        } else {
          schemaEvolutionTypeDescrs =
              typeDescriptionsFromHiveTypeProperty(columnTypeProperty,
                  dataColumns);
          if (schemaEvolutionTypeDescrs.size() !=
              Math.min(dataColumns, schemaEvolutionColumnNames.size())) {
            haveSchemaEvolutionProperties = false;
          }
        }
      } else if (isAcidRead) {
        throw new IllegalArgumentException(ErrorMsg.SCHEMA_REQUIRED_TO_READ_ACID_TABLES.getErrorCodedMsg());
      }
    }

    if (haveSchemaEvolutionProperties) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using schema evolution configuration variables schema.evolution.columns " +
            schemaEvolutionColumnNames.toString() +
            " / schema.evolution.columns.types " +
            schemaEvolutionTypeDescrs.toString() +
            " (isAcidRead " + isAcidRead + ")");
      }
    } else {

      // Try regular properties;
      columnNameProperty = conf.get(serdeConstants.LIST_COLUMNS);
      columnTypeProperty = conf.get(serdeConstants.LIST_COLUMN_TYPES);
      if (columnTypeProperty == null || columnNameProperty == null) {
        return null;
      }

      schemaEvolutionColumnNames = Lists.newArrayList(columnNameProperty.split(","));
      if (schemaEvolutionColumnNames.size() == 0) {
        return null;
      }
      schemaEvolutionTypeDescrs =
          typeDescriptionsFromHiveTypeProperty(columnTypeProperty, dataColumns);
      if (schemaEvolutionTypeDescrs.size() !=
          Math.min(dataColumns, schemaEvolutionColumnNames.size())) {
        return null;
      }

      // Find first virtual column and clip them off.
      int virtualColumnClipNum = -1;
      int columnNum = 0;
      for (String columnName : schemaEvolutionColumnNames) {
        if (VirtualColumn.VIRTUAL_COLUMN_NAMES.contains(columnName)) {
          virtualColumnClipNum = columnNum;
          break;
        }
        columnNum++;
      }
      if (virtualColumnClipNum != -1 && virtualColumnClipNum < dataColumns) {
        schemaEvolutionColumnNames =
            Lists.newArrayList(schemaEvolutionColumnNames.subList(0, virtualColumnClipNum));
        schemaEvolutionTypeDescrs = Lists.newArrayList(schemaEvolutionTypeDescrs.subList(0, virtualColumnClipNum));
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Using column configuration variables columns " +
                schemaEvolutionColumnNames.toString() +
                " / columns.types " +
                schemaEvolutionTypeDescrs.toString() +
                " (isAcidRead " + isAcidRead + ")");
      }
    }

    // Desired schema does not include virtual columns or partition columns.
    TypeDescription result = TypeDescription.createStruct();
    for (int i = 0; i < schemaEvolutionTypeDescrs.size(); i++) {
      result.addField(schemaEvolutionColumnNames.get(i), schemaEvolutionTypeDescrs.get(i));
    }

    return result;
  }

  @VisibleForTesting
  protected ExternalFooterCachesByConf createExternalCaches() {
    return null; // The default ones are created in case of null; tests override this.
  }


  @Override
  public BatchToRowReader<?, ?> getWrapper(
      org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch> vrr,
      VectorizedRowBatchCtx vrbCtx, List<Integer> includedCols) {
    return new OrcOiBatchToRowReader(vrr, vrbCtx, includedCols);
  }
}
