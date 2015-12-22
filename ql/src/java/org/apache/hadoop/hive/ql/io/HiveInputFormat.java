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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.io.HiveIOExceptionHandlerUtil;
import org.apache.hadoop.hive.llap.io.api.LlapIo;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.exec.spark.SparkDynamicPartitionPruner;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hive.common.util.ReflectionUtil;

/**
 * HiveInputFormat is a parameterized InputFormat which looks at the path name
 * and determine the correct InputFormat for that path name from
 * mapredPlan.pathToPartitionInfo(). It can be used to read files with different
 * input format in the same map-reduce job.
 */
public class HiveInputFormat<K extends WritableComparable, V extends Writable>
    implements InputFormat<K, V>, JobConfigurable {

  private static final String CLASS_NAME = HiveInputFormat.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  /**
   * A cache of InputFormat instances.
   */
  private static Map<Class, InputFormat<WritableComparable, Writable>> inputFormats
    = new ConcurrentHashMap<Class, InputFormat<WritableComparable, Writable>>();

  private JobConf job;

  // both classes access by subclasses
  protected Map<String, PartitionDesc> pathToPartitionInfo;
  protected MapWork mrwork;

  /**
   * HiveInputSplit encapsulates an InputSplit with its corresponding
   * inputFormatClass. The reason that it derives from FileSplit is to make sure
   * "map.input.file" in MapTask.
   */
  public static class HiveInputSplit extends FileSplit implements InputSplit,
      Configurable {

    InputSplit inputSplit;
    String inputFormatClassName;

    public HiveInputSplit() {
      // This is the only public constructor of FileSplit
      super((Path) null, 0, 0, (String[]) null);
    }

    public HiveInputSplit(InputSplit inputSplit, String inputFormatClassName) {
      // This is the only public constructor of FileSplit
      super((Path) null, 0, 0, (String[]) null);
      this.inputSplit = inputSplit;
      this.inputFormatClassName = inputFormatClassName;
    }

    public InputSplit getInputSplit() {
      return inputSplit;
    }

    public String inputFormatClassName() {
      return inputFormatClassName;
    }

    @Override
    public Path getPath() {
      if (inputSplit instanceof FileSplit) {
        return ((FileSplit) inputSplit).getPath();
      }
      return new Path("");
    }

    /** The position of the first byte in the file to process. */
    @Override
    public long getStart() {
      if (inputSplit instanceof FileSplit) {
        return ((FileSplit) inputSplit).getStart();
      }
      return 0;
    }

    @Override
    public String toString() {
      return inputFormatClassName + ":" + inputSplit.toString();
    }

    @Override
    public long getLength() {
      long r = 0;
      try {
        r = inputSplit.getLength();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return r;
    }

    @Override
    public String[] getLocations() throws IOException {
      return inputSplit.getLocations();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      String inputSplitClassName = in.readUTF();
      try {
        inputSplit = (InputSplit) ReflectionUtil.newInstance(conf
            .getClassByName(inputSplitClassName), conf);
      } catch (Exception e) {
        throw new IOException(
            "Cannot create an instance of InputSplit class = "
            + inputSplitClassName + ":" + e.getMessage(), e);
      }
      inputSplit.readFields(in);
      inputFormatClassName = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(inputSplit.getClass().getName());
      inputSplit.write(out);
      out.writeUTF(inputFormatClassName);
    }

    Configuration conf;

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }
  }

  public void configure(JobConf job) {
    this.job = job;
  }

  public static InputFormat<WritableComparable, Writable> wrapForLlap(
      InputFormat<WritableComparable, Writable> inputFormat, Configuration conf) {
    if (!HiveConf.getBoolVar(conf, ConfVars.LLAP_IO_ENABLED)) {
      return inputFormat; // LLAP not enabled, no-op.
    }
    boolean isSupported = inputFormat instanceof LlapWrappableInputFormatInterface,
        isVector = Utilities.isVectorMode(conf);
    if (!isSupported || !isVector) {
      LOG.info("Not using llap for " + inputFormat + ": " + isSupported + ", " + isVector);
      return inputFormat;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Wrapping " + inputFormat);
    }
    @SuppressWarnings("unchecked")
    LlapIo<VectorizedRowBatch> llapIo = LlapProxy.getIo();
    if (llapIo == null) {
      LOG.info("Not using LLAP IO because it is not initialized");
      return inputFormat;
    }
    return castInputFormat(llapIo.getInputFormat(inputFormat));
  }

  public static boolean isLlapEnabled(Configuration conf) {
    // Don't check IO - it needn't be initialized on client.
    return HiveConf.getBoolVar(conf, ConfVars.LLAP_IO_ENABLED);
  }

  public static boolean canWrapAnyForLlap(Configuration conf, MapWork mapWork) {
    return Utilities.isVectorMode(conf, mapWork);
  }

  public static boolean canWrapForLlap(Class<? extends InputFormat> inputFormatClass) {
    return LlapWrappableInputFormatInterface.class.isAssignableFrom(inputFormatClass);
  }

  @SuppressWarnings("unchecked")
  private static <T, U, V, W> InputFormat<T, U> castInputFormat(InputFormat<V, W> from) {
    // This is ugly in two ways...
    // 1) We assume that LlapWrappableInputFormatInterface has NullWritable as first parameter.
    //    Since we are using Java and not, say, a programming language, there's no way to check.
    // 2) We ignore the fact that 2nd arg is completely incompatible (VRB -> Writable), because
    //    vectorization currently works by magic, getting VRB from IF with non-VRB value param.
    // So we just cast blindly and hope for the best (which is obviously what happens).
    return (InputFormat<T, U>)from;
  }


  public static InputFormat<WritableComparable, Writable> getInputFormatFromCache(
    Class inputFormatClass, JobConf job) throws IOException {
    InputFormat<WritableComparable, Writable> instance = inputFormats.get(inputFormatClass);
    if (instance == null) {
      try {
        instance = (InputFormat<WritableComparable, Writable>) ReflectionUtil
            .newInstance(inputFormatClass, job);
        // HBase input formats are not thread safe today. See HIVE-8808.
        String inputFormatName = inputFormatClass.getName().toLowerCase();
        if (!inputFormatName.contains("hbase")) {
          inputFormats.put(inputFormatClass, instance);
        }
      } catch (Exception e) {
        throw new IOException("Cannot create an instance of InputFormat class "
            + inputFormatClass.getName() + " as specified in mapredWork!", e);
      }
    }
    return wrapForLlap(instance, job);
  }

  public RecordReader getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    HiveInputSplit hsplit = (HiveInputSplit) split;

    InputSplit inputSplit = hsplit.getInputSplit();
    String inputFormatClassName = null;
    Class inputFormatClass = null;
    try {
      inputFormatClassName = hsplit.inputFormatClassName();
      inputFormatClass = job.getClassByName(inputFormatClassName);
    } catch (Exception e) {
      throw new IOException("cannot find class " + inputFormatClassName, e);
    }

    if (this.mrwork == null) {
      init(job);
    }

    boolean nonNative = false;
    PartitionDesc part = pathToPartitionInfo.get(hsplit.getPath().toString());
    if ((part != null) && (part.getTableDesc() != null)) {
      Utilities.copyTableJobPropertiesToConf(part.getTableDesc(), job);
      nonNative = part.getTableDesc().isNonNative();
    }

    pushProjectionsAndFilters(job, inputFormatClass, hsplit.getPath()
      .toString(), hsplit.getPath().toUri().getPath(), nonNative);

    InputFormat inputFormat = getInputFormatFromCache(inputFormatClass, job);
    RecordReader innerReader = null;
    try {
      innerReader = inputFormat.getRecordReader(inputSplit, job, reporter);
    } catch (Exception e) {
      innerReader = HiveIOExceptionHandlerUtil
          .handleRecordReaderCreationException(e, job);
    }
    HiveRecordReader<K,V> rr = new HiveRecordReader(innerReader, job);
    rr.initIOContext(hsplit, job, inputFormatClass, innerReader);
    return rr;
  }

  protected void init(JobConf job) {
    if (mrwork == null || pathToPartitionInfo == null) {
      if (HiveConf.getVar(job, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
        mrwork = (MapWork) Utilities.getMergeWork(job);
        if (mrwork == null) {
          mrwork = Utilities.getMapWork(job);
        }
      } else {
        mrwork = Utilities.getMapWork(job);
      }

      // Prune partitions
      if (HiveConf.getVar(job, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")
          && HiveConf.getBoolVar(job, HiveConf.ConfVars.SPARK_DYNAMIC_PARTITION_PRUNING)) {
        SparkDynamicPartitionPruner pruner = new SparkDynamicPartitionPruner();
        try {
          pruner.prune(mrwork, job);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      pathToPartitionInfo = mrwork.getPathToPartitionInfo();
    }
  }

  /*
   * AddSplitsForGroup collects separate calls to setInputPaths into one where possible.
   * The reason for this is that this is faster on some InputFormats. E.g.: Orc will start
   * a threadpool to do the work and calling it multiple times unnecessarily will create a lot
   * of unnecessary thread pools.
   */
  private void addSplitsForGroup(List<Path> dirs, TableScanOperator tableScan, JobConf conf,
      InputFormat inputFormat, Class<? extends InputFormat> inputFormatClass, int splits,
      TableDesc table, List<InputSplit> result) throws IOException {

    Utilities.copyTablePropertiesToConf(table, conf);

    if (tableScan != null) {
      pushFilters(conf, tableScan);
    }

    FileInputFormat.setInputPaths(conf, dirs.toArray(new Path[dirs.size()]));
    conf.setInputFormat(inputFormat.getClass());

    int headerCount = 0;
    int footerCount = 0;
    if (table != null) {
      headerCount = Utilities.getHeaderCount(table);
      footerCount = Utilities.getFooterCount(table, conf);
      if (headerCount != 0 || footerCount != 0) {
        // Input file has header or footer, cannot be splitted.
        HiveConf.setLongVar(conf, ConfVars.MAPREDMINSPLITSIZE, Long.MAX_VALUE);
      }
    }

    InputSplit[] iss = inputFormat.getSplits(conf, splits);
    for (InputSplit is : iss) {
      result.add(new HiveInputSplit(is, inputFormatClass.getName()));
    }
  }

  Path[] getInputPaths(JobConf job) throws IOException {
    Path[] dirs;
    if (HiveConf.getVar(job, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {
      Set<String> pathStrings = mrwork.getPathToPartitionInfo().keySet();
      dirs = new Path[pathStrings.size()];
      Iterator<String> it = pathStrings.iterator();
      for (int i = 0; i < dirs.length; i++) {
        dirs[i] = new Path(it.next());
      }
    } else {
      dirs = FileInputFormat.getInputPaths(job);
      if (dirs.length == 0) {
        // on tez we're avoiding to duplicate the file info in FileInputFormat.
        if (HiveConf.getVar(job, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
          try {
            List<Path> paths = Utilities.getInputPathsTez(job, mrwork);
            dirs = paths.toArray(new Path[paths.size()]);
          } catch (Exception e) {
            throw new IOException("Could not create input files", e);
          }
        } else {
          throw new IOException("No input paths specified in job");
        }
      }
    }
    return dirs;
  }

  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.GET_SPLITS);
    init(job);
    Path[] dirs = getInputPaths(job);
    JobConf newjob = new JobConf(job);
    List<InputSplit> result = new ArrayList<InputSplit>();

    List<Path> currentDirs = new ArrayList<Path>();
    Class<? extends InputFormat> currentInputFormatClass = null;
    TableDesc currentTable = null;
    TableScanOperator currentTableScan = null;

    boolean pushDownProjection = false;
    //Buffers to hold filter pushdown information
    StringBuilder readColumnsBuffer = new StringBuilder(newjob.
      get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, ""));;
    StringBuilder readColumnNamesBuffer = new StringBuilder(newjob.
      get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, ""));
    // for each dir, get the InputFormat, and do getSplits.
    for (Path dir : dirs) {
      PartitionDesc part = getPartitionDescFromPath(pathToPartitionInfo, dir);
      Class<? extends InputFormat> inputFormatClass = part.getInputFileFormatClass();
      TableDesc table = part.getTableDesc();
      TableScanOperator tableScan = null;

      List<String> aliases = mrwork.getPathToAliases().get(dir.toString());

      // Make filter pushdown information available to getSplits.
      if ((aliases != null) && (aliases.size() == 1)) {
        Operator op = mrwork.getAliasToWork().get(aliases.get(0));
        if ((op != null) && (op instanceof TableScanOperator)) {
          tableScan = (TableScanOperator) op;
          //Reset buffers to store filter push down columns
          readColumnsBuffer.setLength(0);
          readColumnNamesBuffer.setLength(0);
          // push down projections.
          ColumnProjectionUtils.appendReadColumns(readColumnsBuffer, readColumnNamesBuffer,
            tableScan.getNeededColumnIDs(), tableScan.getNeededColumns());
          pushDownProjection = true;
          // push down filters
          pushFilters(newjob, tableScan);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("aliases: {} pathToAliases: {} dir: {}", aliases, mrwork.getPathToAliases(), dir);
        }
      }

      if (!currentDirs.isEmpty() &&
          inputFormatClass.equals(currentInputFormatClass) &&
          table.equals(currentTable) &&
          tableScan == currentTableScan) {
        currentDirs.add(dir);
        continue;
      }

      if (!currentDirs.isEmpty()) {
        if (LOG.isInfoEnabled()) {
          LOG.info("Generating splits as currentDirs is not empty. currentDirs: {}", currentDirs);
        }

        // set columns to read in conf
        if (pushDownProjection) {
          pushProjection(newjob, readColumnsBuffer, readColumnNamesBuffer);
        }

        addSplitsForGroup(currentDirs, currentTableScan, newjob,
            getInputFormatFromCache(currentInputFormatClass, job),
            currentInputFormatClass, currentDirs.size()*(numSplits / dirs.length),
            currentTable, result);
      }

      currentDirs.clear();
      currentDirs.add(dir);
      currentTableScan = tableScan;
      currentTable = table;
      currentInputFormatClass = inputFormatClass;
    }

    // set columns to read in conf
    if (pushDownProjection) {
      pushProjection(newjob, readColumnsBuffer, readColumnNamesBuffer);
    }

    if (dirs.length != 0) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Generating splits for dirs: {}", dirs);
      }
      addSplitsForGroup(currentDirs, currentTableScan, newjob,
          getInputFormatFromCache(currentInputFormatClass, job),
          currentInputFormatClass, currentDirs.size()*(numSplits / dirs.length),
          currentTable, result);
    }

    Utilities.clearWorkMapForConf(job);
    if (LOG.isInfoEnabled()) {
      LOG.info("number of splits " + result.size());
    }
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.GET_SPLITS);
    return result.toArray(new HiveInputSplit[result.size()]);
  }

  private void pushProjection(final JobConf newjob, final StringBuilder readColumnsBuffer,
      final StringBuilder readColumnNamesBuffer) {
    String readColIds = readColumnsBuffer.toString();
    String readColNames = readColumnNamesBuffer.toString();
    boolean readAllColumns = readColIds.isEmpty() ? true : false;
    newjob.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, readAllColumns);
    newjob.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, readColIds);
    newjob.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, readColNames);

    if (LOG.isInfoEnabled()) {
      LOG.info("{} = {}", ColumnProjectionUtils.READ_ALL_COLUMNS, readAllColumns);
      LOG.info("{} = {}", ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, readColIds);
      LOG.info("{} = {}", ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, readColNames);
    }
  }

  protected static PartitionDesc getPartitionDescFromPath(
      Map<String, PartitionDesc> pathToPartitionInfo, Path dir)
      throws IOException {
    PartitionDesc partDesc = pathToPartitionInfo.get(dir.toString());
    if (partDesc == null) {
      partDesc = pathToPartitionInfo.get(dir.toUri().getPath());
    }
    if (partDesc == null) {
      throw new IOException("cannot find dir = " + dir.toString()
          + " in " + pathToPartitionInfo);
    }

    return partDesc;
  }

  public static void pushFilters(JobConf jobConf, TableScanOperator tableScan) {

    // ensure filters are not set from previous pushFilters
    jobConf.unset(TableScanDesc.FILTER_TEXT_CONF_STR);
    jobConf.unset(TableScanDesc.FILTER_EXPR_CONF_STR);

    Utilities.unsetSchemaEvolution(jobConf);

    TableScanDesc scanDesc = tableScan.getConf();
    if (scanDesc == null) {
      return;
    }

    Utilities.addTableSchemaToConf(jobConf, tableScan);

    // construct column name list and types for reference by filter push down
    Utilities.setColumnNameList(jobConf, tableScan);
    Utilities.setColumnTypeList(jobConf, tableScan);
    // push down filters
    ExprNodeGenericFuncDesc filterExpr = (ExprNodeGenericFuncDesc)scanDesc.getFilterExpr();
    if (filterExpr == null) {
      return;
    }

    String serializedFilterObj = scanDesc.getSerializedFilterObject();
    String serializedFilterExpr = scanDesc.getSerializedFilterExpr();
    boolean hasObj = serializedFilterObj != null, hasExpr = serializedFilterExpr != null;
    if (!hasObj) {
      Serializable filterObject = scanDesc.getFilterObject();
      if (filterObject != null) {
        serializedFilterObj = SerializationUtilities.serializeObject(filterObject);
      }
    }
    if (serializedFilterObj != null) {
      jobConf.set(TableScanDesc.FILTER_OBJECT_CONF_STR, serializedFilterObj);
    }
    if (!hasExpr) {
      serializedFilterExpr = SerializationUtilities.serializeExpression(filterExpr);
    }
    String filterText = filterExpr.getExprString();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Pushdown initiated with filterText = " + filterText + ", filterExpr = "
          + filterExpr + ", serializedFilterExpr = " + serializedFilterExpr + " ("
          + (hasExpr ? "desc" : "new") + ")" + (serializedFilterObj == null ? "" :
            (", serializedFilterObj = " + serializedFilterObj + " (" + (hasObj ? "desc" : "new")
                + ")")));
    }
    jobConf.set(TableScanDesc.FILTER_TEXT_CONF_STR, filterText);
    jobConf.set(TableScanDesc.FILTER_EXPR_CONF_STR, serializedFilterExpr);
  }

  protected void pushProjectionsAndFilters(JobConf jobConf, Class inputFormatClass,
      String splitPath, String splitPathWithNoSchema) {
    pushProjectionsAndFilters(jobConf, inputFormatClass, splitPath,
      splitPathWithNoSchema, false);
  }

  protected void pushProjectionsAndFilters(JobConf jobConf, Class inputFormatClass,
      String splitPath, String splitPathWithNoSchema, boolean nonNative) {
    if (this.mrwork == null) {
      init(job);
    }

    if(this.mrwork.getPathToAliases() == null) {
      return;
    }

    ArrayList<String> aliases = new ArrayList<String>();
    Iterator<Entry<String, ArrayList<String>>> iterator = this.mrwork
        .getPathToAliases().entrySet().iterator();

    while (iterator.hasNext()) {
      Entry<String, ArrayList<String>> entry = iterator.next();
      String key = entry.getKey();
      boolean match;
      if (nonNative) {
        // For non-native tables, we need to do an exact match to avoid
        // HIVE-1903.  (The table location contains no files, and the string
        // representation of its path does not have a trailing slash.)
        match =
          splitPath.equals(key) || splitPathWithNoSchema.equals(key);
      } else {
        // But for native tables, we need to do a prefix match for
        // subdirectories.  (Unlike non-native tables, prefix mixups don't seem
        // to be a potential problem here since we are always dealing with the
        // path to something deeper than the table location.)
        match =
          splitPath.startsWith(key) || splitPathWithNoSchema.startsWith(key);
      }
      if (match) {
        ArrayList<String> list = entry.getValue();
        for (String val : list) {
          aliases.add(val);
        }
      }
    }

    for (String alias : aliases) {
      Operator<? extends OperatorDesc> op = this.mrwork.getAliasToWork().get(
        alias);
      if (op instanceof TableScanOperator) {
        TableScanOperator ts = (TableScanOperator) op;
        // push down projections.
        ColumnProjectionUtils.appendReadColumns(
            jobConf, ts.getNeededColumnIDs(), ts.getNeededColumns());
        // push down filters
        pushFilters(jobConf, ts);
      }
    }
  }
}
