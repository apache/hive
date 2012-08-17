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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.io.HiveIOExceptionHandlerUtil;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
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
import org.apache.hadoop.util.ReflectionUtils;

/**
 * HiveInputFormat is a parameterized InputFormat which looks at the path name
 * and determine the correct InputFormat for that path name from
 * mapredPlan.pathToPartitionInfo(). It can be used to read files with different
 * input format in the same map-reduce job.
 */
public class HiveInputFormat<K extends WritableComparable, V extends Writable>
    implements InputFormat<K, V>, JobConfigurable {

  public static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.hive.ql.io.HiveInputFormat");

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
        inputSplit = (InputSplit) ReflectionUtils.newInstance(conf
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

  JobConf job;

  public void configure(JobConf job) {
    this.job = job;
  }

  /**
   * A cache of InputFormat instances.
   */
  protected static Map<Class, InputFormat<WritableComparable, Writable>> inputFormats;

  public static InputFormat<WritableComparable, Writable> getInputFormatFromCache(
    Class inputFormatClass, JobConf job) throws IOException {

    if (inputFormats == null) {
      inputFormats = new HashMap<Class, InputFormat<WritableComparable, Writable>>();
    }
    if (!inputFormats.containsKey(inputFormatClass)) {
      try {
        InputFormat<WritableComparable, Writable> newInstance = (InputFormat<WritableComparable, Writable>) ReflectionUtils
            .newInstance(inputFormatClass, job);
        inputFormats.put(inputFormatClass, newInstance);
      } catch (Exception e) {
        throw new IOException("Cannot create an instance of InputFormat class "
            + inputFormatClass.getName() + " as specified in mapredWork!", e);
      }
    }
    return inputFormats.get(inputFormatClass);
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

    // clone a jobConf for setting needed columns for reading
    JobConf cloneJobConf = new JobConf(job);

    if (this.mrwork == null) {
      init(job);
    }

    boolean nonNative = false;
    PartitionDesc part = pathToPartitionInfo.get(hsplit.getPath().toString());
    if ((part != null) && (part.getTableDesc() != null)) {
      Utilities.copyTableJobPropertiesToConf(part.getTableDesc(), cloneJobConf);
      nonNative = part.getTableDesc().isNonNative();
    }

    pushProjectionsAndFilters(cloneJobConf, inputFormatClass, hsplit.getPath()
      .toString(), hsplit.getPath().toUri().getPath(), nonNative);

    InputFormat inputFormat = getInputFormatFromCache(inputFormatClass,
        cloneJobConf);
    RecordReader innerReader = null;
    try {
      innerReader = inputFormat.getRecordReader(inputSplit,
        cloneJobConf, reporter);
    } catch (Exception e) {
      innerReader = HiveIOExceptionHandlerUtil
          .handleRecordReaderCreationException(e, cloneJobConf);
    }
    HiveRecordReader<K,V> rr = new HiveRecordReader(innerReader, job);
    rr.initIOContext(hsplit, job, inputFormatClass, innerReader);
    return rr;
  }

  protected Map<String, PartitionDesc> pathToPartitionInfo;
  MapredWork mrwork = null;

  protected void init(JobConf job) {
    mrwork = Utilities.getMapRedWork(job);
    pathToPartitionInfo = mrwork.getPathToPartitionInfo();
  }

  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

    init(job);

    Path[] dirs = FileInputFormat.getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }
    JobConf newjob = new JobConf(job);
    ArrayList<InputSplit> result = new ArrayList<InputSplit>();

    // for each dir, get the InputFormat, and do getSplits.
    for (Path dir : dirs) {
      PartitionDesc part = getPartitionDescFromPath(pathToPartitionInfo, dir);
      // create a new InputFormat instance if this is the first time to see this
      // class
      Class inputFormatClass = part.getInputFileFormatClass();
      InputFormat inputFormat = getInputFormatFromCache(inputFormatClass, job);
      Utilities.copyTableJobPropertiesToConf(part.getTableDesc(), newjob);

      // Make filter pushdown information available to getSplits.
      ArrayList<String> aliases =
        mrwork.getPathToAliases().get(dir.toUri().toString());
      if ((aliases != null) && (aliases.size() == 1)) {
        Operator op = mrwork.getAliasToWork().get(aliases.get(0));
        if ((op != null) && (op instanceof TableScanOperator)) {
          TableScanOperator tableScan = (TableScanOperator) op;
          pushFilters(newjob, tableScan);
        }
      }

      FileInputFormat.setInputPaths(newjob, dir);
      newjob.setInputFormat(inputFormat.getClass());
      InputSplit[] iss = inputFormat.getSplits(newjob, numSplits / dirs.length);
      for (InputSplit is : iss) {
        result.add(new HiveInputSplit(is, inputFormatClass.getName()));
      }
    }

    LOG.info("number of splits " + result.size());

    return result.toArray(new HiveInputSplit[result.size()]);
  }

  public void validateInput(JobConf job) throws IOException {

    init(job);

    Path[] dirs = FileInputFormat.getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }
    JobConf newjob = new JobConf(job);

    // for each dir, get the InputFormat, and do validateInput.
    for (Path dir : dirs) {
      PartitionDesc part = getPartitionDescFromPath(pathToPartitionInfo, dir);
      // create a new InputFormat instance if this is the first time to see this
      // class
      InputFormat inputFormat = getInputFormatFromCache(part
          .getInputFileFormatClass(), job);

      FileInputFormat.setInputPaths(newjob, dir);
      newjob.setInputFormat(inputFormat.getClass());
      ShimLoader.getHadoopShims().inputFormatValidateInput(inputFormat, newjob);
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
          + " in partToPartitionInfo!");
    }

    return partDesc;
  }

  public static void pushFilters(JobConf jobConf, TableScanOperator tableScan) {

    TableScanDesc scanDesc = tableScan.getConf();
    if (scanDesc == null) {
      return;
    }

    // construct column name list and types for reference by filter push down
    Utilities.setColumnNameList(jobConf, tableScan);
    Utilities.setColumnTypeList(jobConf, tableScan);
    // push down filters
    ExprNodeDesc filterExpr = scanDesc.getFilterExpr();
    if (filterExpr == null) {
      return;
    }

    String filterText = filterExpr.getExprString();
    String filterExprSerialized = Utilities.serializeExpression(filterExpr);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Filter text = " + filterText);
      LOG.debug("Filter expression = " + filterExprSerialized);
    }
    jobConf.set(
      TableScanDesc.FILTER_TEXT_CONF_STR,
      filterText);
    jobConf.set(
      TableScanDesc.FILTER_EXPR_CONF_STR,
      filterExprSerialized);
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
      Operator<? extends Serializable> op = this.mrwork.getAliasToWork().get(
          alias);
      if (op != null && op instanceof TableScanOperator) {
        TableScanOperator tableScan = (TableScanOperator) op;

        // push down projections
        ArrayList<Integer> list = tableScan.getNeededColumnIDs();
        if (list != null) {
          ColumnProjectionUtils.appendReadColumnIDs(jobConf, list);
        } else {
          ColumnProjectionUtils.setFullyReadColumns(jobConf);
        }

        pushFilters(jobConf, tableScan);
      }
    }
  }
}
