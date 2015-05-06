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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

public abstract class HCatBaseInputFormat
  extends InputFormat<WritableComparable, HCatRecord> {

  /**
   * get the schema for the HCatRecord data returned by HCatInputFormat.
   *
   * @param context the jobContext
   * @throws IllegalArgumentException
   */
  private Class<? extends InputFormat> inputFileFormatClass;

  // TODO needs to go in InitializeInput? as part of InputJobInfo
  private static HCatSchema getOutputSchema(Configuration conf)
    throws IOException {
    String os = conf.get(HCatConstants.HCAT_KEY_OUTPUT_SCHEMA);
    if (os == null) {
      return getTableSchema(conf);
    } else {
      return (HCatSchema) HCatUtil.deserialize(os);
    }
  }

  /**
   * Set the schema for the HCatRecord data returned by HCatInputFormat.
   * @param job the job object
   * @param hcatSchema the schema to use as the consolidated schema
   */
  public static void setOutputSchema(Job job, HCatSchema hcatSchema)
    throws IOException {
    job.getConfiguration().set(HCatConstants.HCAT_KEY_OUTPUT_SCHEMA,
      HCatUtil.serialize(hcatSchema));
  }

  protected static org.apache.hadoop.mapred.InputFormat<WritableComparable, Writable>
  getMapRedInputFormat(JobConf job, Class inputFormatClass) throws IOException {
    return (
      org.apache.hadoop.mapred.InputFormat<WritableComparable, Writable>)
      ReflectionUtils.newInstance(inputFormatClass, job);
  }

  /**
   * Logically split the set of input files for the job. Returns the
   * underlying InputFormat's splits
   * @param jobContext the job context object
   * @return the splits, an HCatInputSplit wrapper over the storage
   *         handler InputSplits
   * @throws IOException or InterruptedException
   */
  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
    throws IOException, InterruptedException {
    Configuration conf = jobContext.getConfiguration();

    //Get the job info from the configuration,
    //throws exception if not initialized
    InputJobInfo inputJobInfo;
    try {
      inputJobInfo = getJobInfo(conf);
    } catch (Exception e) {
      throw new IOException(e);
    }

    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<PartInfo> partitionInfoList = inputJobInfo.getPartitions();
    if (partitionInfoList == null) {
      //No partitions match the specified partition filter
      return splits;
    }

    HiveStorageHandler storageHandler;
    JobConf jobConf;
    //For each matching partition, call getSplits on the underlying InputFormat
    for (PartInfo partitionInfo : partitionInfoList) {
      jobConf = HCatUtil.getJobConfFromContext(jobContext);
      setInputPath(jobConf, partitionInfo.getLocation());
      Map<String, String> jobProperties = partitionInfo.getJobProperties();

      HCatUtil.copyJobPropertiesToJobConf(jobProperties, jobConf);

      storageHandler = HCatUtil.getStorageHandler(
        jobConf, partitionInfo);

      //Get the input format
      Class inputFormatClass = storageHandler.getInputFormatClass();
      org.apache.hadoop.mapred.InputFormat inputFormat =
        getMapRedInputFormat(jobConf, inputFormatClass);

      //Call getSplit on the InputFormat, create an HCatSplit for each
      //underlying split. When the desired number of input splits is missing,
      //use a default number (denoted by zero).
      //TODO(malewicz): Currently each partition is split independently into
      //a desired number. However, we want the union of all partitions to be
      //split into a desired number while maintaining balanced sizes of input
      //splits.
      int desiredNumSplits =
        conf.getInt(HCatConstants.HCAT_DESIRED_PARTITION_NUM_SPLITS, 0);
      org.apache.hadoop.mapred.InputSplit[] baseSplits =
        inputFormat.getSplits(jobConf, desiredNumSplits);

      for (org.apache.hadoop.mapred.InputSplit split : baseSplits) {
        splits.add(new HCatSplit(partitionInfo, split));
      }
    }

    return splits;
  }

  /**
   * Create the RecordReader for the given InputSplit. Returns the underlying
   * RecordReader if the required operations are supported and schema matches
   * with HCatTable schema. Returns an HCatRecordReader if operations need to
   * be implemented in HCat.
   * @param split the split
   * @param taskContext the task attempt context
   * @return the record reader instance, either an HCatRecordReader(later) or
   *         the underlying storage handler's RecordReader
   * @throws IOException or InterruptedException
   */
  @Override
  public RecordReader<WritableComparable, HCatRecord>
  createRecordReader(InputSplit split,
             TaskAttemptContext taskContext) throws IOException, InterruptedException {

    HCatSplit hcatSplit = InternalUtil.castToHCatSplit(split);
    PartInfo partitionInfo = hcatSplit.getPartitionInfo();
    // Ensure PartInfo's TableInfo is initialized.
    if (partitionInfo.getTableInfo() == null) {
      partitionInfo.setTableInfo(((InputJobInfo)HCatUtil.deserialize(
          taskContext.getConfiguration().get(HCatConstants.HCAT_KEY_JOB_INFO)
      )).getTableInfo());
    }
    JobContext jobContext = taskContext;
    Configuration conf = jobContext.getConfiguration();

    HiveStorageHandler storageHandler = HCatUtil.getStorageHandler(
      conf, partitionInfo);

    JobConf jobConf = HCatUtil.getJobConfFromContext(jobContext);
    Map<String, String> jobProperties = partitionInfo.getJobProperties();
    HCatUtil.copyJobPropertiesToJobConf(jobProperties, jobConf);

    Map<String, Object> valuesNotInDataCols = getColValsNotInDataColumns(
      getOutputSchema(conf), partitionInfo
    );

    return new HCatRecordReader(storageHandler, valuesNotInDataCols);
  }


  /**
   * gets values for fields requested by output schema which will not be in the data
   */
  private static Map<String, Object> getColValsNotInDataColumns(HCatSchema outputSchema,
                                  PartInfo partInfo) throws HCatException {
    HCatSchema dataSchema = partInfo.getPartitionSchema();
    Map<String, Object> vals = new HashMap<String, Object>();
    for (String fieldName : outputSchema.getFieldNames()) {
      if (dataSchema.getPosition(fieldName) == null) {
        // this entry of output is not present in the output schema
        // so, we first check the table schema to see if it is a part col
        if (partInfo.getPartitionValues().containsKey(fieldName)) {

          // First, get the appropriate field schema for this field
          HCatFieldSchema fschema = outputSchema.get(fieldName);

          // For a partition key type, this will be a primitive typeinfo.
          // Obtain relevant object inspector for this typeinfo
          ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(fschema.getTypeInfo());

          // get appropriate object from the string representation of the value in partInfo.getPartitionValues()
          // Essentially, partition values are represented as strings, but we want the actual object type associated
          Object objVal = ObjectInspectorConverters
              .getConverter(PrimitiveObjectInspectorFactory.javaStringObjectInspector, oi)
              .convert(partInfo.getPartitionValues().get(fieldName));

          vals.put(fieldName, objVal);
        } else {
          vals.put(fieldName, null);
        }
      }
    }
    return vals;
  }

  /**
   * Gets the HCatTable schema for the table specified in the HCatInputFormat.setInput call
   * on the specified job context. This information is available only after HCatInputFormat.setInput
   * has been called for a JobContext.
   * @param conf the Configuration object
   * @return the table schema
   * @throws IOException if HCatInputFormat.setInput has not been called
   *                     for the current context
   */
  public static HCatSchema getTableSchema(Configuration conf)
    throws IOException {
    InputJobInfo inputJobInfo = getJobInfo(conf);
    HCatSchema allCols = new HCatSchema(new LinkedList<HCatFieldSchema>());
    for (HCatFieldSchema field :
      inputJobInfo.getTableInfo().getDataColumns().getFields()) {
      allCols.append(field);
    }
    for (HCatFieldSchema field :
      inputJobInfo.getTableInfo().getPartitionColumns().getFields()) {
      allCols.append(field);
    }
    return allCols;
  }

  /**
   * Gets the InputJobInfo object by reading the Configuration and deserializing
   * the string. If InputJobInfo is not present in the configuration, throws an
   * exception since that means HCatInputFormat.setInput has not been called.
   * @param conf the Configuration object
   * @return the InputJobInfo object
   * @throws IOException the exception
   */
  private static InputJobInfo getJobInfo(Configuration conf)
    throws IOException {
    String jobString = conf.get(
      HCatConstants.HCAT_KEY_JOB_INFO);
    if (jobString == null) {
      throw new IOException("job information not found in JobContext."
        + " HCatInputFormat.setInput() not called?");
    }

    return (InputJobInfo) HCatUtil.deserialize(jobString);
  }

  private void setInputPath(JobConf jobConf, String location)
    throws IOException {

    // ideally we should just call FileInputFormat.setInputPaths() here - but
    // that won't work since FileInputFormat.setInputPaths() needs
    // a Job object instead of a JobContext which we are handed here

    int length = location.length();
    int curlyOpen = 0;
    int pathStart = 0;
    boolean globPattern = false;
    List<String> pathStrings = new ArrayList<String>();

    for (int i = 0; i < length; i++) {
      char ch = location.charAt(i);
      switch (ch) {
      case '{': {
        curlyOpen++;
        if (!globPattern) {
          globPattern = true;
        }
        break;
      }
      case '}': {
        curlyOpen--;
        if (curlyOpen == 0 && globPattern) {
          globPattern = false;
        }
        break;
      }
      case ',': {
        if (!globPattern) {
          pathStrings.add(location.substring(pathStart, i));
          pathStart = i + 1;
        }
        break;
      }
      }
    }
    pathStrings.add(location.substring(pathStart, length));

    Path[] paths = StringUtils.stringToPath(pathStrings.toArray(new String[0]));
    String separator = "";
    StringBuilder str = new StringBuilder();

    for (Path path : paths) {
      FileSystem fs = path.getFileSystem(jobConf);
      final String qualifiedPath = fs.makeQualified(path).toString();
      str.append(separator)
        .append(StringUtils.escapeString(qualifiedPath));
      separator = StringUtils.COMMA_STR;
    }

    jobConf.set("mapred.input.dir", str.toString());
  }

}
