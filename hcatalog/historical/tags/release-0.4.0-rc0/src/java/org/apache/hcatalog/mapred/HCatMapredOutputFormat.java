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
package org.apache.hcatalog.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progressable;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.data.Pair;
import org.apache.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.hcatalog.data.schema.HCatSchemaUtils.CollectionBuilder;
import org.apache.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hcatalog.mapreduce.InitializeInput;
import org.apache.hcatalog.mapreduce.InputJobInfo;
import org.apache.hcatalog.mapreduce.OutputJobInfo;
import org.apache.hcatalog.shims.HCatHadoopShims;
import org.apache.hcatalog.storagehandler.HCatStorageHandlerImpl;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class HCatMapredOutputFormat implements OutputFormat, HiveOutputFormat {

  HCatOutputFormat hco;
  private static final Log LOG = LogFactory.getLog(HCatMapredOutputFormat.class);
  
  public HCatMapredOutputFormat() {
    LOG.debug("HCatMapredOutputFormat init");
    hco = new HCatOutputFormat();
  }
  
  @Override
  public void checkOutputSpecs(FileSystem arg0, JobConf arg1)
      throws IOException {
    LOG.debug("HCatMapredOutputFormat checkOutputSpecs");
    JobContext context = HCatHadoopShims.Instance.get().createJobContext(arg1, new JobID());
    try {
      hco.checkOutputSpecs(context);
    } catch (InterruptedException e) {
      LOG.warn(e.getMessage());
      HCatUtil.logStackTrace(LOG);
    }
  }

  @Override
  public RecordWriter getRecordWriter(FileSystem arg0, JobConf arg1,
      String arg2, Progressable arg3) throws IOException {
    // this is never really called from hive, but it's part of the IF interface
    
    LOG.debug("HCatMapredOutputFormat getRecordWriter");
    return getRW(arg1);
  }

  public HCatMapredRecordWriter getRW(Configuration arg1) throws IOException {
    try {
      JobContext jc = HCatHadoopShims.Instance.get().createJobContext(arg1, new JobID());
      TaskAttemptContext taContext = HCatHadoopShims.Instance.get().createTaskAttemptContext(arg1, new TaskAttemptID());
    return new HCatMapredOutputFormat.HCatMapredRecordWriter(hco,jc,taContext);
    } catch (Exception e){
      throw new IOException(e);
    }
  }

  @Override
  public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
      JobConf jc, Path finalOutPath, Class valueClass, boolean isCompressed,
      Properties tableProperties, Progressable progress) throws IOException {
    LOG.debug("HCatMapredOutputFormat getHiveRecordWriter");
    final HCatMapredRecordWriter rw = getRW(jc);
    return new org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter() {
      public void write(Writable r) throws IOException {
        rw.write(null, (HCatRecord) r);
      }
      public void close(boolean abort) throws IOException {
        rw.setAbortStatus(abort);
        rw.close(null);
      }
    };

  }
  
  public static void setTableDesc(TableDesc tableDesc, Map<String,String> jobProperties) throws IOException {
    setTableDesc(tableDesc,jobProperties,new LinkedHashMap<String, String>());
  }

  public static void setPartitionDesc(PartitionDesc ptnDesc, Map<String,String> jobProperties) throws IOException {
    setTableDesc(ptnDesc.getTableDesc(),jobProperties,ptnDesc.getPartSpec());
  }

  public static void setTableDesc(TableDesc tableDesc, Map<String,String> jobProperties, Map<String,String> ptnValues) throws IOException {
    Pair<String,String> dbAndTableName = HCatUtil.getDbAndTableName(tableDesc.getTableName());

    OutputJobInfo outputJobInfo = OutputJobInfo.create(
        dbAndTableName.first, dbAndTableName.second, 
        ptnValues);
    
    Job job = new Job(new Configuration()); 
      // TODO : verify with thw if this needs to be shim-ed. There exists no current Shim
      // for instantiating a Job, and we use it only temporarily.
    
    HCatOutputFormat.setOutput(job, outputJobInfo);
    LOG.debug("HCatOutputFormat.setOutput() done");
    
    // Now we need to set the schema we intend to write
    
    Properties tprops = tableDesc.getProperties();
    String columnNameProperty = tprops.getProperty(Constants.LIST_COLUMNS);
    String columnTypeProperty = tprops.getProperty(Constants.LIST_COLUMN_TYPES);
    
    List<String> columnNames;
    // all table column names
    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(","));
    }
    
    List<TypeInfo> columnTypes;
    // all column types
    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    HCatSchema hsch = HCatSchemaUtils.getHCatSchema(rowTypeInfo).getFields().get(0).getStructSubSchema();
      // getting inner schema, because it's the difference between struct<i:int,j:int> and i:int,j:int.
      // and that's what we need to provide to HCatOutputFormat
    
    LOG.debug("schema "+hsch.toString());
    HCatOutputFormat.setSchema(job, hsch);
    
    for (String confToSave : HCatConstants.OUTPUT_CONFS_TO_SAVE){
      String confVal = job.getConfiguration().get(confToSave);
      if (confVal != null){
        jobProperties.put(confToSave, confVal);
      }
    }

  }
  
  public class HCatMapredRecordWriter implements org.apache.hadoop.mapred.RecordWriter<WritableComparable<?>, HCatRecord>{

    org.apache.hadoop.mapreduce.RecordWriter writer;
    org.apache.hadoop.mapreduce.OutputCommitter outputCommitter;
    TaskAttemptContext taContext;
    JobContext jc;
    boolean jobIsSetup = false;
    boolean wroteData = false;
    boolean aborted = false;
    
    public HCatMapredRecordWriter(
       HCatOutputFormat hco, JobContext jc,
        TaskAttemptContext taContext) throws IOException{
      this.taContext = taContext;
      try {
        this.outputCommitter = hco.getOutputCommitter(taContext);
        this.writer = hco.getRecordWriter(taContext);
      } catch (java.lang.InterruptedException e){
        throw new IOException(e);
      }
      this.wroteData = false;
      this.aborted = false;
    }
    
    public void setAbortStatus(boolean abort) {
      this.aborted = abort;
    }
    
    @Override
    public void close(Reporter arg0) throws IOException {
      try {
        writer.close(taContext);
        if (outputCommitter.needsTaskCommit(taContext)){
          outputCommitter.commitTask(taContext);
        }
        if (this.wroteData && this.jobIsSetup){
          if (!this.aborted){
            outputCommitter.commitJob(taContext);
          } else {
            outputCommitter.cleanupJob(taContext);
          }
        }
      } catch (Exception e){
        throw new IOException(e);
      }
    }

    @Override
    public void write(WritableComparable arg0, HCatRecord arg1) throws IOException {
      try {
        if (!jobIsSetup){
          this.outputCommitter.setupJob(taContext);
          jobIsSetup = true;
        }
        writer.write(arg0, arg1);
        this.wroteData = true;
      } catch (Exception e){
        throw new IOException(e);
      }
    }
    
  }
}
