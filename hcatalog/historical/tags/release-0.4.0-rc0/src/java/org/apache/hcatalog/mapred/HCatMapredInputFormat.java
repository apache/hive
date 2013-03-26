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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hcatalog.mapreduce.HCatSplit;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.Pair;
import org.apache.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hcatalog.mapreduce.InitializeInput;
import org.apache.hcatalog.mapreduce.InputJobInfo;
import org.apache.hcatalog.shims.HCatHadoopShims;
import org.apache.hcatalog.storagehandler.HCatStorageHandlerImpl;
import org.apache.hadoop.hive.ql.plan.TableDesc;



public class HCatMapredInputFormat implements InputFormat {

  
  private static final Log LOG = LogFactory.getLog(HCatMapredInputFormat.class);
  HCatInputFormat hci;
  
  public HCatMapredInputFormat(){
    hci = new HCatInputFormat();
  }
  
  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job,
      Reporter arg2) throws IOException {
    try {
      org.apache.hadoop.mapreduce.RecordReader<WritableComparable, HCatRecord> rr;
      TaskAttemptContext taContext 
      = HCatHadoopShims.Instance.get().createTaskAttemptContext(job, new TaskAttemptID());
      rr = hci.createRecordReader(((HiveHCatSplitWrapper)split).getHCatSplit(), taContext);
      rr.initialize(((HiveHCatSplitWrapper)split).getHCatSplit(),taContext);
      return (RecordReader) rr;

    } catch (java.lang.InterruptedException e){
      throw new IOException(e);
    }
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int arg1) throws IOException {

    try {
      List<InputSplit> hsplits = new ArrayList<InputSplit>();
      for (org.apache.hadoop.mapreduce.InputSplit hs : hci.getSplits(
          HCatHadoopShims.Instance.get().createJobContext(job, new JobID()))){
        HiveHCatSplitWrapper hwrapper = new HiveHCatSplitWrapper((HCatSplit)hs);

        String hwrapperPath = hwrapper.getPath().toString();
        String mapredInputDir = job.get("mapred.input.dir","null");

        if(hwrapperPath.startsWith(mapredInputDir)){
          hsplits.add(hwrapper);
        }
      }
      InputSplit[] splits = new InputSplit[hsplits.size()];
      for (int i = 0 ; i <hsplits.size(); i++){
        splits[i] = hsplits.get(i);
      }
      return splits;
    } catch (java.lang.InterruptedException e){
      throw new IOException(e);
    }
  }
  
  public static void setTableDesc(TableDesc tableDesc, Map<String,String> jobProperties) throws IOException{
    try {
    Pair<String,String> dbAndTableName = HCatUtil.getDbAndTableName(tableDesc.getTableName());
    InputJobInfo info = InputJobInfo.create(dbAndTableName.first, dbAndTableName.second, "");
    jobProperties.put(HCatConstants.HCAT_KEY_JOB_INFO
        ,InitializeInput.getSerializedHcatKeyJobInfo(
            null, info,tableDesc.getProperties().getProperty("location")));
    } catch (Exception e){
      throw new IOException(e);
    }
  }

}
