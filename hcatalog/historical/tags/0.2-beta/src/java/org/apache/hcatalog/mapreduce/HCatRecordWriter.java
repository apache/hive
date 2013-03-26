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
package org.apache.hcatalog.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.common.ErrorType;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;

public class HCatRecordWriter extends RecordWriter<WritableComparable<?>, HCatRecord> {

    private final HCatOutputStorageDriver storageDriver;

    private boolean dynamicPartitioningUsed = false;
    
//    static final private Log LOG = LogFactory.getLog(HCatRecordWriter.class);

    private final RecordWriter<? super WritableComparable<?>, ? super Writable> baseWriter;
    private final Map<Integer,RecordWriter<? super WritableComparable<?>, ? super Writable>> baseDynamicWriters;
    private final Map<Integer,HCatOutputStorageDriver> baseDynamicStorageDrivers;

    private final List<Integer> partColsToDel;
    private final List<Integer> dynamicPartCols;
    private int maxDynamicPartitions;

    private OutputJobInfo jobInfo;
    private TaskAttemptContext context;

    public HCatRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

      jobInfo = HCatOutputFormat.getJobInfo(context);
      this.context = context;

      // If partition columns occur in data, we want to remove them.
      partColsToDel = jobInfo.getPosOfPartCols();
      dynamicPartitioningUsed = jobInfo.getTableInfo().isDynamicPartitioningUsed();
      dynamicPartCols = jobInfo.getPosOfDynPartCols();
      maxDynamicPartitions = jobInfo.getMaxDynamicPartitions();

      if((partColsToDel == null) || (dynamicPartitioningUsed && (dynamicPartCols == null))){
        throw new HCatException("It seems that setSchema() is not called on " +
        		"HCatOutputFormat. Please make sure that method is called.");
      }
      

      if (!dynamicPartitioningUsed){
        this.storageDriver = HCatOutputFormat.getOutputDriverInstance(context, jobInfo);
        this.baseWriter = storageDriver.getOutputFormat().getRecordWriter(context);
        this.baseDynamicStorageDrivers = null;
        this.baseDynamicWriters = null;
      }else{
        this.baseDynamicStorageDrivers = new HashMap<Integer,HCatOutputStorageDriver>();
        this.baseDynamicWriters = new HashMap<Integer,RecordWriter<? super WritableComparable<?>, ? super Writable>>();
        this.storageDriver = null;
        this.baseWriter = null;
      }

    }

    /**
     * @return the storageDriver
     */
    public HCatOutputStorageDriver getStorageDriver() {
      return storageDriver;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
      if (dynamicPartitioningUsed){
        for (RecordWriter<? super WritableComparable<?>, ? super Writable> bwriter : baseDynamicWriters.values()){
          bwriter.close(context);
        }
        for (HCatOutputStorageDriver osd : baseDynamicStorageDrivers.values()){
          OutputCommitter baseOutputCommitter = osd.getOutputFormat().getOutputCommitter(context);
          if (baseOutputCommitter.needsTaskCommit(context)){
            baseOutputCommitter.commitTask(context);
          }
        }
      } else {
        baseWriter.close(context);
      }
    }

    @Override
    public void write(WritableComparable<?> key, HCatRecord value) throws IOException,
            InterruptedException {
      RecordWriter<? super WritableComparable<?>, ? super Writable> localWriter;
      HCatOutputStorageDriver localDriver;
      
//      HCatUtil.logList(LOG, "HCatRecord to write", value.getAll());

      if (dynamicPartitioningUsed){
        // calculate which writer to use from the remaining values - this needs to be done before we delete cols

        List<String> dynamicPartValues = new ArrayList<String>();
        for (Integer colToAppend :  dynamicPartCols){
          dynamicPartValues.add(value.get(colToAppend).toString());
        }
        
        int dynHashCode = dynamicPartValues.hashCode();
        if (!baseDynamicWriters.containsKey(dynHashCode)){
//          LOG.info("Creating new storage driver["+baseDynamicStorageDrivers.size()
//              +"/"+maxDynamicPartitions+ "] for "+dynamicPartValues.toString());
          if ((maxDynamicPartitions != -1) && (baseDynamicStorageDrivers.size() > maxDynamicPartitions)){
            throw new HCatException(ErrorType.ERROR_TOO_MANY_DYNAMIC_PTNS, 
                "Number of dynamic partitions being created "
                + "exceeds configured max allowable partitions["
                + maxDynamicPartitions 
                + "], increase parameter [" 
                + HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS.varname
                + "] if needed.");
          }
//          HCatUtil.logList(LOG, "dynamicpartvals", dynamicPartValues);
//          HCatUtil.logList(LOG, "dynamicpartCols", dynamicPartCols);
          
          HCatOutputStorageDriver localOsd = createDynamicStorageDriver(dynamicPartValues);
          RecordWriter<? super WritableComparable<?>, ? super Writable> baseRecordWriter 
            = localOsd.getOutputFormat().getRecordWriter(context);
          localOsd.setupOutputCommitterJob(context);
          OutputCommitter baseOutputCommitter = localOsd.getOutputFormat().getOutputCommitter(context);
          baseOutputCommitter.setupTask(context);
          prepareForStorageDriverOutput(localOsd,context);
          baseDynamicWriters.put(dynHashCode, baseRecordWriter);
          baseDynamicStorageDrivers.put(dynHashCode,localOsd);
        }

        localWriter = baseDynamicWriters.get(dynHashCode);
        localDriver = baseDynamicStorageDrivers.get(dynHashCode);
      }else{
        localWriter = baseWriter;
        localDriver = storageDriver;
      }

      for(Integer colToDel : partColsToDel){
        value.remove(colToDel);
      }

      //The key given by user is ignored
      WritableComparable<?> generatedKey = localDriver.generateKey(value);
      Writable convertedValue = localDriver.convertValue(value);
      localWriter.write(generatedKey, convertedValue);
    }

    protected HCatOutputStorageDriver createDynamicStorageDriver(List<String> dynamicPartVals) throws IOException {
      HCatOutputStorageDriver localOsd = HCatOutputFormat.getOutputDriverInstance(context,jobInfo,dynamicPartVals);
      return localOsd;
    }

    public void prepareForStorageDriverOutput(TaskAttemptContext context) throws IOException {
      // Set permissions and group on freshly created files.
      if (!dynamicPartitioningUsed){
        HCatOutputStorageDriver localOsd = this.getStorageDriver();
        prepareForStorageDriverOutput(localOsd,context);
      }
    }

    private void prepareForStorageDriverOutput(HCatOutputStorageDriver localOsd,
        TaskAttemptContext context) throws IOException {
      HCatOutputFormat.prepareOutputLocation(localOsd,context);
    }
}
