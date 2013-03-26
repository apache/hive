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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.common.ErrorType;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.HCatRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Part of the FileOutput*Container classes
 * See {@link FileOutputFormatContainer} for more information
 */
class FileRecordWriterContainer extends RecordWriterContainer {

    private final HCatOutputStorageDriver storageDriver;

    private boolean dynamicPartitioningUsed = false;

//    static final private Log LOG = LogFactory.getLog(FileRecordWriterContainer.class);

    private final Map<Integer,RecordWriter<? super WritableComparable<?>, ? super Writable>> baseDynamicWriters;
    private final Map<Integer,HCatOutputStorageDriver> baseDynamicStorageDrivers;
    private final Map<Integer,OutputCommitter> baseDynamicCommitters;


    private final List<Integer> partColsToDel;
    private final List<Integer> dynamicPartCols;
    private int maxDynamicPartitions;

    private OutputJobInfo jobInfo;
    private TaskAttemptContext context;

    /**
     * @param baseWriter RecordWriter to contain
     * @param context current TaskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    public FileRecordWriterContainer(RecordWriter<? super WritableComparable<?>, ? super Writable> baseWriter,
                                     TaskAttemptContext context) throws IOException, InterruptedException {
        super(context,baseWriter);
        this.context = context;
        jobInfo = HCatOutputFormat.getJobInfo(context);

        // If partition columns occur in data, we want to remove them.
        partColsToDel = jobInfo.getPosOfPartCols();
        dynamicPartitioningUsed = jobInfo.isDynamicPartitioningUsed();
        dynamicPartCols = jobInfo.getPosOfDynPartCols();
        maxDynamicPartitions = jobInfo.getMaxDynamicPartitions();

        if((partColsToDel == null) || (dynamicPartitioningUsed && (dynamicPartCols == null))){
            throw new HCatException("It seems that setSchema() is not called on " +
                    "HCatOutputFormat. Please make sure that method is called.");
        }


        if (!dynamicPartitioningUsed) {
            storageDriver = HCatOutputFormat.getOutputDriverInstance(context,jobInfo);
            this.baseDynamicStorageDrivers = null;
            this.baseDynamicWriters = null;
            this.baseDynamicCommitters = null;
            prepareForStorageDriverOutput(context);
        }
        else {
            storageDriver = null;
            this.baseDynamicStorageDrivers = new HashMap<Integer,HCatOutputStorageDriver>();
            this.baseDynamicWriters = new HashMap<Integer,RecordWriter<? super WritableComparable<?>, ? super Writable>>();
            this.baseDynamicCommitters = new HashMap<Integer,OutputCommitter>();
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
            for(Map.Entry<Integer,OutputCommitter>entry : baseDynamicCommitters.entrySet()) {
//            for (HCatOutputStorageDriver osd : baseDynamicStorageDrivers.values()){
                OutputCommitter baseOutputCommitter = entry.getValue();
                if (baseOutputCommitter.needsTaskCommit(context)){
                    baseOutputCommitter.commitTask(context);
                }
            }
        } else {
            getBaseRecordWriter().close(context);
        }
    }

    @Override
    public void write(WritableComparable<?> key, HCatRecord value) throws IOException,
            InterruptedException {
        RecordWriter localWriter;
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

                RecordWriter baseRecordWriter = localOsd.getOutputFormat().getRecordWriter(context);
                OutputCommitter baseOutputCommitter = localOsd.getOutputFormat().getOutputCommitter(context);
                baseOutputCommitter.setupJob(context);
                baseOutputCommitter.setupTask(context);
                prepareForStorageDriverOutput(localOsd,context);
                baseDynamicWriters.put(dynHashCode, baseRecordWriter);
                baseDynamicStorageDrivers.put(dynHashCode,localOsd);
                baseDynamicCommitters.put(dynHashCode,baseOutputCommitter);
            }

            localWriter = baseDynamicWriters.get(dynHashCode);
            localDriver = baseDynamicStorageDrivers.get(dynHashCode);
        }else{
            localWriter = getBaseRecordWriter();
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
        FileOutputStorageDriver.prepareOutputLocation(localOsd, context);
    }

}
