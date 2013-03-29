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

package org.apache.hcatalog.pig.drivers;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.mapreduce.OutputJobInfo;
import org.apache.hcatalog.pig.PigHCatUtil;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.StoreMetadata;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class StoreFuncBasedOutputFormat extends
        OutputFormat<BytesWritable, Tuple> {

    private final StoreFuncInterface storeFunc;
    
    public StoreFuncBasedOutputFormat(StoreFuncInterface storeFunc) {

        this.storeFunc = storeFunc;
    }
    
    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException,
            InterruptedException {
        OutputFormat<BytesWritable,Tuple> outputFormat =  storeFunc.getOutputFormat();
        outputFormat.checkOutputSpecs(jobContext);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext ctx)
            throws IOException, InterruptedException {
        String serializedJobInfo = ctx.getConfiguration().get(HCatConstants.HCAT_KEY_OUTPUT_INFO);
        OutputJobInfo outputJobInfo = (OutputJobInfo)HCatUtil.deserialize(serializedJobInfo);
        ResourceSchema rs = PigHCatUtil.getResourceSchema(outputJobInfo.getOutputSchema());
        String location = outputJobInfo.getLocation();
        OutputFormat<BytesWritable,Tuple> outputFormat =  storeFunc.getOutputFormat();
        return new StoreFuncBasedOutputCommitter(storeFunc, outputFormat.getOutputCommitter(ctx), location, rs);
    }

    @Override
    public RecordWriter<BytesWritable, Tuple> getRecordWriter(
            TaskAttemptContext ctx) throws IOException, InterruptedException {
        RecordWriter<BytesWritable,Tuple> writer = storeFunc.getOutputFormat().getRecordWriter(ctx);
        String serializedJobInfo = ctx.getConfiguration().get(HCatConstants.HCAT_KEY_OUTPUT_INFO);
        OutputJobInfo outputJobInfo = (OutputJobInfo)HCatUtil.deserialize(serializedJobInfo);
        ResourceSchema rs = PigHCatUtil.getResourceSchema(outputJobInfo.getOutputSchema());
        String location = outputJobInfo.getLocation();
        return new StoreFuncBasedRecordWriter(writer, storeFunc, location, rs);
    }
    
    static class StoreFuncBasedRecordWriter extends RecordWriter<BytesWritable, Tuple> {
        private final RecordWriter<BytesWritable,Tuple> writer;
        private final StoreFuncInterface storeFunc;
        private final ResourceSchema schema;
        private final String location;
        
        public StoreFuncBasedRecordWriter(RecordWriter<BytesWritable,Tuple> writer, StoreFuncInterface sf, String location, ResourceSchema rs) throws IOException {
            this.writer = writer;
            this.storeFunc = sf;
            this.schema = rs;
            this.location = location;
            storeFunc.prepareToWrite(writer);
        }
        
        @Override
        public void close(TaskAttemptContext ctx) throws IOException,
                InterruptedException {
            writer.close(ctx);
        }

        @Override
        public void write(BytesWritable key, Tuple value) throws IOException,
                InterruptedException {
            storeFunc.putNext(value);
        }
    }
    
    static class StoreFuncBasedOutputCommitter extends OutputCommitter {
        StoreFuncInterface sf;
        OutputCommitter wrappedOutputCommitter;
        String location;
        ResourceSchema rs;
        public StoreFuncBasedOutputCommitter(StoreFuncInterface sf, OutputCommitter outputCommitter, String location, ResourceSchema rs) {
            this.sf = sf;
            this.wrappedOutputCommitter = outputCommitter;
            this.location = location;
            this.rs = rs;
        }
        @Override
        public void abortTask(TaskAttemptContext context) throws IOException {
            wrappedOutputCommitter.abortTask(context);
        }

        @Override
        public void commitTask(TaskAttemptContext context) throws IOException {
            wrappedOutputCommitter.commitTask(context);
        }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext context)
                throws IOException {
            return wrappedOutputCommitter.needsTaskCommit(context);
        }

        @Override
        public void setupJob(JobContext context) throws IOException {
            wrappedOutputCommitter.setupJob(context);
        }

        @Override
        public void setupTask(TaskAttemptContext context) throws IOException {
            wrappedOutputCommitter.setupTask(context);
        }
        
        public void commitJob(JobContext context) throws IOException {
            wrappedOutputCommitter.commitJob(context);
            if (sf instanceof StoreMetadata) {
                if (rs != null) {
                    ((StoreMetadata) sf).storeSchema(
                            rs, location, new Job(context.getConfiguration()) );
                }
            }
        }
        
        @Override
        public void cleanupJob(JobContext context) throws IOException {
            wrappedOutputCommitter.cleanupJob(context);
            if (sf instanceof StoreMetadata) {
                if (rs != null) {
                    ((StoreMetadata) sf).storeSchema(
                            rs, location, new Job(context.getConfiguration()) );
                }
            }
        }
        
        public void abortJob(JobContext context, JobStatus.State state) throws IOException {
            wrappedOutputCommitter.abortJob(context, state);
        }
    }
}
