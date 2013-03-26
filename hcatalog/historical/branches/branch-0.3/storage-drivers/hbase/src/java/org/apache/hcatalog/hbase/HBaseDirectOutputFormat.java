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

package org.apache.hcatalog.hbase;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.hbase.snapshot.RevisionManager;


import java.io.IOException;

/**
 * "Direct" implementation of OutputFormat for HBase. Uses HTable client's put API to write each row to HBase one a
 * time. Presently it is just using TableOutputFormat as the underlying implementation in the future we can
 * tune this to make the writes faster such as permanently disabling WAL, caching, etc.
 */
class HBaseDirectOutputFormat extends OutputFormat<WritableComparable<?>,Writable> implements Configurable {

    private TableOutputFormat<WritableComparable<?>> outputFormat;

    public HBaseDirectOutputFormat() {
        this.outputFormat = new TableOutputFormat<WritableComparable<?>>();
    }

    @Override
    public RecordWriter<WritableComparable<?>, Writable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return outputFormat.getRecordWriter(context);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        outputFormat.checkOutputSpecs(context);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new HBaseDirectOutputCommitter(outputFormat.getOutputCommitter(context));
    }

    @Override
    public void setConf(Configuration conf) {
        String tableName = conf.get(HBaseConstants.PROPERTY_OUTPUT_TABLE_NAME_KEY);
        conf = new Configuration(conf);
        conf.set(TableOutputFormat.OUTPUT_TABLE,tableName);
        outputFormat.setConf(conf);
    }

    @Override
    public Configuration getConf() {
        return outputFormat.getConf();
    }

    private static class HBaseDirectOutputCommitter extends OutputCommitter {
        private OutputCommitter baseOutputCommitter;

        public HBaseDirectOutputCommitter(OutputCommitter baseOutputCommitter) throws IOException {
            this.baseOutputCommitter = baseOutputCommitter;
        }

        @Override
        public void abortTask(TaskAttemptContext context) throws IOException {
            baseOutputCommitter.abortTask(context);
        }

        @Override
        public void commitTask(TaskAttemptContext context) throws IOException {
            baseOutputCommitter.commitTask(context);
        }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
            return baseOutputCommitter.needsTaskCommit(context);
        }

        @Override
        public void setupJob(JobContext context) throws IOException {
            baseOutputCommitter.setupJob(context);
        }

        @Override
        public void setupTask(TaskAttemptContext context) throws IOException {
            baseOutputCommitter.setupTask(context);
        }

        @Override
        public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
            RevisionManager rm = null;
            try {
                baseOutputCommitter.abortJob(jobContext, state);
                rm = HBaseHCatStorageHandler.getOpenedRevisionManager(jobContext.getConfiguration());
                rm.abortWriteTransaction(HBaseHCatStorageHandler.getWriteTransaction(jobContext.getConfiguration()));
            } finally {
                if(rm != null)
                    rm.close();
            }
        }

        @Override
        public void commitJob(JobContext jobContext) throws IOException {
            RevisionManager rm = null;
            try {
                baseOutputCommitter.commitJob(jobContext);
                rm = HBaseHCatStorageHandler.getOpenedRevisionManager(jobContext.getConfiguration());
                rm.commitWriteTransaction(HBaseHCatStorageHandler.getWriteTransaction(jobContext.getConfiguration()));
            } finally {
                if(rm != null)
                    rm.close();
            }
        }
    }
}
