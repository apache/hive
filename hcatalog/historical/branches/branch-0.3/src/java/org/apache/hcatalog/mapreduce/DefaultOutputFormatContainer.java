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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.data.HCatRecord;

import java.io.IOException;

/**
 * Bare bones implementation of OutputFormatContainer. Does only the required
 * tasks to work properly with HCatalog. HCatalog features which require a
 * storage specific implementation are unsupported (ie partitioning).
 */
class DefaultOutputFormatContainer extends OutputFormatContainer {

    public DefaultOutputFormatContainer(OutputFormat<WritableComparable<?>, Writable> of) {
        super(of);
    }


    /**
     * Get the record writer for the job. Uses the Table's default OutputStorageDriver
     * to get the record writer.
     * @param context the information about the current task.
     * @return a RecordWriter to write the output for the job.
     * @throws IOException
     */
    @Override
    public RecordWriter<WritableComparable<?>, HCatRecord>
    getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new DefaultRecordWriterContainer(context, getBaseOutputFormat().getRecordWriter(context));
    }


    /**
     * Get the output committer for this output format. This is responsible
     * for ensuring the output is committed correctly.
     * @param context the task context
     * @return an output committer
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        OutputFormat outputFormat = getBaseOutputFormat();
        return new DefaultOutputCommitterContainer(context, getBaseOutputFormat().getOutputCommitter(context));
    }

    /**
     * Check for validity of the output-specification for the job.
     * @param context information about the job
     * @throws IOException when output should not be attempted
     */
    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        OutputFormat<? super WritableComparable<?>, ? super Writable> outputFormat = getBaseOutputFormat();
        outputFormat.checkOutputSpecs(context);
    }

}
