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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.data.HCatRecord;

/**
 * Part of the DefaultOutput*Container classes
 * See {@link DefaultOutputFormatContainer} for more information
 */
class DefaultRecordWriterContainer extends RecordWriterContainer {

    private final HCatOutputStorageDriver storageDriver;
    private final RecordWriter<? super WritableComparable<?>, ? super Writable> baseRecordWriter;
    private final OutputJobInfo jobInfo;

    /**
     * @param context current JobContext
     * @param baseRecordWriter RecordWriter to contain
     * @throws IOException
     * @throws InterruptedException
     */
    public DefaultRecordWriterContainer(TaskAttemptContext context,
                                        RecordWriter<? super WritableComparable<?>, ? super Writable> baseRecordWriter) throws IOException, InterruptedException {
        super(context,baseRecordWriter);
        jobInfo = HCatOutputFormat.getJobInfo(context);
        this.storageDriver = HCatOutputFormat.getOutputDriverInstance(context, jobInfo);
        this.baseRecordWriter = baseRecordWriter;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
        baseRecordWriter.close(context);
    }

    @Override
    public void write(WritableComparable<?> key, HCatRecord value) throws IOException,
            InterruptedException {
        WritableComparable<?> generatedKey = storageDriver.generateKey(value);
        Writable convertedValue = storageDriver.convertValue(value);
        baseRecordWriter.write(generatedKey, convertedValue);
    }

    @Override
    public RecordWriter<? super WritableComparable<?>, ? super Writable> getBaseRecordWriter() {
        return baseRecordWriter;
    }
}
