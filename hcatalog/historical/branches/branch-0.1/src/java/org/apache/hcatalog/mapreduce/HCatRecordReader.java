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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.data.HCatRecord;

/** The Howl wrapper for the underlying RecordReader, this ensures that the initialize on
 * the underlying record reader is done with the underlying split, not with HowlSplit.
 */
class HCatRecordReader extends RecordReader<WritableComparable, HCatRecord> {

    /** The underlying record reader to delegate to. */
    private final RecordReader<? extends WritableComparable, ? extends Writable> baseRecordReader;

    /** The storage driver used */
    private final HCatInputStorageDriver storageDriver;

    /**
     * Instantiates a new howl record reader.
     * @param baseRecordReader the base record reader
     */
    public HCatRecordReader(HCatInputStorageDriver storageDriver, RecordReader<? extends WritableComparable, ? extends Writable> baseRecordReader) {
        this.baseRecordReader = baseRecordReader;
        this.storageDriver = storageDriver;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext taskContext)
    throws IOException, InterruptedException {
        InputSplit baseSplit = split;

        if( split instanceof HCatSplit ) {
            baseSplit = ((HCatSplit) split).getBaseSplit();
        }

        baseRecordReader.initialize(baseSplit, taskContext);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
     */
    @Override
    public WritableComparable getCurrentKey() throws IOException, InterruptedException {
        return baseRecordReader.getCurrentKey();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
     */
    @Override
    public HCatRecord getCurrentValue() throws IOException, InterruptedException {
        return storageDriver.convertToHCatRecord(baseRecordReader.getCurrentKey(),baseRecordReader.getCurrentValue());
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return baseRecordReader.getProgress();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return baseRecordReader.nextKeyValue();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#close()
     */
    @Override
    public void close() throws IOException {
        baseRecordReader.close();
    }
}
