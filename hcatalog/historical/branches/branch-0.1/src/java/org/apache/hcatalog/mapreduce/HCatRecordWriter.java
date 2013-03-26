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
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.HCatRecord;

public class HCatRecordWriter extends RecordWriter<WritableComparable<?>, HCatRecord> {

    private final HCatOutputStorageDriver storageDriver;
    /**
     * @return the storageDriver
     */
    public HCatOutputStorageDriver getStorageDriver() {
      return storageDriver;
    }

    private final RecordWriter<? super WritableComparable<?>, ? super Writable> baseWriter;
    private final List<Integer> partColsToDel;

    public HCatRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

      OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(context);

      // If partition columns occur in data, we want to remove them.
      partColsToDel = jobInfo.getPosOfPartCols();

      if(partColsToDel == null){
        throw new HCatException("It seems that setSchema() is not called on " +
        		"HowlOutputFormat. Please make sure that method is called.");
      }

      this.storageDriver = HCatOutputFormat.getOutputDriverInstance(context, jobInfo);
      this.baseWriter = storageDriver.getOutputFormat().getRecordWriter(context);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
        baseWriter.close(context);
    }

    @Override
    public void write(WritableComparable<?> key, HCatRecord value) throws IOException,
            InterruptedException {

      for(Integer colToDel : partColsToDel){
        value.remove(colToDel);
      }
        //The key given by user is ignored
        WritableComparable<?> generatedKey = storageDriver.generateKey(value);
        Writable convertedValue = storageDriver.convertValue(value);
        baseWriter.write(generatedKey, convertedValue);
    }
}
