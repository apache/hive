/**
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.mapreduce;


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.data.HCatRecord;

/**
 *  This class will contain an implementation of an RecordWriter.
 *  See {@link OutputFormatContainer} for more information about containers.
 */
abstract class RecordWriterContainer extends  RecordWriter<WritableComparable<?>, HCatRecord> {

  private final org.apache.hadoop.mapred.RecordWriter<? super WritableComparable<?>, ? super Writable> baseRecordWriter;

  /**
   * @param context current JobContext
   * @param baseRecordWriter RecordWriter that this instance will contain
   */
  public RecordWriterContainer(TaskAttemptContext context,
                 org.apache.hadoop.mapred.RecordWriter<? super WritableComparable<?>, ? super Writable> baseRecordWriter) {
    this.baseRecordWriter = baseRecordWriter;
  }

  /**
   * @return underlying RecordWriter
   */
  public org.apache.hadoop.mapred.RecordWriter getBaseRecordWriter() {
    return baseRecordWriter;
  }

}
