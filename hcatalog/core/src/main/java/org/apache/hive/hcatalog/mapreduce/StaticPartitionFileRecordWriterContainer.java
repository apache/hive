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

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.mapreduce.FileRecordWriterContainer;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;

/**
 * Record writer container for tables using static partitioning. See
 * {@link FileOutputFormatContainer} for more information
 */
class StaticPartitionFileRecordWriterContainer extends FileRecordWriterContainer {
  /**
   * @param baseWriter RecordWriter to contain
   * @param context current TaskAttemptContext
   * @throws IOException
   * @throws InterruptedException
   */
  public StaticPartitionFileRecordWriterContainer(
      RecordWriter<? super WritableComparable<?>, ? super Writable> baseWriter,
      TaskAttemptContext context) throws IOException, InterruptedException {
    super(baseWriter, context);
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    Reporter reporter = InternalUtil.createReporter(context);
    getBaseRecordWriter().close(reporter);
  }

  @Override
  protected LocalFileWriter getLocalFileWriter(HCatRecord value) throws IOException, HCatException {
    return new LocalFileWriter(getBaseRecordWriter(), objectInspector, serDe, jobInfo);
  }
}
