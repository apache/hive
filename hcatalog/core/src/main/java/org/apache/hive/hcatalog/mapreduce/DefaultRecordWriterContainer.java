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

import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;

/**
 * Part of the DefaultOutput*Container classes
 * See {@link DefaultOutputFormatContainer} for more information
 */
class DefaultRecordWriterContainer extends RecordWriterContainer {

  private final HiveStorageHandler storageHandler;
  private final SerDe serDe;
  private final OutputJobInfo jobInfo;
  private final ObjectInspector hcatRecordOI;

  /**
   * @param context current JobContext
   * @param baseRecordWriter RecordWriter to contain
   * @throws IOException
   * @throws InterruptedException
   */
  public DefaultRecordWriterContainer(TaskAttemptContext context,
                    org.apache.hadoop.mapred.RecordWriter<? super WritableComparable<?>, ? super Writable> baseRecordWriter) throws IOException, InterruptedException {
    super(context, baseRecordWriter);
    jobInfo = HCatOutputFormat.getJobInfo(context);
    storageHandler = HCatUtil.getStorageHandler(context.getConfiguration(), jobInfo.getTableInfo().getStorerInfo());
    HCatOutputFormat.configureOutputStorageHandler(context);
    serDe = ReflectionUtils.newInstance(storageHandler.getSerDeClass(), context.getConfiguration());
    hcatRecordOI = InternalUtil.createStructObjectInspector(jobInfo.getOutputSchema());
    try {
      InternalUtil.initializeOutputSerDe(serDe, context.getConfiguration(), jobInfo);
    } catch (SerDeException e) {
      throw new IOException("Failed to initialize SerDe", e);
    }
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException,
    InterruptedException {
    getBaseRecordWriter().close(InternalUtil.createReporter(context));
  }

  @Override
  public void write(WritableComparable<?> key, HCatRecord value) throws IOException,
    InterruptedException {
    try {
      getBaseRecordWriter().write(null, serDe.serialize(value.getAll(), hcatRecordOI));
    } catch (SerDeException e) {
      throw new IOException("Failed to serialize object", e);
    }
  }

}
