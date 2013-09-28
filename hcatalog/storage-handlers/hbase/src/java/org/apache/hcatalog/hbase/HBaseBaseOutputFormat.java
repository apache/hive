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

package org.apache.hcatalog.hbase;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hive.ql.io.FSRecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.mapreduce.OutputJobInfo;

public class HBaseBaseOutputFormat implements OutputFormat<WritableComparable<?>, Put>,
  HiveOutputFormat<WritableComparable<?>, Put> {

  @Override
  public FSRecordWriter getHiveRecordWriter(
    JobConf jc, Path finalOutPath,
    Class<? extends Writable> valueClass, boolean isCompressed,
    Properties tableProperties, Progressable progress)
    throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    OutputFormat<WritableComparable<?>, Put> outputFormat = getOutputFormat(job);
    outputFormat.checkOutputSpecs(ignored, job);
  }

  @Override
  public RecordWriter<WritableComparable<?>, Put> getRecordWriter(FileSystem ignored,
                                  JobConf job, String name, Progressable progress) throws IOException {
    OutputFormat<WritableComparable<?>, Put> outputFormat = getOutputFormat(job);
    return outputFormat.getRecordWriter(ignored, job, name, progress);
  }

  private OutputFormat<WritableComparable<?>, Put> getOutputFormat(JobConf job)
    throws IOException {
    String outputInfo = job.get(HCatConstants.HCAT_KEY_OUTPUT_INFO);
    OutputJobInfo outputJobInfo = (OutputJobInfo) HCatUtil.deserialize(outputInfo);
    OutputFormat<WritableComparable<?>, Put> outputFormat = null;
    if (HBaseHCatStorageHandler.isBulkMode(outputJobInfo)) {
      outputFormat = new HBaseBulkOutputFormat();
    } else {
      outputFormat = new HBaseDirectOutputFormat();
    }
    return outputFormat;
  }
}
