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

package org.apache.hadoop.hive.llap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.hive.ql.io.arrow.RootAllocatorFactory;
import java.util.UUID;

/*
 * Adapts an Arrow batch reader to a row reader
 * Only used for testing
 */
public class LlapArrowRowInputFormat implements InputFormat<NullWritable, Row> {

  private LlapBaseInputFormat baseInputFormat;

  public LlapArrowRowInputFormat(long arrowAllocatorLimit) {
    BufferAllocator allocator = RootAllocatorFactory.INSTANCE.getOrCreateRootAllocator(arrowAllocatorLimit).newChildAllocator(
        //allocator name, use UUID for testing
        UUID.randomUUID().toString(),
        //No use for reservation, allocators claim memory from the same pool,
        //but allocate/releases are tracked per-allocator
        0,
        //Limit passed in by client
        arrowAllocatorLimit);
    baseInputFormat = new LlapBaseInputFormat(true, allocator);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return baseInputFormat.getSplits(job, numSplits);
  }

  @Override
  public RecordReader<NullWritable, Row> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    LlapInputSplit llapSplit = (LlapInputSplit) split;
    LlapArrowBatchRecordReader reader =
        (LlapArrowBatchRecordReader) baseInputFormat.getRecordReader(llapSplit, job, reporter);
    return new LlapArrowRowRecordReader(job, reader.getSchema(), reader);
  }
}
