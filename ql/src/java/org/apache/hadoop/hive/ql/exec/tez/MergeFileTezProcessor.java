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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.tez;

import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;

import java.io.IOException;
import java.util.Map;

/**
 * Tez processor for fast file merging. This is same as TezProcessor except it
 * has different record processor.
 */
public class MergeFileTezProcessor extends TezProcessor {

  public MergeFileTezProcessor(ProcessorContext context) {
    super(context);
  }

  @Override
  public void run(Map<String, LogicalInput> inputs,
      Map<String, LogicalOutput> outputs) throws Exception {
    rproc = new MergeFileRecordProcessor(jobConf, getContext());
    initializeAndRunProcessor(inputs, outputs);
  }
}
