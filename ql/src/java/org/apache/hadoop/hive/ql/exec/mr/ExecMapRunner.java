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

package org.apache.hadoop.hive.ql.exec.mr;

import java.io.IOException;

import org.apache.hadoop.mapred.MapRunner;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class ExecMapRunner<K1, V1, K2, V2> extends MapRunner<K1, V1, K2, V2> {
  @Override
  public void run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output, Reporter reporter) throws IOException {
    Mapper<K1, V1, K2, V2> mapper = getMapper();
    if (mapper instanceof ExecMapper) {
      ExecMapper execMapper = (ExecMapper) mapper;
      execMapper.ensureOutputInitialize(output, reporter);
    }
    super.run(input, output, reporter);
  }

}
