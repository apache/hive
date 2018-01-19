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
package org.apache.hadoop.hive.ql.index;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

public final class MockHiveInputSplits {
  private static final String[] HOSTS = {};
  private static final String INPUT_FORMAT_CLASS_NAME = SequenceFileInputFormat.class.getCanonicalName();

  private MockHiveInputSplits() {
  }

  public static HiveInputSplit createMockSplit(String pathString, long start, long length) {
    InputSplit inputSplit = new FileSplit(new Path(pathString), start, length, HOSTS);
    return new HiveInputSplit(inputSplit, INPUT_FORMAT_CLASS_NAME);
  }
}
