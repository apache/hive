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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class HivePassThroughRecordWriter <K extends WritableComparable<?>, V extends Writable>
implements RecordWriter {

  private final org.apache.hadoop.mapred.RecordWriter<K, V> mWriter;

  public HivePassThroughRecordWriter(org.apache.hadoop.mapred.RecordWriter<K, V> writer) {
    this.mWriter = writer;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(Writable r) throws IOException {
    mWriter.write(null, (V) r);
  }

  @Override
  public void close(boolean abort) throws IOException {
    //close with null reporter
    mWriter.close(null);
  }
}

