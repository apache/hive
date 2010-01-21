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

package org.apache.hadoop.hive.contrib.util.typedbytes;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.RecordWriter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

public class TypedBytesRecordWriter implements RecordWriter {

  private OutputStream out;

  public void initialize(OutputStream out, Configuration conf)
      throws IOException {
    this.out = out;
  }

  public void write(Writable row) throws IOException {
    BytesWritable brow = (BytesWritable) row;
    out.write(brow.get(), 0, brow.getSize());
  }

  public void close() throws IOException {
    out.flush();
    out.close();
  }
}
