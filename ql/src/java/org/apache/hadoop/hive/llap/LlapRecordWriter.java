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

package org.apache.hadoop.hive.llap;

import java.io.IOException;
import java.io.OutputStream;
import java.io.DataOutputStream;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapRecordWriter<K extends Writable, V extends WritableComparable>
  implements RecordWriter<K,V> {
  public static final Logger LOG = LoggerFactory.getLogger(LlapRecordWriter.class);

  DataOutputStream dos;

  public LlapRecordWriter(OutputStream out) {
    dos = new DataOutputStream(out);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    LOG.info("CLOSING the record writer output stream");
    dos.close();
  }

  @Override
  public void write(K key, V value) throws IOException {
    value.write(dos);
  }
}
