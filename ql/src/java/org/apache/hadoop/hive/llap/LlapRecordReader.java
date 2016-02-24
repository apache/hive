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
import java.io.InputStream;
import java.io.DataInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.RCFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hive.metastore.api.Schema;

public class LlapRecordReader<V extends WritableComparable> implements RecordReader<NullWritable, V> {

  DataInputStream din;
  Schema schema;
  Class<V> clazz;

  public LlapRecordReader(InputStream in, Schema schema, Class<V> clazz) {
    din = new DataInputStream(in);
    this.schema = schema;
    this.clazz = clazz;
  }

  public Schema getSchema() {
    return schema;
  }

  @Override
  public void close() throws IOException {
    din.close();
  }

  @Override
  public long getPos() { return 0; }

  @Override
  public float getProgress() { return 0f; }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public V createValue() {
    try {
      return clazz.newInstance();
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public boolean next(NullWritable key, V value) {
    try {
      value.readFields(din);
      return true;
    } catch (IOException io) {
      return false;
    }
  }
}
