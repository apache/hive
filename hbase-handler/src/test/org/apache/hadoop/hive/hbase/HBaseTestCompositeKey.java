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

package org.apache.hadoop.hive.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;

import java.util.Properties;

public class HBaseTestCompositeKey extends HBaseCompositeKey {

  byte[] bytes;
  String bytesAsString;
  Properties tbl;
  Configuration conf;

  public HBaseTestCompositeKey(LazySimpleStructObjectInspector oi, Properties tbl, Configuration conf) {
    super(oi);
    this.tbl = tbl;
    this.conf = conf;
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    this.bytes = bytes.getData();
  }

  @Override
  public Object getField(int fieldID) {
    if (bytesAsString == null) {
      bytesAsString = Bytes.toString(bytes).trim();
    }

    // Randomly pick the character corresponding to the field id and convert it to byte array
    byte[] fieldBytes = new byte[] {(byte) bytesAsString.charAt(fieldID)};

    return toLazyObject(fieldID, fieldBytes);
  }
}
