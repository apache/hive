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

package org.apache.hadoop.hive.ql.anon.index.tab;

import org.apache.hadoop.hive.ql.anon.btree.StructValueList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.hadoop.hive.ql.anon.utils.Utils.createWritable;

public class TabularEntry implements Writable {

  private WritableComparable key;
  private StructValueList value;

  public TabularEntry(final WritableComparable key, final StructValueList value) {
    this.key = key;
    this.value = value;
  }

  public WritableComparable getKey() {
    return key;
  }

  public StructValueList getValue() {
    return value;
  }

  public void clear() {
    value.clear();
  }

  public TabularEntry(String keyType, String valueTypes) {
    key = (WritableComparable) createWritable(keyType.charAt(0));
    value = new StructValueList(valueTypes);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    key.write(out);
    value.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    key.readFields(in);
    value.readFields(in);
  }
}
