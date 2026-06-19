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

package org.apache.hadoop.hive.ql.anon.btree;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StructValueList implements Writable {

  private final List<ValueItem> items = new ArrayList<>();
  private String types;

  public StructValueList(String types) {
    this.types = types;
  }

  public StructValueList() {
  }

  public void add(ValueItem item) {
    items.add(item);
  }

  public void clear() {
    for (ValueItem item : items) {
      item.clear();
    }
    items.clear();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(items.size());
    for (ValueItem item : items) {
      item.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      ValueItem item = new ValueItem(types);
      item.readFields(in);
      items.add(item);
    }
  }

  @Override
  public String toString() {
    return "items#: " + items.size();
  }

  public List<ValueItem> getItems() {
    return items;
  }

  public String getTypes() {
    return types;
  }
}
