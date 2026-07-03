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

import static org.apache.hadoop.hive.ql.anon.utils.Utils.createWritable;

public class ValueItem implements Writable {
  public Writable filePath;
  private final List<LocatorSchemaItem> itemList = new ArrayList<>();

  private String types;

  public ValueItem(String types) {
    this.types = types;
    filePath = createWritable(types.charAt(0));
  }

  public ValueItem() {
  }

  public void add(LocatorSchemaItem item) {
    itemList.add(item);
  }

  public void clear() {
    itemList.clear();
  }

  public List<LocatorSchemaItem> getItemList() {
    return itemList;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    filePath.write(out);
    out.writeInt(itemList.size());
    for (LocatorSchemaItem item : itemList) {
      item.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    filePath.readFields(in);
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      LocatorSchemaItem item = new LocatorSchemaItem(types);
      item.readFields(in);
      itemList.add(item);
    }
  }
}
