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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Writable;

public class Schema implements Writable {

  private final List<FieldDesc> columns;

  public Schema(List<FieldDesc> columns) {
    this.columns = columns;
  }

  public Schema() {
    columns = new ArrayList<FieldDesc>();
  }

  public List<FieldDesc> getColumns() {
    return columns;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    boolean first = true;
    for (FieldDesc colDesc : getColumns()) {
      if (!first) {
        sb.append(",");
      }
      sb.append(colDesc.toString());
      first = false;
    }
    return sb.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(columns.size());
    for (FieldDesc column : columns) {
      column.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numColumns = in.readInt();
    columns.clear();
    for (int idx = 0; idx < numColumns; ++idx) {
      FieldDesc colDesc = new FieldDesc();
      colDesc.readFields(in);
      columns.add(colDesc);
    }
  }
}
