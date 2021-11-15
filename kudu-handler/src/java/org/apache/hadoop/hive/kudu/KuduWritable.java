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
package org.apache.hadoop.hive.kudu;

import java.io.DataInput;
import java.io.DataOutput;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.Writable;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;

/**
 * A Writable representation of a Kudu Row.
 * The row may be either a PartialRow or a RowResult.
 */
public class KuduWritable implements Writable {

  private PartialRow partialRow;
  private RowResult rowResult;

  public KuduWritable(PartialRow row) {
    this.partialRow = row;
    this.rowResult = null;
  }

  public void setRow(RowResult rowResult) {
    this.rowResult = rowResult;
    this.partialRow = null;
  }

  public PartialRow getPartialRow() {
    Preconditions.checkNotNull(partialRow);
    return partialRow;
  }

  public RowResult getRowResult() {
    Preconditions.checkNotNull(rowResult);
    return rowResult;
  }

  public Object getValueObject(int colIndex) {
    if (partialRow != null) {
      return partialRow.getObject(colIndex);
    } else {
      return rowResult.getObject(colIndex);
    }
  }

  public Object getValueObject(String colName) {
    if (partialRow != null) {
      return partialRow.getObject(colName);
    } else {
      return rowResult.getObject(colName);
    }
  }

  public boolean isSet(int colIndex) {
    if (partialRow != null) {
      return partialRow.isSet(colIndex);
    } else {
      // RowResult columns are always set.
      return true;
    }
  }

  public boolean isSet(String colName) {
    if (partialRow != null) {
      return partialRow.isSet(colName);
    } else {
      // RowResult columns are always set.
      return true;
    }
  }

  public void populateRow(PartialRow row) {
    Schema schema = row.getSchema();
    for (int i = 0; i < schema.getColumnCount(); i++) {
      ColumnSchema col = schema.getColumnByIndex(i);
      if (isSet(col.getName())) {
        Object value = getValueObject(col.getName());
        row.addObject(i, value);
      }
    }
  }

  @Override
  public void readFields(DataInput in) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(DataOutput out) {
    throw new UnsupportedOperationException();
  }
}
