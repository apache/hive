/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.accumulo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

/**
 * Holds column tuples for rowID. Each tuple contains column family label, qualifier label, and byte
 * array value.
 */
public class AccumuloHiveRow implements Writable {

  private String rowId;
  private List<ColumnTuple> tuples = new ArrayList<ColumnTuple>();

  public AccumuloHiveRow() {}

  public AccumuloHiveRow(String rowId) {
    this.rowId = rowId;
  }

  public void setRowId(String rowId) {
    this.rowId = rowId;
  }

  public List<ColumnTuple> getTuples() {
    return Collections.unmodifiableList(tuples);
  }

  /**
   * @return true if this instance has a tuple containing fam and qual, false otherwise.
   */
  public boolean hasFamAndQual(Text fam, Text qual) {
    for (ColumnTuple tuple : tuples) {
      if (tuple.getCf().equals(fam) && tuple.getCq().equals(qual)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return byte [] value for first tuple containing fam and qual or null if no match.
   */
  public byte[] getValue(Text fam, Text qual) {
    for (ColumnTuple tuple : tuples) {
      if (tuple.getCf().equals(fam) && tuple.getCq().equals(qual)) {
        return tuple.getValue();
      }
    }
    return null;
  }

  public String getRowId() {
    return rowId;
  }

  public void clear() {
    this.rowId = null;
    this.tuples = new ArrayList<ColumnTuple>();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("AccumuloHiveRow{");
    builder.append("rowId='").append(rowId).append("', tuples: ");
    for (ColumnTuple tuple : tuples) {
      builder.append(tuple.toString());
      builder.append("\n");
    }
    return builder.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof AccumuloHiveRow) {
      AccumuloHiveRow other = (AccumuloHiveRow) o;
      if (null == rowId) {
        if (null != other.rowId) {
          return false;
        }
      } else if (!rowId.equals(other.rowId)) {
        return false;
      }

      return tuples.equals(other.tuples);
    }

    return false;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    if (null != rowId) {
      dataOutput.writeBoolean(true);
      dataOutput.writeUTF(rowId);
    } else {
      dataOutput.writeBoolean(false);
    }
    int size = tuples.size();
    dataOutput.writeInt(size);
    for (ColumnTuple tuple : tuples) {
      Text cf = tuple.getCf(), cq = tuple.getCq();
      dataOutput.writeInt(cf.getLength());
      dataOutput.write(cf.getBytes(), 0, cf.getLength());
      dataOutput.writeInt(cq.getLength());
      dataOutput.write(cq.getBytes(), 0, cq.getLength());
      byte[] value = tuple.getValue();
      dataOutput.writeInt(value.length);
      dataOutput.write(value);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    if (dataInput.readBoolean()) {
      rowId = dataInput.readUTF();
    }
    int size = dataInput.readInt();
    for (int i = 0; i < size; i++) {
      int cfLength = dataInput.readInt();
      byte[] cfData = new byte[cfLength];
      dataInput.readFully(cfData, 0, cfLength);
      Text cf = new Text(cfData);
      int cqLength = dataInput.readInt();
      byte[] cqData = new byte[cqLength];
      dataInput.readFully(cqData, 0, cqLength);
      Text cq = new Text(cqData);
      int valSize = dataInput.readInt();
      byte[] val = new byte[valSize];
      for (int j = 0; j < valSize; j++) {
        val[j] = dataInput.readByte();
      }
      tuples.add(new ColumnTuple(cf, cq, val));
    }
  }

  public void add(String cf, String qual, byte[] val) {
    Preconditions.checkNotNull(cf);
    Preconditions.checkNotNull(qual);
    Preconditions.checkNotNull(val);

    add(new Text(cf), new Text(qual), val);
  }

  public void add(Text cf, Text qual, byte[] val) {
    Preconditions.checkNotNull(cf);
    Preconditions.checkNotNull(qual);
    Preconditions.checkNotNull(val);

    tuples.add(new ColumnTuple(cf, qual, val));
  }

  public static class ColumnTuple {
    private final Text cf;
    private final Text cq;
    private final byte[] value;

    public ColumnTuple(Text cf, Text cq, byte[] value) {
      this.value = value;
      this.cf = cf;
      this.cq = cq;
    }

    public byte[] getValue() {
      return value;
    }

    public Text getCf() {
      return cf;
    }

    public Text getCq() {
      return cq;
    }

    @Override
    public int hashCode() {
      HashCodeBuilder hcb = new HashCodeBuilder(9683, 68783);
      return hcb.append(cf).append(cq).append(value).toHashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ColumnTuple) {
        ColumnTuple other = (ColumnTuple) o;
        if (null == cf) {
          if (null != other.cf) {
            return false;
          }
        } else if (!cf.equals(other.cf)) {
          return false;
        }

        if (null == cq) {
          if (null != other.cq) {
            return false;
          }
        } else if (!cq.equals(other.cq)) {
          return false;
        }

        if (null == value) {
          if (null != other.value) {
            return false;
          }
        }

        return Arrays.equals(value, other.value);
      }

      return false;
    }

    @Override
    public String toString() {
      return cf + " " + cq + " " + new String(value);
    }
  }
}
