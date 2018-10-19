/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JdbcInputSplit extends FileSplit implements InputSplit {

  private static final String[] EMPTY_ARRAY = new String[] {};

  private int limit = 0;
  private int offset = 0;
  private String partitionColumn = null;
  private String lowerBound = null;
  private String upperBound = null;


  public JdbcInputSplit() {
    super(null, 0, 0, EMPTY_ARRAY);
    this.limit = -1;
    this.offset = 0;
  }

  public JdbcInputSplit(Path dummyPath) {
    super(dummyPath, 0, 0, EMPTY_ARRAY);
    this.limit = -1;
    this.offset = 0;
  }

  public JdbcInputSplit(int limit, int offset, Path dummyPath) {
    super(dummyPath, 0, 0, EMPTY_ARRAY);
    this.limit = limit;
    this.offset = offset;
  }

  public JdbcInputSplit(int limit, int offset) {
    super(null, 0, 0, EMPTY_ARRAY);
    this.limit = limit;
    this.offset = offset;
  }

  public JdbcInputSplit(String partitionColumn, String lowerBound, String upperBound) {
    super(null, 0, 0, EMPTY_ARRAY);
    this.partitionColumn = partitionColumn;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(limit);
    out.writeInt(offset);
    if (partitionColumn != null) {
      out.writeBoolean(true);
      out.writeUTF(partitionColumn);
      out.writeUTF(lowerBound);
      out.writeUTF(upperBound);
    } else {
      out.writeBoolean(false);
    }
  }


  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    limit = in.readInt();
    offset = in.readInt();
    boolean partitionColumnExists = in.readBoolean();
    if (partitionColumnExists) {
      partitionColumn = in.readUTF();
      lowerBound = in.readUTF();
      upperBound = in.readUTF();
    }
  }


  @Override
  public long getLength() {
    return limit;
  }


  @Override
  public String[] getLocations() throws IOException {
    return EMPTY_ARRAY;
  }


  public int getLimit() {
    return limit;
  }


  public void setLimit(int limit) {
    this.limit = limit;
  }


  public int getOffset() {
    return offset;
  }


  public void setOffset(int offset) {
    this.offset = offset;
  }

  public String getPartitionColumn() {
    return this.partitionColumn;
  }

  public String getLowerBound() {
    return this.lowerBound;
  }

  public String getUpperBound() {
    return this.upperBound;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (partitionColumn != null) {
      sb.append("interval:");
      sb.append(partitionColumn).append("[");
      if (lowerBound != null) {
        sb.append(lowerBound);
      }
      sb.append(",");
      if (upperBound != null) {
        sb.append(upperBound);
      }
      sb.append(")");
    } else {
      sb.append("limit:" + limit + ", offset:" + offset);
    }
    return sb.toString();
  }
}
