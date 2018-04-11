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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

/**
 * HBaseSplit augments FileSplit with HBase column mapping.
 */
public class HBaseSplit extends FileSplit implements InputSplit {

  private final TableSplit tableSplit;
  private final InputSplit snapshotSplit;
  private boolean isTableSplit; // should be final but Writable

  /**
   * For Writable
   */
  public HBaseSplit() {
    super((Path) null, 0, 0, (String[]) null);
    tableSplit = new TableSplit();
    snapshotSplit = HBaseTableSnapshotInputFormatUtil.createTableSnapshotRegionSplit();
  }

  public HBaseSplit(TableSplit tableSplit, Path dummyPath) {
    super(dummyPath, 0, 0, (String[]) null);
    this.tableSplit = tableSplit;
    this.snapshotSplit = HBaseTableSnapshotInputFormatUtil.createTableSnapshotRegionSplit();
    this.isTableSplit = true;
  }

  /**
   * TODO: use TableSnapshotRegionSplit HBASE-11555 is fixed.
   */
  public HBaseSplit(InputSplit snapshotSplit, Path dummyPath) {
    super(dummyPath, 0, 0, (String[]) null);
    this.tableSplit = new TableSplit();
    this.snapshotSplit = snapshotSplit;
    this.isTableSplit = false;
  }

  public TableSplit getTableSplit() {
    assert isTableSplit;
    return this.tableSplit;
  }

  public InputSplit getSnapshotSplit() {
    assert !isTableSplit;
    return this.snapshotSplit;
  }

  @Override
  public String toString() {
    return "" + (isTableSplit ? tableSplit : snapshotSplit);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.isTableSplit = in.readBoolean();
    if (this.isTableSplit) {
      tableSplit.readFields(in);
    } else {
      snapshotSplit.readFields(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeBoolean(isTableSplit);
    if (isTableSplit) {
      tableSplit.write(out);
    } else {
      snapshotSplit.write(out);
    }
  }

  @Override
  public long getLength() {
    long val = 0;
    try {
      val = isTableSplit ? tableSplit.getLength() : snapshotSplit.getLength();
    } finally {
      return val;
    }
  }

  @Override
  public String[] getLocations() throws IOException {
    return isTableSplit ? tableSplit.getLocations() : snapshotSplit.getLocations();
  }
}
