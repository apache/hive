/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.tez.HashableInputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.mr.mapreduce.IcebergSplit;
import org.apache.iceberg.mr.mapreduce.IcebergSplitContainer;
import org.apache.iceberg.relocated.com.google.common.primitives.Longs;
import org.apache.iceberg.util.SerializationUtil;

// Hive requires file formats to return splits that are instances of `FileSplit`.
public class HiveIcebergSplit extends FileSplit implements IcebergSplitContainer, HashableInputSplit {

  private IcebergSplit innerSplit;

  // Hive uses the path name of a split to map it back to a partition (`PartitionDesc`) or table description object
  // (`TableDesc`) which specifies the relevant input format for reading the files belonging to that partition or table.
  // That way, `HiveInputFormat` and `CombineHiveInputFormat` can read files with different file formats in the same
  // MapReduce job and merge compatible splits together.
  private String tableLocation;

  // public no-argument constructor for deserialization
  public HiveIcebergSplit() {
  }

  HiveIcebergSplit(IcebergSplit split, String tableLocation) {
    this.innerSplit = split;
    this.tableLocation = tableLocation;
  }

  @Override
  public IcebergSplit icebergSplit() {
    return innerSplit;
  }

  @Override
  public long getLength() {
    return innerSplit.getLength();
  }

  @Override
  public String[] getLocations() {
    return innerSplit.getLocations();
  }

  @Override
  public Path getPath() {
    return new Path(tableLocation);
  }

  @Override
  public byte[] getBytesForHash() {
    Collection<FileScanTask> fileScanTasks = innerSplit.task().files();

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      for (FileScanTask task : fileScanTasks) {
        baos.write(task.file().path().toString().getBytes());
        baos.write(Longs.toByteArray(task.start()));
      }
      return baos.toByteArray();
    } catch (IOException ioe) {
      throw new RuntimeException("Couldn't produce hash input bytes for HiveIcebergSplit: " + this, ioe);
    }
  }

  @Override
  public long getStart() {
    return 0;
  }

  /**
   * This hack removes residual expressions from the file scan task just before split serialization.
   * Residuals can sometime take up too much space in the payload causing Tez AM to OOM.
   * Unfortunately Tez AM doesn't distribute splits in a streamed way, that is, it serializes all splits for a job
   * before sending them out to executors. Some residuals may take ~ 1 MB in memory, multiplied with thousands of splits
   * could kill the Tez AM JVM.
   * Until the streamed split distribution is implemented we will kick residuals out of the split, essentially the
   * executor side won't use it anyway (yet).
   */
  private static final Class<?> SPLIT_SCAN_TASK_CLAZZ;
  private static final DynFields.UnboundField<Object> FILE_SCAN_TASK_FIELD;
  private static final DynFields.UnboundField<Object> RESIDUALS_FIELD;
  private static final DynFields.UnboundField<Object> EXPR_FIELD;
  private static final DynFields.UnboundField<Object> UNPARTITIONED_EXPR_FIELD;

  static {
    SPLIT_SCAN_TASK_CLAZZ = DynClasses.builder().impl("org.apache.iceberg.BaseFileScanTask$SplitScanTask").build();
    FILE_SCAN_TASK_FIELD = DynFields.builder().hiddenImpl(SPLIT_SCAN_TASK_CLAZZ, "fileScanTask").build();
    RESIDUALS_FIELD = DynFields.builder().hiddenImpl("org.apache.iceberg.BaseFileScanTask", "residuals").build();
    EXPR_FIELD = DynFields.builder().hiddenImpl(ResidualEvaluator.class, "expr").build();
    UNPARTITIONED_EXPR_FIELD = DynFields.builder().hiddenImpl("org.apache.iceberg.expressions." +
        "ResidualEvaluator$UnpartitionedResidualEvaluator", "expr").build();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    for (FileScanTask fileScanTask : icebergSplit().task().files()) {
      if (fileScanTask.residual() != Expressions.alwaysTrue() &&
          fileScanTask.getClass().isAssignableFrom(SPLIT_SCAN_TASK_CLAZZ)) {

        Object residuals = RESIDUALS_FIELD.get(FILE_SCAN_TASK_FIELD.get(fileScanTask));

        if (fileScanTask.spec().isPartitioned()) {
          EXPR_FIELD.set(residuals, Expressions.alwaysTrue());
        } else {
          UNPARTITIONED_EXPR_FIELD.set(residuals, Expressions.alwaysTrue());
        }

      }
    }
    byte[] bytes = SerializationUtil.serializeToBytes(tableLocation);
    out.writeInt(bytes.length);
    out.write(bytes);

    innerSplit.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte[] bytes = new byte[in.readInt()];
    in.readFully(bytes);
    tableLocation = SerializationUtil.deserializeFromBytes(bytes);

    innerSplit = new IcebergSplit();
    innerSplit.readFields(in);
  }
}
