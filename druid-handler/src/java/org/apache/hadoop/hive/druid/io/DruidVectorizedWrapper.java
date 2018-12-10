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

package org.apache.hadoop.hive.druid.io;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.druid.serde.DruidQueryRecordReader;
import org.apache.hadoop.hive.druid.serde.DruidSerDe;
import org.apache.hadoop.hive.druid.serde.DruidWritable;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorAssignRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.Properties;

/**
 * A Wrapper class that consumes row-by-row from base Druid Record Reader and provides a Vectorized one.
 * @param <T> type of the Druid query.
 */
public class DruidVectorizedWrapper<T extends Comparable<T>> implements RecordReader<NullWritable, VectorizedRowBatch> {
  private final VectorAssignRow vectorAssignRow = new VectorAssignRow();
  private final DruidQueryRecordReader baseReader;
  private final VectorizedRowBatchCtx rbCtx;
  private final DruidSerDe serDe;
  private final Object[] rowBoat;

  /**
   * Actual projected columns needed by the query, this can be empty in case of query like: select count(*) from src.
   */
  private final int[] projectedColumns;

  private final DruidWritable druidWritable;

  public DruidVectorizedWrapper(DruidQueryRecordReader reader, Configuration jobConf) {
    this.rbCtx = Utilities.getVectorizedRowBatchCtx(jobConf);
    if (rbCtx.getDataColumnNums() != null) {
      projectedColumns = rbCtx.getDataColumnNums();
    } else {
      // case all the columns are selected
      projectedColumns = new int[rbCtx.getRowColumnTypeInfos().length];
      for (int i = 0; i < projectedColumns.length; i++) {
        projectedColumns[i] = i;
      }
    }
    this.serDe = createAndInitializeSerde(jobConf);
    this.baseReader = Preconditions.checkNotNull(reader);

    // row parser and row assigner initializing
    try {
      vectorAssignRow.init((StructObjectInspector) serDe.getObjectInspector());
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }

    druidWritable = baseReader.createValue();
    rowBoat = new Object[rbCtx.getDataColumnCount()];
  }

  @Override public boolean next(NullWritable nullWritable, VectorizedRowBatch vectorizedRowBatch) throws IOException {
    vectorizedRowBatch.reset();
    int rowsCount = 0;
    while (rowsCount < vectorizedRowBatch.getMaxSize() && baseReader.next(nullWritable, druidWritable)) {
      if (projectedColumns.length > 0) {
        try {
          serDe.deserializeAsPrimitive(druidWritable, rowBoat);
        } catch (SerDeException e) {
          throw new IOException(e);
        }
        for (int i : projectedColumns) {
          vectorAssignRow.assignRowColumn(vectorizedRowBatch, rowsCount, i, rowBoat[i]);
        }
      }
      rowsCount++;
    }
    vectorizedRowBatch.size = rowsCount;
    return rowsCount > 0;
  }

  @Override public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override public VectorizedRowBatch createValue() {
    return rbCtx.createVectorizedRowBatch();
  }

  @Override public long getPos() throws IOException {
    return baseReader.getPos();
  }

  @Override public void close() throws IOException {
    baseReader.close();
  }

  @Override public float getProgress() throws IOException {
    return baseReader.getProgress();
  }

  private static DruidSerDe createAndInitializeSerde(Configuration jobConf) {
    DruidSerDe serDe = new DruidSerDe();
    MapWork mapWork = Preconditions.checkNotNull(Utilities.getMapWork(jobConf), "Map work is null");
    Properties
        properties =
        mapWork.getPartitionDescs()
            .stream()
            .map(partitionDesc -> partitionDesc.getTableDesc().getProperties())
            .findAny()
            .orElseThrow(() -> new RuntimeException("Can not find table property at the map work"));
    try {
      serDe.initialize(jobConf, properties, null);
    } catch (SerDeException e) {
      throw new RuntimeException("Can not initialized the serde", e);
    }
    return serDe;
  }
}
