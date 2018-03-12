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

package org.apache.hadoop.hive.ql.parse.spark;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkPartitionPruningSinkDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * This operator gets partition info from the upstream operators, and write them
 * to HDFS. This will later be read at the driver, and used for pruning the partitions
 * for the big table side.
 */
public class SparkPartitionPruningSinkOperator extends Operator<SparkPartitionPruningSinkDesc> {

  @SuppressWarnings("deprecation")
  protected transient Serializer serializer;
  protected transient DataOutputBuffer buffer;
  protected static final Logger LOG = LoggerFactory.getLogger(SparkPartitionPruningSinkOperator.class);
  private static final AtomicLong SEQUENCE_NUM = new AtomicLong(0);

  private transient String uniqueId = null;

  /** Kryo ctor. */
  @VisibleForTesting
  public SparkPartitionPruningSinkOperator() {
    super();
  }

  public SparkPartitionPruningSinkOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  @SuppressWarnings("deprecation")
  public void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    serializer = (Serializer) ReflectionUtils.newInstance(
        conf.getTable().getDeserializerClass(), null);
    buffer = new DataOutputBuffer();
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    ObjectInspector rowInspector = inputObjInspectors[0];
    try {
      Writable writableRow = serializer.serialize(row, rowInspector);
      writableRow.write(buffer);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (!abort) {
      try {
        flushToFile();
      } catch (Exception e) {
        throw new HiveException(e);
      }
    }
  }

  /* This function determines whether sparkpruningsink is with mapjoin.  This will be called
     to check whether the tree should be split for dpp.  For mapjoin it won't be.  Also called
     to determine whether dpp should be enabled for anything other than mapjoin.
   */
  public boolean isWithMapjoin() {
    Operator<?> branchingOp = this.getBranchingOp();

    // Check if this is a MapJoin. If so, do not split.
    for (Operator<?> childOp : branchingOp.getChildOperators()) {
      if (childOp instanceof ReduceSinkOperator &&
          childOp.getChildOperators().get(0) instanceof MapJoinOperator) {
        return true;
      }
    }

    return false;
  }

  /* Locate the op where the branch starts.  This function works only for the following pattern.
   *     TS1       TS2
   *      |         |
   *     FIL       FIL
   *      |         |
   *      |     ---------
   *      RS    |   |   |
   *      |    RS  SEL SEL
   *      |    /    |   |
   *      |   /    GBY GBY
   *      JOIN       |  |
   *                 |  SPARKPRUNINGSINK
   *                 |
   *              SPARKPRUNINGSINK
   */
  public Operator<?> getBranchingOp() {
    Operator<?> branchingOp = this;

    while (branchingOp != null) {
      if (branchingOp.getNumChild() > 1) {
        break;
      } else {
        branchingOp = branchingOp.getParentOperators().get(0);
      }
    }

    return branchingOp;
  }

  private void flushToFile() throws IOException {
    // write an intermediate file to the specified path
    // the format of the path is: tmpPath/targetWorkId/sourceWorkId/randInt
    Path path = conf.getPath();
    FileSystem fs = path.getFileSystem(this.getConfiguration());
    fs.mkdirs(path);

    while (true) {
      path = new Path(path, String.valueOf(Utilities.randGen.nextInt()));
      if (!fs.exists(path)) {
        break;
      }
    }

    short numOfRepl = fs.getDefaultReplication(path);

    ObjectOutputStream out = null;
    FSDataOutputStream fsout = null;

    try {
      fsout = fs.create(path, numOfRepl);
      out = new ObjectOutputStream(new BufferedOutputStream(fsout));
      out.writeInt(conf.getTargetInfos().size());
      for (SparkPartitionPruningSinkDesc.DPPTargetInfo info : conf.getTargetInfos()) {
        out.writeUTF(info.columnName);
      }
      buffer.writeTo(out);
    } catch (Exception e) {
      try {
        fs.delete(path, false);
      } catch (Exception ex) {
        LOG.warn("Exception happened while trying to clean partial file.");
      }
      throw e;
    } finally {
      if (out != null) {
        LOG.info("Flushed to file: " + path);
        out.close();
      } else if (fsout != null) {
        fsout.close();
      }
    }
  }

  @Override
  public OperatorType getType() {
    return OperatorType.SPARKPRUNINGSINK;
  }

  @Override
  public String getName() {
    return SparkPartitionPruningSinkOperator.getOperatorName();
  }

  public static String getOperatorName() {
    return "SPARKPRUNINGSINK";
  }

  public synchronized String getUniqueId() {
    if (uniqueId == null) {
      uniqueId = getOperatorId() + "_" + SEQUENCE_NUM.getAndIncrement();
    }
    return uniqueId;
  }

  public synchronized void setUniqueId(String uniqueId) {
    this.uniqueId = uniqueId;
  }

  /**
   * Add this DPP sink as a pruning source for the target MapWork. It means the DPP sink's output
   * will be used to prune a certain partition in the MapWork. The MapWork's event source maps will
   * be updated to remember the DPP sink's unique ID and corresponding target columns.
   */
  public void addAsSourceEvent(MapWork mapWork, ExprNodeDesc partKey, String columnName,
      String columnType) {
    String sourceId = getUniqueId();
    SparkPartitionPruningSinkDesc conf = getConf();

    // store table descriptor in map-targetWork
    List<TableDesc> tableDescs = mapWork.getEventSourceTableDescMap().computeIfAbsent(sourceId,
        v -> new ArrayList<>());
    tableDescs.add(conf.getTable());

    // store partition key expr in map-targetWork
    List<ExprNodeDesc> partKeys = mapWork.getEventSourcePartKeyExprMap().computeIfAbsent(sourceId,
        v -> new ArrayList<>());
    partKeys.add(partKey);

    // store column name in map-targetWork
    List<String> columnNames = mapWork.getEventSourceColumnNameMap().computeIfAbsent(sourceId,
        v -> new ArrayList<>());
    columnNames.add(columnName);

    List<String> columnTypes = mapWork.getEventSourceColumnTypeMap().computeIfAbsent(sourceId,
        v -> new ArrayList<>());
    columnTypes.add(columnType);
  }

  /**
   * Remove this DPP sink from the target MapWork's pruning source. The MapWork's event source maps
   * will be updated to remove the association between the target column and the DPP sink's unique
   * ID. If the DPP sink has no target columns after the removal, its unique ID is removed from the
   * event source maps.
   */
  public void removeFromSourceEvent(MapWork mapWork, ExprNodeDesc partKey, String columnName,
      String columnType) {
    String sourceId = getUniqueId();
    SparkPartitionPruningSinkDesc conf = getConf();

    List<TableDesc> tableDescs = mapWork.getEventSourceTableDescMap().get(sourceId);
    if (tableDescs != null) {
      tableDescs.remove(conf.getTable());
      if (tableDescs.isEmpty()) {
        mapWork.getEventSourceTableDescMap().remove(sourceId);
      }
    }

    List<ExprNodeDesc> partKeys = mapWork.getEventSourcePartKeyExprMap().get(sourceId);
    if (partKeys != null) {
      partKeys.remove(partKey);
      if (partKeys.isEmpty()) {
        mapWork.getEventSourcePartKeyExprMap().remove(sourceId);
      }
    }

    List<String> columnNames = mapWork.getEventSourceColumnNameMap().get(sourceId);
    if (columnNames != null) {
      columnNames.remove(columnName);
      if (columnNames.isEmpty()) {
        mapWork.getEventSourceColumnNameMap().remove(sourceId);
      }
    }

    List<String> columnTypes = mapWork.getEventSourceColumnTypeMap().get(sourceId);
    if (columnTypes != null) {
      columnTypes.remove(columnType);
      if (columnTypes.isEmpty()) {
        mapWork.getEventSourceColumnTypeMap().remove(sourceId);
      }
    }
  }
}
