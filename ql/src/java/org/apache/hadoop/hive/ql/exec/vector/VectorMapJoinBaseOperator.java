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

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.persistence.HybridHashTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.ObjectContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.DataOutputBuffer;

/**
 * The *NON-NATIVE* base vector map join operator class used by VectorMapJoinOperator and
 * VectorMapJoinOuterFilteredOperator.
 *
 * It has common variables and code for the output batch, Hybrid Grace spill batch, and more.
 */
public class VectorMapJoinBaseOperator extends MapJoinOperator implements VectorizationContextRegion {

  private static final Log LOG = LogFactory.getLog(VectorMapJoinBaseOperator.class.getName());

  private static final long serialVersionUID = 1L;

  protected VectorizationContext vOutContext;

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  protected transient VectorizedRowBatch outputBatch;
  protected transient VectorizedRowBatch scratchBatch;  // holds restored (from disk) big table rows

  protected transient Map<ObjectInspector, VectorAssignRowSameBatch> outputVectorAssignRowMap;

  protected transient VectorizedRowBatchCtx vrbCtx = null;

  protected transient int tag;  // big table alias

  public VectorMapJoinBaseOperator() {
    super();
  }

  public VectorMapJoinBaseOperator (VectorizationContext vContext, OperatorDesc conf)
    throws HiveException {
    super();

    MapJoinDesc desc = (MapJoinDesc) conf;
    this.conf = desc;

    order = desc.getTagOrder();
    numAliases = desc.getExprs().size();
    posBigTable = (byte) desc.getPosBigTable();
    filterMaps = desc.getFilterMap();
    noOuterJoin = desc.isNoOuterJoin();

     // We are making a new output vectorized row batch.
    vOutContext = new VectorizationContext(getName(), desc.getOutputColumnNames());
  }

  @Override
  public Collection<Future<?>> initializeOp(Configuration hconf) throws HiveException {

    Collection<Future<?>> result = super.initializeOp(hconf);

    vrbCtx = new VectorizedRowBatchCtx();
    vrbCtx.init(vOutContext.getScratchColumnTypeMap(), (StructObjectInspector) this.outputObjInspector);

    outputBatch = vrbCtx.createVectorizedRowBatch();

    outputVectorAssignRowMap = new HashMap<ObjectInspector, VectorAssignRowSameBatch>();

    return result;
  }

  /**
   * 'forwards' the (row-mode) record into the (vectorized) output batch
   */
  @Override
  protected void internalForward(Object row, ObjectInspector outputOI) throws HiveException {
    Object[] values = (Object[]) row;
    VectorAssignRowSameBatch va = outputVectorAssignRowMap.get(outputOI);
    if (va == null) {
      va = new VectorAssignRowSameBatch();
      va.init((StructObjectInspector) outputOI, vOutContext.getProjectedColumns());
      va.setOneBatch(outputBatch);
      outputVectorAssignRowMap.put(outputOI, va);
    }

    va.assignRow(outputBatch.size, values);

    ++outputBatch.size;
    if (outputBatch.size == VectorizedRowBatch.DEFAULT_SIZE) {
      flushOutput();
    }
  }

  private void flushOutput() throws HiveException {
    forward(outputBatch, null);
    outputBatch.reset();
  }

  @Override
  public void closeOp(boolean aborted) throws HiveException {
    super.closeOp(aborted);
    for (MapJoinTableContainer tableContainer : mapJoinTables) {
      if (tableContainer != null) {
        tableContainer.dumpMetrics();
      }
    }
    if (!aborted && 0 < outputBatch.size) {
      flushOutput();
    }
  }

  /**
   * For a vectorized row batch from the rows feed from the super MapJoinOperator.
   */
  @Override
  protected void reProcessBigTable(int partitionId)
      throws HiveException {

    if (scratchBatch == null) {
      // The process method was not called -- no big table rows.
      return;
    }

    HybridHashTableContainer.HashPartition partition = firstSmallTable.getHashPartitions()[partitionId];
    ObjectContainer bigTable = partition.getMatchfileObjContainer();

    DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
    while (bigTable.hasNext()) {
      Object row = bigTable.next();
      VectorizedBatchUtil.addProjectedRowToBatchFrom(row,
          (StructObjectInspector) inputObjInspectors[posBigTable],
          scratchBatch.size, scratchBatch, dataOutputBuffer);
      scratchBatch.size++;

      if (scratchBatch.size == VectorizedRowBatch.DEFAULT_SIZE) {
        process(scratchBatch, tag); // call process once we have a full batch
        scratchBatch.reset();
        dataOutputBuffer.reset();
      }
    }
    // Process the row batch that has less than DEFAULT_SIZE rows
    if (scratchBatch.size > 0) {
      process(scratchBatch, tag);
      scratchBatch.reset();
      dataOutputBuffer.reset();
    }
    bigTable.clear();
  }

  @Override
  public VectorizationContext getOuputVectorizationContext() {
    return vOutContext;
  }
}