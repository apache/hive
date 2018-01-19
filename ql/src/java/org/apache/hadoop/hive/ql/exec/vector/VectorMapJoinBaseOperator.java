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

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.persistence.HybridHashTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.ObjectContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.DataOutputBuffer;

/**
 * The *NON-NATIVE* base vector map join operator class used by VectorMapJoinOperator and
 * VectorMapJoinOuterFilteredOperator.
 *
 * It has common variables and code for the output batch, Hybrid Grace spill batch, and more.
 */
public class VectorMapJoinBaseOperator extends MapJoinOperator
    implements VectorizationOperator, VectorizationContextRegion {

  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinBaseOperator.class.getName());

  protected VectorizationContext vContext;
  protected VectorMapJoinDesc vectorDesc;

  private static final long serialVersionUID = 1L;

  protected VectorizationContext vOutContext;

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  protected transient VectorizedRowBatch outputBatch;
  protected transient VectorizedRowBatch scratchBatch;  // holds restored (from disk) big table rows

  protected transient Map<ObjectInspector, VectorAssignRow> outputVectorAssignRowMap;

  protected transient VectorizedRowBatchCtx vrbCtx = null;

  protected transient int tag;  // big table alias

  /** Kryo ctor. */
  protected VectorMapJoinBaseOperator() {
    super();
  }

  public VectorMapJoinBaseOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorMapJoinBaseOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) throws HiveException {
    super(ctx);

    MapJoinDesc desc = (MapJoinDesc) conf;
    this.conf = desc;
    this.vContext = vContext;
    this.vectorDesc = (VectorMapJoinDesc) vectorDesc;

    order = desc.getTagOrder();
    numAliases = desc.getExprs().size();
    posBigTable = (byte) desc.getPosBigTable();
    filterMaps = desc.getFilterMap();
    noOuterJoin = desc.isNoOuterJoin();

     // We are making a new output vectorized row batch.
    vOutContext = new VectorizationContext(getName(), desc.getOutputColumnNames(),
        /* vContextEnvironment */ vContext);
    vOutContext.setInitialTypeInfos(Arrays.asList(getOutputTypeInfos(desc)));
  }

  @Override
  public VectorizationContext getInputVectorizationContext() {
    return vContext;
  }

  public static TypeInfo[] getOutputTypeInfos(MapJoinDesc desc) {

    final byte posBigTable = (byte) desc.getPosBigTable();

    List<ExprNodeDesc> keyDesc = desc.getKeys().get(posBigTable);
    List<ExprNodeDesc> bigTableExprs = desc.getExprs().get(posBigTable);

    Byte[] order = desc.getTagOrder();
    Byte posSingleVectorMapJoinSmallTable = (order[0] == posBigTable ? order[1] : order[0]);

    final int outputColumnCount = desc.getOutputColumnNames().size();
    TypeInfo[] outputTypeInfos = new TypeInfo[outputColumnCount];

    /*
     * Gather up big and small table output result information from the MapJoinDesc.
     */
    List<Integer> bigTableRetainList = desc.getRetainList().get(posBigTable);
    final int bigTableRetainSize = bigTableRetainList.size();

    int[] smallTableIndices;
    int smallTableIndicesSize;
    List<ExprNodeDesc> smallTableExprs = desc.getExprs().get(posSingleVectorMapJoinSmallTable);
    if (desc.getValueIndices() != null && desc.getValueIndices().get(posSingleVectorMapJoinSmallTable) != null) {
      smallTableIndices = desc.getValueIndices().get(posSingleVectorMapJoinSmallTable);
      smallTableIndicesSize = smallTableIndices.length;
    } else {
      smallTableIndices = null;
      smallTableIndicesSize = 0;
    }

    List<Integer> smallTableRetainList = desc.getRetainList().get(posSingleVectorMapJoinSmallTable);
    final int smallTableRetainSize = smallTableRetainList.size();

    int smallTableResultSize = 0;
    if (smallTableIndicesSize > 0) {
      smallTableResultSize = smallTableIndicesSize;
    } else if (smallTableRetainSize > 0) {
      smallTableResultSize = smallTableRetainSize;
    }

    /*
     * Determine the big table retained mapping first so we can optimize out (with
     * projection) copying inner join big table keys in the subsequent small table results section.
     */

    int nextOutputColumn = (order[0] == posBigTable ? 0 : smallTableResultSize);
    for (int i = 0; i < bigTableRetainSize; i++) {

      TypeInfo typeInfo = bigTableExprs.get(i).getTypeInfo();
      outputTypeInfos[nextOutputColumn] = typeInfo;

      nextOutputColumn++;
    }

    /*
     * Now determine the small table results.
     */
    int firstSmallTableOutputColumn;
    firstSmallTableOutputColumn = (order[0] == posBigTable ? bigTableRetainSize : 0);
    int smallTableOutputCount = 0;
    nextOutputColumn = firstSmallTableOutputColumn;

    // Small table indices has more information (i.e. keys) than retain, so use it if it exists...
    if (smallTableIndicesSize > 0) {
      smallTableOutputCount = smallTableIndicesSize;

      for (int i = 0; i < smallTableIndicesSize; i++) {
        if (smallTableIndices[i] >= 0) {

          // Zero and above numbers indicate a big table key is needed for
          // small table result "area".

          int keyIndex = smallTableIndices[i];

          TypeInfo typeInfo = keyDesc.get(keyIndex).getTypeInfo();
          outputTypeInfos[nextOutputColumn] = typeInfo;

        } else {

          // Negative numbers indicate a column to be (deserialize) read from the small table's
          // LazyBinary value row.
          int smallTableValueIndex = -smallTableIndices[i] - 1;

          TypeInfo typeInfo = smallTableExprs.get(smallTableValueIndex).getTypeInfo();
          outputTypeInfos[nextOutputColumn] = typeInfo;

        }
        nextOutputColumn++;
      }
    } else if (smallTableRetainSize > 0) {
      smallTableOutputCount = smallTableRetainSize;

      // Only small table values appear in join output result.

      for (int i = 0; i < smallTableRetainSize; i++) {
        int smallTableValueIndex = smallTableRetainList.get(i);

        TypeInfo typeInfo = smallTableExprs.get(smallTableValueIndex).getTypeInfo();
        outputTypeInfos[nextOutputColumn] = typeInfo;

        nextOutputColumn++;
      }
    }
    return outputTypeInfos;
  }

  @Override
  public void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    vrbCtx = new VectorizedRowBatchCtx();
    vrbCtx.init((StructObjectInspector) this.outputObjInspector,
        vOutContext.getScratchColumnTypeNames(), vOutContext.getScratchDataTypePhysicalVariations());

    outputBatch = vrbCtx.createVectorizedRowBatch();

    outputVectorAssignRowMap = new HashMap<ObjectInspector, VectorAssignRow>();
  }

  /**
   * 'forwards' the (row-mode) record into the (vectorized) output batch
   */
  @Override
  protected void internalForward(Object row, ObjectInspector outputOI) throws HiveException {
    Object[] values = (Object[]) row;
    VectorAssignRow va = outputVectorAssignRowMap.get(outputOI);
    if (va == null) {
      va = new VectorAssignRow();
      va.init((StructObjectInspector) outputOI, vOutContext.getProjectedColumns());
      outputVectorAssignRowMap.put(outputOI, va);
    }

    va.assignRow(outputBatch, outputBatch.size, values);

    ++outputBatch.size;
    if (outputBatch.size == VectorizedRowBatch.DEFAULT_SIZE) {
      flushOutput();
    }
  }

  private void flushOutput() throws HiveException {
    forward(outputBatch, null, true);
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
  public VectorizationContext getOutputVectorizationContext() {
    return vOutContext;
  }

  @Override
  public VectorDesc getVectorDesc() {
    return vectorDesc;
  }
}
