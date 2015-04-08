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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.persistence.HybridHashTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer.ReusableGetAdaptor;
import org.apache.hadoop.hive.ql.exec.persistence.ObjectContainer;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.DataOutputBuffer;

/**
 * The vectorized version of the MapJoinOperator.
 */
public class VectorMapJoinOperator extends MapJoinOperator implements VectorizationContextRegion {

  private static final Log LOG = LogFactory.getLog(
      VectorMapJoinOperator.class.getName());

   /**
   *
   */
  private static final long serialVersionUID = 1L;

  private VectorExpression[] keyExpressions;

  private VectorExpression[] bigTableFilterExpressions;
  private VectorExpression[] bigTableValueExpressions;

  private VectorizationContext vOutContext;

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  private transient VectorizedRowBatch outputBatch;
  private transient VectorizedRowBatch scratchBatch;  // holds restored (from disk) big table rows
  private transient VectorExpressionWriter[] valueWriters;
  private transient Map<ObjectInspector, VectorColumnAssign[]> outputVectorAssigners;

  // These members are used as out-of-band params
  // for the inner-loop supper.processOp callbacks
  //
  private transient int batchIndex;
  private transient VectorHashKeyWrapper[] keyValues;
  private transient VectorHashKeyWrapperBatch keyWrapperBatch;
  private transient VectorExpressionWriter[] keyOutputWriters;

  private transient VectorizedRowBatchCtx vrbCtx = null;

  private transient int tag;  // big table alias
  private VectorExpressionWriter[] rowWriters;  // Writer for producing row from input batch
  protected transient Object[] singleRow;

  public VectorMapJoinOperator() {
    super();
  }


  public VectorMapJoinOperator (VectorizationContext vContext, OperatorDesc conf)
    throws HiveException {
    this();

    MapJoinDesc desc = (MapJoinDesc) conf;
    this.conf = desc;

    order = desc.getTagOrder();
    numAliases = desc.getExprs().size();
    posBigTable = (byte) desc.getPosBigTable();
    filterMaps = desc.getFilterMap();
    noOuterJoin = desc.isNoOuterJoin();

    Map<Byte, List<ExprNodeDesc>> filterExpressions = desc.getFilters();
    bigTableFilterExpressions = vContext.getVectorExpressions(filterExpressions.get(posBigTable),
        VectorExpressionDescriptor.Mode.FILTER);

    List<ExprNodeDesc> keyDesc = desc.getKeys().get(posBigTable);
    keyExpressions = vContext.getVectorExpressions(keyDesc);

    // We're only going to evaluate the big table vectorized expressions,
    Map<Byte, List<ExprNodeDesc>> exprs = desc.getExprs();
    bigTableValueExpressions = vContext.getVectorExpressions(exprs.get(posBigTable));

    // We are making a new output vectorized row batch.
    vOutContext = new VectorizationContext(desc.getOutputColumnNames());
    vOutContext.setFileKey(vContext.getFileKey() + "/MAP_JOIN_" + desc.getBigTableAlias());
  }

  @Override
  public Collection<Future<?>> initializeOp(Configuration hconf) throws HiveException {
    // Code borrowed from VectorReduceSinkOperator.initializeOp
    VectorExpressionWriterFactory.processVectorInspector(
        (StructObjectInspector) inputObjInspectors[0],
        new VectorExpressionWriterFactory.SingleOIDClosure() {
          @Override
          public void assign(VectorExpressionWriter[] writers,
                             ObjectInspector objectInspector) {
            rowWriters = writers;
            inputObjInspectors[0] = objectInspector;
          }
        });
    singleRow = new Object[rowWriters.length];

    Collection<Future<?>> result = super.initializeOp(hconf);

    List<ExprNodeDesc> keyDesc = conf.getKeys().get(posBigTable);
    keyOutputWriters = VectorExpressionWriterFactory.getExpressionWriters(keyDesc);

    vrbCtx = new VectorizedRowBatchCtx();
    vrbCtx.init(vOutContext.getScratchColumnTypeMap(), (StructObjectInspector) this.outputObjInspector);

    outputBatch = vrbCtx.createVectorizedRowBatch();

    keyWrapperBatch =VectorHashKeyWrapperBatch.compileKeyWrapperBatch(keyExpressions);

    Map<Byte, List<ExprNodeDesc>> valueExpressions = conf.getExprs();
    List<ExprNodeDesc> bigTableExpressions = valueExpressions.get(posBigTable);

    VectorExpressionWriterFactory.processVectorExpressions(
        bigTableExpressions,
        new VectorExpressionWriterFactory.ListOIDClosure() {
          @Override
          public void assign(VectorExpressionWriter[] writers, List<ObjectInspector> oids) {
            valueWriters = writers;
            joinValuesObjectInspectors[posBigTable] = oids;
          }
        });

    // We're hijacking the big table evaluators an replace them with our own custom ones
    // which are going to return values from the input batch vector expressions
    List<ExprNodeEvaluator> vectorNodeEvaluators = new ArrayList<ExprNodeEvaluator>(bigTableExpressions.size());

    for(int i=0; i<bigTableExpressions.size(); ++i) {
      ExprNodeDesc desc = bigTableExpressions.get(i);
      VectorExpression vectorExpr = bigTableValueExpressions[i];

      // This is a vectorized aware evaluator
      ExprNodeEvaluator eval = new ExprNodeEvaluator<ExprNodeDesc>(desc) {
        int columnIndex;;
        int writerIndex;

        public ExprNodeEvaluator initVectorExpr(int columnIndex, int writerIndex) {
          this.columnIndex = columnIndex;
          this.writerIndex = writerIndex;
          return this;
        }

        @Override
        public ObjectInspector initialize(ObjectInspector rowInspector) throws HiveException {
          throw new HiveException("should never reach here");
        }

        @Override
        protected Object _evaluate(Object row, int version) throws HiveException {
          VectorizedRowBatch inBatch = (VectorizedRowBatch) row;
          int rowIndex = inBatch.selectedInUse ? inBatch.selected[batchIndex] : batchIndex;
          return valueWriters[writerIndex].writeValue(inBatch.cols[columnIndex], rowIndex);
        }
      }.initVectorExpr(vectorExpr.getOutputColumn(), i);
      vectorNodeEvaluators.add(eval);
    }
    // Now replace the old evaluators with our own
    joinValues[posBigTable] = vectorNodeEvaluators;

    // Filtering is handled in the input batch processing
    filterMaps[posBigTable] = null;

    outputVectorAssigners = new HashMap<ObjectInspector, VectorColumnAssign[]>();
    return result;
  }

  /**
   * 'forwards' the (row-mode) record into the (vectorized) output batch
   */
  @Override
  protected void internalForward(Object row, ObjectInspector outputOI) throws HiveException {
    Object[] values = (Object[]) row;
    VectorColumnAssign[] vcas = outputVectorAssigners.get(outputOI);
    if (null == vcas) {
      vcas = VectorColumnAssignFactory.buildAssigners(
          outputBatch, outputOI, vOutContext.getProjectionColumnMap(), conf.getOutputColumnNames());
      outputVectorAssigners.put(outputOI, vcas);
    }
    for (int i=0; i<values.length; ++i) {
      vcas[i].assignObjectValue(values[i], outputBatch.size);
    }
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
    if (!aborted && 0 < outputBatch.size) {
      flushOutput();
    }
  }

  @Override
  protected JoinUtil.JoinResult setMapJoinKey(ReusableGetAdaptor dest, Object row, byte alias)
      throws HiveException {
    return dest.setFromVector(keyValues[batchIndex], keyOutputWriters, keyWrapperBatch);
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    byte alias = (byte) tag;
    VectorizedRowBatch inBatch = (VectorizedRowBatch) row;

    // Preparation for hybrid grace hash join
    this.tag = tag;
    if (scratchBatch == null) {
      scratchBatch = makeLike(inBatch);
    }

    if (null != bigTableFilterExpressions) {
      for(VectorExpression ve:bigTableFilterExpressions) {
        ve.evaluate(inBatch);
      }
    }

    if (null != bigTableValueExpressions) {
      for(VectorExpression ve: bigTableValueExpressions) {
        ve.evaluate(inBatch);
      }
    }

    keyWrapperBatch.evaluateBatch(inBatch);
    keyValues = keyWrapperBatch.getVectorHashKeyWrappers();

    // This implementation of vectorized JOIN is delegating all the work
    // to the row-mode implementation by hijacking the big table node evaluators
    // and calling the row-mode join processOp for each row in the input batch.
    // Since the JOIN operator is not fully vectorized anyway atm (due to the use
    // of row-mode small-tables) this is a reasonable trade-off.
    //
    for(batchIndex=0; batchIndex < inBatch.size; ++batchIndex) {
      super.process(row, tag);
    }

    // Set these two to invalid values so any attempt to use them
    // outside the inner loop results in NPE/OutOfBounds errors
    batchIndex = -1;
    keyValues = null;
  }

  @Override
  public VectorizationContext getOuputVectorizationContext() {
    return vOutContext;
  }

  @Override
  protected void spillBigTableRow(MapJoinTableContainer hybridHtContainer, Object row)
      throws HiveException {
    // Extract the actual row from row batch
    VectorizedRowBatch inBatch = (VectorizedRowBatch) row;
    Object[] actualRow = getRowObject(inBatch, batchIndex);
    super.spillBigTableRow(hybridHtContainer, actualRow);
  }

  @Override
  protected void reProcessBigTable(HybridHashTableContainer.HashPartition partition)
      throws HiveException {
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

  // Code borrowed from VectorReduceSinkOperator
  private Object[] getRowObject(VectorizedRowBatch vrb, int rowIndex) throws HiveException {
    int batchIndex = rowIndex;
    if (vrb.selectedInUse) {
      batchIndex = vrb.selected[rowIndex];
    }
    for (int i = 0; i < vrb.projectionSize; i++) {
      ColumnVector vectorColumn = vrb.cols[vrb.projectedColumns[i]];
      if (vectorColumn != null) {
        singleRow[i] = rowWriters[i].writeValue(vectorColumn, batchIndex);
      } else {
        // Some columns from tables are not used.
        singleRow[i] = null;
      }
    }
    return singleRow;
  }

  /**
   * Make a new (scratch) batch, which is exactly "like" the batch provided, except that it's empty
   * @param batch the batch to imitate
   * @return the new batch
   * @throws HiveException
   */
  VectorizedRowBatch makeLike(VectorizedRowBatch batch) throws HiveException {
    VectorizedRowBatch newBatch = new VectorizedRowBatch(batch.numCols);
    for (int i = 0; i < batch.numCols; i++) {
      ColumnVector colVector = batch.cols[i];
      if (colVector != null) {
        ColumnVector newColVector;
        if (colVector instanceof LongColumnVector) {
          newColVector = new LongColumnVector();
        } else if (colVector instanceof DoubleColumnVector) {
          newColVector = new DoubleColumnVector();
        } else if (colVector instanceof BytesColumnVector) {
          newColVector = new BytesColumnVector();
        } else if (colVector instanceof DecimalColumnVector) {
          DecimalColumnVector decColVector = (DecimalColumnVector) colVector;
          newColVector = new DecimalColumnVector(decColVector.precision, decColVector.scale);
        } else {
          throw new HiveException("Column vector class " + colVector.getClass().getName() +
          " is not supported!");
        }
        newBatch.cols[i] = newColVector;
        newBatch.cols[i].init();
      }
    }
    newBatch.projectedColumns = Arrays.copyOf(batch.projectedColumns, batch.projectedColumns.length);
    newBatch.projectionSize = batch.projectionSize;
    newBatch.reset();
    return newBatch;
  }
}
