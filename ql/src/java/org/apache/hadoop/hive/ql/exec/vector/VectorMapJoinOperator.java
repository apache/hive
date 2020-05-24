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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer.ReusableGetAdaptor;
import org.apache.hadoop.hive.ql.exec.persistence.MatchTracker;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBase;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * The vectorized version of the MapJoinOperator.
 */
public class VectorMapJoinOperator extends VectorMapJoinBaseOperator {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(
      VectorMapJoinOperator.class.getName());

  protected VectorExpression[] keyExpressions;

  protected VectorExpression[] bigTableFilterExpressions;
  protected VectorExpression[] bigTableValueExpressions;

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------


  private transient VectorExpressionWriter[] valueWriters;

  // These members are used as out-of-band params
  // for the inner-loop supper.processOp callbacks
  //
  private transient int batchIndex;
  private transient VectorHashKeyWrapperBase[] keyValues;
  private transient VectorHashKeyWrapperBatch keyWrapperBatch;
  private transient VectorExpressionWriter[] keyOutputWriters;

  private VectorExpressionWriter[] rowWriters;  // Writer for producing row from input batch
  protected transient Object[] singleRow;

  /** Kryo ctor. */
  @VisibleForTesting
  public VectorMapJoinOperator() {
    super();
  }

  public VectorMapJoinOperator(CompilationOpContext ctx) {
    super(ctx);
  }


  public VectorMapJoinOperator (CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) throws HiveException {

    super(ctx, conf, vContext, vectorDesc);

    MapJoinDesc desc = (MapJoinDesc) conf;

    Map<Byte, List<ExprNodeDesc>> filterExpressions = desc.getFilters();
    bigTableFilterExpressions = vContext.getVectorExpressions(filterExpressions.get(posBigTable),
        VectorExpressionDescriptor.Mode.FILTER);

    keyExpressions = this.vectorDesc.getAllBigTableKeyExpressions();

    bigTableValueExpressions = this.vectorDesc.getAllBigTableValueExpressions();
  }

  @Override
  public void initializeOp(Configuration hconf) throws HiveException {
    VectorExpression.doTransientInit(bigTableFilterExpressions, hconf);
    VectorExpression.doTransientInit(keyExpressions, hconf);
    VectorExpression.doTransientInit(bigTableValueExpressions, hconf);

    // Use a final variable to properly parameterize the processVectorInspector closure.
    // Using a member variable in the closure will not do the right thing...
    final int parameterizePosBigTable = conf.getPosBigTable();

    // Code borrowed from VectorReduceSinkOperator.initializeOp
    VectorExpressionWriterFactory.processVectorInspector(
        (StructObjectInspector) inputObjInspectors[parameterizePosBigTable],
        new VectorExpressionWriterFactory.SingleOIDClosure() {
          @Override
          public void assign(VectorExpressionWriter[] writers,
                             ObjectInspector objectInspector) {
            rowWriters = writers;
            inputObjInspectors[parameterizePosBigTable] = objectInspector;
          }
        });
    singleRow = new Object[rowWriters.length];

    super.initializeOp(hconf);

    List<ExprNodeDesc> keyDesc = conf.getKeys().get(posBigTable);
    keyOutputWriters = VectorExpressionWriterFactory.getExpressionWriters(keyDesc);

    keyWrapperBatch = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(keyExpressions);

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
      ExprNodeEvaluator eval = new ExprNodeEvaluator<ExprNodeDesc>(desc, hconf) {
        int columnIndex;
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
      }.initVectorExpr(vectorExpr.getOutputColumnNum(), i);
      vectorNodeEvaluators.add(eval);
    }
    // Now replace the old evaluators with our own
    joinValues[posBigTable] = vectorNodeEvaluators;
  }

  @Override
  protected JoinUtil.JoinResult setMapJoinKey(ReusableGetAdaptor dest, Object row, byte alias)
      throws HiveException {
    return dest.setFromVector(keyValues[batchIndex], keyOutputWriters, keyWrapperBatch);
  }

  /*
   * This variation is for FULL OUTER MapJoin.  It does key match tracking only if the key has
   * no NULLs.
   */
  @Override
  protected JoinUtil.JoinResult setMapJoinKeyNoNulls(ReusableGetAdaptor dest, Object row, byte alias,
      MatchTracker matchTracker)
      throws HiveException {
    return dest.setFromVectorNoNulls(keyValues[batchIndex], keyOutputWriters, keyWrapperBatch,
        matchTracker);
  }

  @Override
  public void process(Object row, int tag) throws HiveException {

    VectorizedRowBatch inBatch = (VectorizedRowBatch) row;

    // Preparation for hybrid grace hash join
    this.tag = tag;
    if (scratchBatch == null) {
      scratchBatch = VectorizedBatchUtil.makeLike(inBatch);
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

    for (VectorExpression ve : keyExpressions) {
      ve.evaluate(inBatch);
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
  public void closeOp(boolean aborted) throws HiveException {
    super.closeOp(aborted);
  }

  @Override
  protected void spillBigTableRow(MapJoinTableContainer hybridHtContainer, Object row)
      throws HiveException {
    // Extract the actual row from row batch
    VectorizedRowBatch inBatch = (VectorizedRowBatch) row;
    Object[] actualRow = getRowObject(inBatch, batchIndex);
    super.spillBigTableRow(hybridHtContainer, actualRow);
  }

  // Only used by spillBigTableRow?
  // Code borrowed from VectorReduceSinkOperator
  private Object[] getRowObject(VectorizedRowBatch vrb, int rowIndex) throws HiveException {
    int batchIndex = rowIndex;
    if (vrb.selectedInUse) {
      batchIndex = vrb.selected[rowIndex];
    }
    for (int i = 0; i < singleRow.length; i++) {
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
}
