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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBase;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorSMBJoinDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * VectorSMBJoinOperator.
 * Implements the vectorized SMB join operator. The implementation relies on the row-mode SMB join operator.
 * It accepts a vectorized batch input from the big table and iterates over the batch, calling the parent row-mode
 * implementation for each row in the batch.
 */
public class VectorSMBMapJoinOperator extends SMBMapJoinOperator
    implements VectorizationOperator, VectorizationContextRegion {

  private static final Logger LOG = LoggerFactory.getLogger(
      VectorSMBMapJoinOperator.class.getName());

  private static final long serialVersionUID = 1L;

  private VectorizationContext vContext;
  private VectorSMBJoinDesc vectorDesc;

  private VectorExpression[] bigTableValueExpressions;

  private VectorExpression[] bigTableFilterExpressions;

  private VectorExpression[] keyExpressions;

  private VectorExpressionWriter[] keyOutputWriters;

  private VectorizationContext vOutContext;

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  private transient VectorizedRowBatch outputBatch;

  private transient VectorizedRowBatchCtx vrbCtx = null;

  private transient VectorHashKeyWrapperBatch keyWrapperBatch;

  private transient Map<ObjectInspector, VectorAssignRow> outputVectorAssignRowMap;

  private transient int batchIndex = -1;

  private transient VectorHashKeyWrapperBase[] keyValues;

  private transient SMBJoinKeyEvaluator keyEvaluator;

  private transient VectorExpressionWriter[] valueWriters;

  private interface SMBJoinKeyEvaluator {
    List<Object> evaluate(VectorHashKeyWrapperBase kw) throws HiveException;
}

  /** Kryo ctor. */
  @VisibleForTesting
  public VectorSMBMapJoinOperator() {
    super();
  }

  public VectorSMBMapJoinOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorSMBMapJoinOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) throws HiveException {
    this(ctx);
    SMBJoinDesc desc = (SMBJoinDesc) conf;
    this.conf = desc;
    this.vContext = vContext;
    this.vectorDesc = (VectorSMBJoinDesc) vectorDesc;

    order = desc.getTagOrder();
    numAliases = desc.getExprs().size();
    posBigTable = (byte) desc.getPosBigTable();
    filterMaps = desc.getFilterMap();
    noOuterJoin = desc.isNoOuterJoin();

    // Must obtain vectorized equivalents for filter and value expressions

    Map<Byte, List<ExprNodeDesc>> filterExpressions = desc.getFilters();
    bigTableFilterExpressions = vContext.getVectorExpressions(filterExpressions.get(posBigTable),
        VectorExpressionDescriptor.Mode.FILTER);

    List<ExprNodeDesc> keyDesc = desc.getKeys().get(posBigTable);
    keyExpressions = vContext.getVectorExpressions(keyDesc);
    keyOutputWriters = VectorExpressionWriterFactory.getExpressionWriters(keyDesc);

    Map<Byte, List<ExprNodeDesc>> exprs = desc.getExprs();
    bigTableValueExpressions = vContext.getVectorExpressions(exprs.get(posBigTable));

    // We are making a new output vectorized row batch.
    vOutContext = new VectorizationContext(getName(), desc.getOutputColumnNames(),
        /* vContextEnvironment */ vContext);
  }

  @Override
  public VectorizationContext getInputVectorizationContext() {
    return vContext;
  }

  @Override
  protected List<Object> smbJoinComputeKeys(Object row, byte alias) throws HiveException {
    if (alias == this.posBigTable) {

      // The keyEvaluate reuses storage.  That doesn't work with SMB MapJoin because it
      // holds references to keys as it is merging.
      List<Object> singletonListAndObjects = keyEvaluator.evaluate(keyValues[batchIndex]);
      ArrayList<Object> result = new ArrayList<Object>(singletonListAndObjects.size());
      for (int i = 0; i < singletonListAndObjects.size(); i++) {
        result.add(ObjectInspectorUtils.copyToStandardObject(singletonListAndObjects.get(i),
            joinKeysObjectInspectors[alias].get(i),
            ObjectInspectorCopyOption.WRITABLE));
      }
      return result;
    } else {
      return super.smbJoinComputeKeys(row, alias);
    }
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    VectorExpression.doTransientInit(bigTableFilterExpressions);
    VectorExpression.doTransientInit(keyExpressions);
    VectorExpression.doTransientInit(bigTableValueExpressions);

    vrbCtx = new VectorizedRowBatchCtx();
    vrbCtx.init((StructObjectInspector) this.outputObjInspector, vOutContext.getScratchColumnTypeNames());

    outputBatch = vrbCtx.createVectorizedRowBatch();

    keyWrapperBatch = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(keyExpressions);

    outputVectorAssignRowMap = new HashMap<ObjectInspector, VectorAssignRow>();

    // This key evaluator translates from the vectorized VectorHashKeyWrapper format
    // into the row-mode MapJoinKey
    keyEvaluator = new SMBJoinKeyEvaluator() {
      private List<Object> key;

      public SMBJoinKeyEvaluator init() {
        key = new ArrayList<Object>();
        for(int i = 0; i < keyExpressions.length; ++i) {
          key.add(null);
        }
        return this;
      }

      @Override
      public List<Object> evaluate(VectorHashKeyWrapperBase kw) throws HiveException {
        for(int i = 0; i < keyExpressions.length; ++i) {
          key.set(i, keyWrapperBatch.getWritableKeyValue(kw, i, keyOutputWriters[i]));
        }
        return key;
      };
    }.init();

    Map<Byte, List<ExprNodeDesc>> valueExpressions = conf.getExprs();
    List<ExprNodeDesc> bigTableExpressions = valueExpressions.get(posBigTable);

    // We're hijacking the big table evaluators and replacing them with our own custom ones
    // which are going to return values from the input batch vector expressions
    List<ExprNodeEvaluator> vectorNodeEvaluators = new ArrayList<ExprNodeEvaluator>(bigTableExpressions.size());

    VectorExpressionWriterFactory.processVectorExpressions(
        bigTableExpressions,
        new VectorExpressionWriterFactory.ListOIDClosure() {

          @Override
          public void assign(VectorExpressionWriter[] writers, List<ObjectInspector> oids) {
            valueWriters = writers;
            joinValuesObjectInspectors[posBigTable] = oids;
          }
        });

    for(int i=0; i<bigTableExpressions.size(); ++i) {
      ExprNodeDesc desc = bigTableExpressions.get(i);
      VectorExpression vectorExpr = bigTableValueExpressions[i];

      // This is a vectorized aware evaluator
      ExprNodeEvaluator eval = new ExprNodeEvaluator<ExprNodeDesc>(desc, hconf) {
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
      }.initVectorExpr(vectorExpr.getOutputColumnNum(), i);
      vectorNodeEvaluators.add(eval);
    }
    // Now replace the old evaluators with our own
    joinValues[posBigTable] = vectorNodeEvaluators;
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    byte alias = (byte) tag;

    if (alias != this.posBigTable) {
      super.process(row, tag);
    } else {

      VectorizedRowBatch inBatch = (VectorizedRowBatch) row;

      if (null != bigTableFilterExpressions) {
        for(VectorExpression ve : bigTableFilterExpressions) {
          ve.evaluate(inBatch);
        }
      }

      if (null != bigTableValueExpressions) {
        for(VectorExpression ve : bigTableValueExpressions) {
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
      // Since the JOIN operator is not fully vectorized anyway at the moment
      // (due to the use of row-mode small-tables) this is a reasonable trade-off.
      //
      for(batchIndex=0; batchIndex < inBatch.size; ++batchIndex ) {
        super.process(row, tag);
      }

      // Set these two to invalid values so any attempt to use them
      // outside the inner loop results in NPE/OutOfBounds errors
      batchIndex = -1;
      keyValues = null;
    }
  }

  @Override
  public void closeOp(boolean aborted) throws HiveException {
    super.closeOp(aborted);
    if (!aborted && 0 < outputBatch.size) {
      flushOutput();
    }
  }

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
  public VectorizationContext getOutputVectorizationContext() {
    return vOutContext;
  }

  @Override
  public VectorDesc getVectorDesc() {
    return vectorDesc;
  }
}
