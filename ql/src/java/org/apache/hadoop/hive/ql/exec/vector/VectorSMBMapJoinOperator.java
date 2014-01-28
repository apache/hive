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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinKey;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * VectorSMBJoinOperator.
 * Implements the vectorized SMB join operator. The implementation relies on the row-mode SMB join operator.
 * It accepts a vectorized batch input from the big table and iterates over the batch, calling the parent row-mode
 * implementation for each row in the batch.
 */
public class VectorSMBMapJoinOperator extends SMBMapJoinOperator implements VectorizationContextRegion {

  private static final Log LOG = LogFactory.getLog(
      VectorSMBMapJoinOperator.class.getName());  
  
  private static final long serialVersionUID = 1L;

  private int tagLen;
  
  private transient VectorizedRowBatch outputBatch;  
  private transient VectorizationContext vOutContext = null;
  private transient VectorizedRowBatchCtx vrbCtx = null;  
  
  private String fileKey;

  private VectorExpression[] bigTableValueExpressions;

  private VectorExpression[] bigTableFilterExpressions;

  private VectorExpression[] keyExpressions;

  private VectorExpressionWriter[] keyOutputWriters;

  private transient VectorHashKeyWrapperBatch keyWrapperBatch;

  private transient Map<ObjectInspector, VectorColumnAssign[]> outputVectorAssigners;

  private transient int batchIndex = -1;

  private transient VectorHashKeyWrapper[] keyValues;

  private transient SMBJoinKeyEvaluator keyEvaluator;
  
  private transient VectorExpressionWriter[] valueWriters;
  
  private interface SMBJoinKeyEvaluator {
    List<Object> evaluate(VectorHashKeyWrapper kw) throws HiveException;
}  

  public VectorSMBMapJoinOperator() {
    super();
  }
  
  public VectorSMBMapJoinOperator(VectorizationContext vContext, OperatorDesc conf)
      throws HiveException {
    this();
    SMBJoinDesc desc = (SMBJoinDesc) conf;
    this.conf = desc;
    
    order = desc.getTagOrder();
    numAliases = desc.getExprs().size();
    posBigTable = (byte) desc.getPosBigTable();
    filterMaps = desc.getFilterMap();
    tagLen = desc.getTagLength();
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
    
    // Vectorized join operators need to create a new vectorization region for child operators.

    List<String> outColNames = desc.getOutputColumnNames();
    
    Map<String, Integer> mapOutCols = new HashMap<String, Integer>(outColNames.size());
    
    int outColIndex = 0;
    for(String outCol: outColNames) {
      mapOutCols.put(outCol,  outColIndex++);
    }

    vOutContext = new VectorizationContext(mapOutCols, outColIndex);
    vOutContext.setFileKey(vContext.getFileKey() + "/SMB_JOIN_" + desc.getBigTableAlias());
    this.fileKey = vOutContext.getFileKey();
  }
  
  @Override
  protected List<Object> smbJoinComputeKeys(Object row, byte alias) throws HiveException {
    if (alias == this.posBigTable) {
      VectorizedRowBatch inBatch = (VectorizedRowBatch) row;
      return keyEvaluator.evaluate(keyValues[batchIndex]);
    } else {
      return super.smbJoinComputeKeys(row, alias);
    }
  }  
  
  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    vrbCtx = new VectorizedRowBatchCtx();
    vrbCtx.init(hconf, this.fileKey, (StructObjectInspector) this.outputObjInspector);
    
    outputBatch = vrbCtx.createVectorizedRowBatch();
    
    keyWrapperBatch = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(keyExpressions);
    
    outputVectorAssigners = new HashMap<ObjectInspector, VectorColumnAssign[]>();
    
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
      public List<Object> evaluate(VectorHashKeyWrapper kw) throws HiveException {
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
    
  }
  
  @Override
  public void processOp(Object row, int tag) throws HiveException {
    byte alias = (byte) tag;
    
    if (alias != this.posBigTable) {
      super.processOp(row, tag);
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
  
      keyWrapperBatch.evaluateBatch(inBatch);
      keyValues = keyWrapperBatch.getVectorHashKeyWrappers();
  
      // This implementation of vectorized JOIN is delegating all the work
      // to the row-mode implementation by hijacking the big table node evaluators
      // and calling the row-mode join processOp for each row in the input batch.
      // Since the JOIN operator is not fully vectorized anyway at the moment 
      // (due to the use of row-mode small-tables) this is a reasonable trade-off.
      //
      for(batchIndex=0; batchIndex < inBatch.size; ++batchIndex ) {
        super.processOp(row, tag);
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
    VectorColumnAssign[] vcas = outputVectorAssigners.get(outputOI);
    if (null == vcas) {
      Map<String, Map<String, Integer>> allColumnMaps = Utilities.
          getMapRedWork(hconf).getMapWork().getScratchColumnMap();
      Map<String, Integer> columnMap = allColumnMaps.get(fileKey);
      vcas = VectorColumnAssignFactory.buildAssigners(
          outputBatch, outputOI, columnMap, conf.getOutputColumnNames());
      outputVectorAssigners.put(outputOI, vcas);
    }
    for (int i = 0; i < values.length; ++i) {
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
  public VectorizationContext getOuputVectorizationContext() {
    return vOutContext;
  }
}
