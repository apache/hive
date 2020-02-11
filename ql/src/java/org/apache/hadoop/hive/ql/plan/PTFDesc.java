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

package org.apache.hadoop.hive.ql.plan;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorBase;
import org.apache.hadoop.hive.ql.parse.LeadLagInfo;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;
import org.apache.hadoop.hive.ql.plan.VectorPTFDesc.SupportedFunctionType;
import org.apache.hadoop.hive.ql.plan.ptf.PTFInputDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowTableFunctionDef;
import org.apache.hadoop.hive.ql.udf.ptf.Noop;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Explain(displayName = "PTF Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class PTFDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(PTFDesc.class.getName());

  PartitionedTableFunctionDef funcDef;
  transient LeadLagInfo llInfo;
  /*
   * is this PTFDesc for a Map-Side PTF Operation?
   */
  boolean isMapSide = false;

  transient Configuration cfg;

  public PartitionedTableFunctionDef getFuncDef() {
    return funcDef;
  }

  public void setFuncDef(PartitionedTableFunctionDef funcDef) {
    this.funcDef = funcDef;
  }

  public PartitionedTableFunctionDef getStartOfChain() {
    return funcDef == null ? null : funcDef.getStartOfChain();
  }

  @Explain(displayName = "Function definitions", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<PTFInputDef> getFuncDefExplain() {
    if (funcDef == null) {
      return null;
    }
    List<PTFInputDef> inputs = new ArrayList<PTFInputDef>();
    for (PTFInputDef current = funcDef; current != null; current = current.getInput()) {
      inputs.add(current);
    }
    Collections.reverse(inputs);
    return inputs;
  }

  public LeadLagInfo getLlInfo() {
    return llInfo;
  }

  public void setLlInfo(LeadLagInfo llInfo) {
    this.llInfo = llInfo;
  }

  @Explain(displayName = "Lead/Lag information")
  public String getLlInfoExplain() {
    if (llInfo != null && llInfo.getLeadLagExprs() != null) {
      return PlanUtils.getExprListString(llInfo.getLeadLagExprs());
    }
    return null;
  }

  public boolean forWindowing() {
    return funcDef instanceof WindowTableFunctionDef;
  }

  public boolean forNoop() {
    return funcDef.getTFunction() instanceof Noop;
  }

  @Explain(displayName = "Map-side function", displayOnlyOnTrue = true)
  public boolean isMapSide() {
    return isMapSide;
  }

  public void setMapSide(boolean isMapSide) {
    this.isMapSide = isMapSide;
  }

  public Configuration getCfg() {
    return cfg;
  }

  public void setCfg(Configuration cfg) {
    this.cfg = cfg;
  }

  // Since we don't have a non-native or pass-thru version of VectorPTFOperator, we do not
  // have enableConditionsMet / enableConditionsNotMet like we have for VectorReduceSinkOperator,
  // etc.
  public class PTFOperatorExplainVectorization extends OperatorExplainVectorization {

    private final PTFDesc PTFDesc;
    private final VectorPTFDesc vectorPTFDesc;
    private final VectorPTFInfo vectorPTFInfo;

    private VectorizationCondition[] nativeConditions;

    public PTFOperatorExplainVectorization(PTFDesc PTFDesc, VectorPTFDesc vectorPTFDesc) {
      // VectorPTFOperator is native vectorized.
      super(vectorPTFDesc, true);
      this.PTFDesc = PTFDesc;
      this.vectorPTFDesc = vectorPTFDesc;
      vectorPTFInfo = vectorPTFDesc.getVectorPTFInfo();
    }

    @Explain(vectorization = Vectorization.EXPRESSION, displayName = "functionNames", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getFunctionNames() {
      return Arrays.toString(vectorPTFDesc.getEvaluatorFunctionNames());
    }

    @Explain(vectorization = Vectorization.EXPRESSION, displayName = "functionInputExpressions", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getFunctionInputExpressions() {
      return Arrays.toString(vectorPTFInfo.getEvaluatorInputExpressions());
    }

    @Explain(vectorization = Vectorization.EXPRESSION, displayName = "partitionExpressions", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getPartitionExpressions() {
      VectorExpression[] partitionExpressions = vectorPTFInfo.getPartitionExpressions();
      if (partitionExpressions == null) {
        return null;
      }
      return Arrays.toString(partitionExpressions);
    }

    @Explain(vectorization = Vectorization.EXPRESSION, displayName = "orderExpressions", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getOrderExpressions() {
      VectorExpression[] orderExpressions = vectorPTFInfo.getOrderExpressions();
      if (orderExpressions == null) {
        return null;
      }
      return Arrays.toString(orderExpressions);
    }

    @Explain(vectorization = Vectorization.EXPRESSION, displayName = "evaluatorClasses", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getEvaluatorClasses() {

      VectorPTFEvaluatorBase[] evaluators = VectorPTFDesc.getEvaluators(vectorPTFDesc, vectorPTFInfo);

      ArrayList<String> result = new ArrayList<String>(evaluators.length);
      for (VectorPTFEvaluatorBase evaluator : evaluators) {
        result.add(evaluator.getClass().getSimpleName());
      }
      return result.toString();
    }

    @Explain(vectorization = Vectorization.DETAIL, displayName = "outputColumns", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getOutputColumns() {
      return Arrays.toString(vectorPTFInfo.getOutputColumnMap());
    }

    @Explain(vectorization = Vectorization.DETAIL, displayName = "outputTypes", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getOutputTypes() {
      return Arrays.toString(vectorPTFDesc.getOutputTypeInfos());
    }

    @Explain(vectorization = Vectorization.DETAIL, displayName = "keyInputColumns", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getKeyInputColumns() {
      return Arrays.toString(vectorPTFInfo.getKeyInputColumnMap());
    }

    @Explain(vectorization = Vectorization.DETAIL, displayName = "nonKeyInputColumns", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getNonKeyInputColumns() {
      return Arrays.toString(vectorPTFInfo.getNonKeyInputColumnMap());
    }

    @Explain(vectorization = Vectorization.DETAIL, displayName = "streamingColumns", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getStreamingColumns() {
      VectorPTFEvaluatorBase[] evaluators = VectorPTFDesc.getEvaluators(vectorPTFDesc, vectorPTFInfo);
      ArrayList<Integer> result = new ArrayList<Integer>();
      for (VectorPTFEvaluatorBase evaluator : evaluators) {
        if (evaluator.streamsResult()) {
          result.add(evaluator.getOutputColumnNum());
        }
      }
      return result.toString();
    }
  }

  @Explain(vectorization = Vectorization.OPERATOR, displayName = "PTF Vectorization", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public PTFOperatorExplainVectorization getPTFVectorization() {
    VectorPTFDesc vectorPTFDesc = (VectorPTFDesc) getVectorDesc();
    if (vectorPTFDesc == null) {
      return null;
    }
    return new PTFOperatorExplainVectorization(this, vectorPTFDesc);
  }
}
