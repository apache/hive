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
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorBase;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorCount;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorCountStar;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorDecimalAvg;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorDecimalFirstValue;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorDecimalLastValue;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorDecimalMax;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorDecimalMin;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorDecimalSum;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorDenseRank;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorDoubleAvg;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorDoubleFirstValue;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorDoubleLastValue;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorDoubleMax;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorDoubleMin;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorDoubleSum;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorLongAvg;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorLongFirstValue;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorLongLastValue;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorLongMax;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorLongMin;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorLongSum;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorRank;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorRowNumber;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorStreamingDecimalAvg;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorStreamingDecimalMax;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorStreamingDecimalMin;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorStreamingDecimalSum;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorStreamingDoubleAvg;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorStreamingDoubleMax;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorStreamingDoubleMin;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorStreamingDoubleSum;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorStreamingLongAvg;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorStreamingLongMax;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorStreamingLongMin;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorStreamingLongSum;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowType;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * VectorPTFDesc.
 *
 * Extra parameters beyond PTFDesc just for the VectorPTFOperator.
 *
 * We don't extend PTFDesc because the base OperatorDesc doesn't support
 * clone and adding it is a lot work for little gain.
 */
public class VectorPTFDesc extends AbstractVectorDesc  {

  private static final long serialVersionUID = 1L;

  public static enum SupportedFunctionType {
    ROW_NUMBER,
    RANK,
    DENSE_RANK,
    MIN,
    MAX,
    SUM,
    AVG,
    FIRST_VALUE,
    LAST_VALUE,
    COUNT
  }

  public static HashMap<String, SupportedFunctionType> supportedFunctionsMap =
      new HashMap<String, SupportedFunctionType>();
  static {
    supportedFunctionsMap.put("row_number", SupportedFunctionType.ROW_NUMBER);
    supportedFunctionsMap.put("rank", SupportedFunctionType.RANK);
    supportedFunctionsMap.put("dense_rank", SupportedFunctionType.DENSE_RANK);
    supportedFunctionsMap.put("min", SupportedFunctionType.MIN);
    supportedFunctionsMap.put("max", SupportedFunctionType.MAX);
    supportedFunctionsMap.put("sum", SupportedFunctionType.SUM);
    supportedFunctionsMap.put("avg", SupportedFunctionType.AVG);
    supportedFunctionsMap.put("first_value", SupportedFunctionType.FIRST_VALUE);
    supportedFunctionsMap.put("last_value", SupportedFunctionType.LAST_VALUE);
    supportedFunctionsMap.put("count", SupportedFunctionType.COUNT);
  }
  public static List<String> supportedFunctionNames = new ArrayList<String>();
  static {
    TreeSet<String> treeSet = new TreeSet<String>();
    treeSet.addAll(supportedFunctionsMap.keySet());
    supportedFunctionNames.addAll(treeSet);
  }

  private TypeInfo[] reducerBatchTypeInfos;

  private boolean isPartitionOrderBy;

  private String[] evaluatorFunctionNames;
  private WindowFrameDef[] evaluatorWindowFrameDefs;
  private List<ExprNodeDesc>[] evaluatorInputExprNodeDescLists;

  private ExprNodeDesc[] orderExprNodeDescs;
  private ExprNodeDesc[] partitionExprNodeDescs;

  private String[] outputColumnNames;
  private TypeInfo[] outputTypeInfos;

  private VectorPTFInfo vectorPTFInfo;

  private int vectorizedPTFMaxMemoryBufferingBatchCount;

  public VectorPTFDesc() {
    isPartitionOrderBy = false;

    evaluatorFunctionNames = null;
    evaluatorInputExprNodeDescLists = null;

    orderExprNodeDescs = null;
    partitionExprNodeDescs = null;

    outputColumnNames = null;
    outputTypeInfos = null;

    vectorizedPTFMaxMemoryBufferingBatchCount = -1;

  }

  // We provide this public method to help EXPLAIN VECTORIZATION show the evaluator classes.
  public static VectorPTFEvaluatorBase getEvaluator(SupportedFunctionType functionType,
      WindowFrameDef windowFrameDef, Type columnVectorType, VectorExpression inputVectorExpression,
      int outputColumnNum) {

    final boolean isRowEndCurrent =
        (windowFrameDef.getWindowType() == WindowType.ROWS &&
         windowFrameDef.getEnd().isCurrentRow());

    VectorPTFEvaluatorBase evaluator;
    switch (functionType) {
    case ROW_NUMBER:
      evaluator =
          new VectorPTFEvaluatorRowNumber(windowFrameDef, inputVectorExpression, outputColumnNum);
      break;
    case RANK:
      evaluator =
          new VectorPTFEvaluatorRank(windowFrameDef, inputVectorExpression, outputColumnNum);
      break;
    case DENSE_RANK:
      evaluator =
          new VectorPTFEvaluatorDenseRank(windowFrameDef, inputVectorExpression, outputColumnNum);
      break;
    case MIN:
      switch (columnVectorType) {
      case LONG:
        evaluator = !isRowEndCurrent ?
            new VectorPTFEvaluatorLongMin(
                windowFrameDef, inputVectorExpression, outputColumnNum) :
            new VectorPTFEvaluatorStreamingLongMin(
                windowFrameDef, inputVectorExpression, outputColumnNum);
        break;
      case DOUBLE:
        evaluator = !isRowEndCurrent ?
            new VectorPTFEvaluatorDoubleMin(
                windowFrameDef, inputVectorExpression, outputColumnNum) :
            new VectorPTFEvaluatorStreamingDoubleMin(
                windowFrameDef, inputVectorExpression, outputColumnNum);
        break;
      case DECIMAL:
        evaluator = !isRowEndCurrent ?
            new VectorPTFEvaluatorDecimalMin(
                windowFrameDef, inputVectorExpression, outputColumnNum) :
            new VectorPTFEvaluatorStreamingDecimalMin(
                windowFrameDef, inputVectorExpression, outputColumnNum);
        break;
      default:
        throw new RuntimeException("Unexpected column vector type " + columnVectorType + " for " + functionType);
      }
      break;
    case MAX:
      switch (columnVectorType) {
      case LONG:
        evaluator = !isRowEndCurrent ?
            new VectorPTFEvaluatorLongMax(
                windowFrameDef, inputVectorExpression, outputColumnNum) :
            new VectorPTFEvaluatorStreamingLongMax(
                windowFrameDef, inputVectorExpression, outputColumnNum);
        break;
      case DOUBLE:
        evaluator = !isRowEndCurrent ?
            new VectorPTFEvaluatorDoubleMax(
                windowFrameDef, inputVectorExpression, outputColumnNum) :
            new VectorPTFEvaluatorStreamingDoubleMax(
                windowFrameDef, inputVectorExpression, outputColumnNum);
        break;
      case DECIMAL:
        evaluator = !isRowEndCurrent ?
            new VectorPTFEvaluatorDecimalMax(
                windowFrameDef, inputVectorExpression, outputColumnNum) :
            new VectorPTFEvaluatorStreamingDecimalMax(
                windowFrameDef, inputVectorExpression, outputColumnNum);
        break;
      default:
        throw new RuntimeException("Unexpected column vector type " + columnVectorType + " for " + functionType);
      }
      break;
    case SUM:
      switch (columnVectorType) {
      case LONG:
        evaluator = !isRowEndCurrent ?
            new VectorPTFEvaluatorLongSum(
                windowFrameDef, inputVectorExpression, outputColumnNum) :
            new VectorPTFEvaluatorStreamingLongSum(
                windowFrameDef, inputVectorExpression, outputColumnNum);
        break;
      case DOUBLE:
        evaluator = !isRowEndCurrent ?
            new VectorPTFEvaluatorDoubleSum(
                windowFrameDef, inputVectorExpression, outputColumnNum) :
            new VectorPTFEvaluatorStreamingDoubleSum(
                windowFrameDef, inputVectorExpression, outputColumnNum);
        break;
      case DECIMAL:
        evaluator = !isRowEndCurrent ?
            new VectorPTFEvaluatorDecimalSum(
                windowFrameDef, inputVectorExpression, outputColumnNum) :
            new VectorPTFEvaluatorStreamingDecimalSum(
                windowFrameDef, inputVectorExpression, outputColumnNum);
        break;
      default:
        throw new RuntimeException("Unexpected column vector type " + columnVectorType + " for " + functionType);
      }
      break;
    case AVG:
      switch (columnVectorType) {
      case LONG:
        evaluator = !isRowEndCurrent ?
            new VectorPTFEvaluatorLongAvg(
                windowFrameDef, inputVectorExpression, outputColumnNum) :
            new VectorPTFEvaluatorStreamingLongAvg(
                windowFrameDef, inputVectorExpression, outputColumnNum);
        break;
      case DOUBLE:
        evaluator = !isRowEndCurrent ?
            new VectorPTFEvaluatorDoubleAvg(
                windowFrameDef, inputVectorExpression, outputColumnNum) :
            new VectorPTFEvaluatorStreamingDoubleAvg(
                windowFrameDef, inputVectorExpression, outputColumnNum);
        break;
      case DECIMAL:
        evaluator = !isRowEndCurrent ?
            new VectorPTFEvaluatorDecimalAvg(
                windowFrameDef, inputVectorExpression, outputColumnNum) :
            new VectorPTFEvaluatorStreamingDecimalAvg(
                windowFrameDef, inputVectorExpression, outputColumnNum);
        break;
      default:
        throw new RuntimeException("Unexpected column vector type " + columnVectorType + " for " + functionType);
      }
      break;
    case FIRST_VALUE:
      switch (columnVectorType) {
      case LONG:
        evaluator = new VectorPTFEvaluatorLongFirstValue(windowFrameDef, inputVectorExpression, outputColumnNum);
        break;
      case DOUBLE:
        evaluator = new VectorPTFEvaluatorDoubleFirstValue(windowFrameDef, inputVectorExpression, outputColumnNum);
        break;
      case DECIMAL:
        evaluator = new VectorPTFEvaluatorDecimalFirstValue(windowFrameDef, inputVectorExpression, outputColumnNum);
        break;
      default:
        throw new RuntimeException("Unexpected column vector type " + columnVectorType + " for " + functionType);
      }
      break;
    case LAST_VALUE:
      switch (columnVectorType) {
      case LONG:
        evaluator = new VectorPTFEvaluatorLongLastValue(windowFrameDef, inputVectorExpression, outputColumnNum);
        break;
      case DOUBLE:
        evaluator = new VectorPTFEvaluatorDoubleLastValue(windowFrameDef, inputVectorExpression, outputColumnNum);
        break;
      case DECIMAL:
        evaluator = new VectorPTFEvaluatorDecimalLastValue(windowFrameDef, inputVectorExpression, outputColumnNum);
        break;
      default:
        throw new RuntimeException("Unexpected column vector type " + columnVectorType + " for " + functionType);
      }
      break;
    case COUNT:
      if (inputVectorExpression == null) {
        evaluator = new VectorPTFEvaluatorCountStar(windowFrameDef, inputVectorExpression, outputColumnNum);
      } else {
        evaluator = new VectorPTFEvaluatorCount(windowFrameDef, inputVectorExpression, outputColumnNum);
      }
      break;
    default:
      throw new RuntimeException("Unexpected function type " + functionType);
    }
    return evaluator;
  }

  public static VectorPTFEvaluatorBase[] getEvaluators(VectorPTFDesc vectorPTFDesc, VectorPTFInfo vectorPTFInfo) {
    String[] evaluatorFunctionNames = vectorPTFDesc.getEvaluatorFunctionNames();
    int evaluatorCount = evaluatorFunctionNames.length;
    WindowFrameDef[] evaluatorWindowFrameDefs = vectorPTFDesc.getEvaluatorWindowFrameDefs();
    VectorExpression[] evaluatorInputExpressions = vectorPTFInfo.getEvaluatorInputExpressions();
    Type[] evaluatorInputColumnVectorTypes = vectorPTFInfo.getEvaluatorInputColumnVectorTypes();

    int[] outputColumnMap = vectorPTFInfo.getOutputColumnMap();

    VectorPTFEvaluatorBase[] evaluators = new VectorPTFEvaluatorBase[evaluatorCount];
    for (int i = 0; i < evaluatorCount; i++) {
      String functionName = evaluatorFunctionNames[i];
      WindowFrameDef windowFrameDef = evaluatorWindowFrameDefs[i];
      SupportedFunctionType functionType = VectorPTFDesc.supportedFunctionsMap.get(functionName);
      VectorExpression inputVectorExpression = evaluatorInputExpressions[i];
      final Type columnVectorType = evaluatorInputColumnVectorTypes[i];

      // The output* arrays start at index 0 for output evaluator aggregations.
      final int outputColumnNum = outputColumnMap[i];

      VectorPTFEvaluatorBase evaluator =
          VectorPTFDesc.getEvaluator(
              functionType, windowFrameDef, columnVectorType, inputVectorExpression, outputColumnNum);

      evaluators[i] = evaluator;
    }
    return evaluators;
  }

  public static int[] getStreamingEvaluatorNums(VectorPTFEvaluatorBase[] evaluators) {
    final int evaluatorCount = evaluators.length;
    ArrayList<Integer> streamingEvaluatorNums = new ArrayList<Integer>();
    for (int i = 0; i < evaluatorCount; i++) {
      final VectorPTFEvaluatorBase evaluator = evaluators[i];
      if (evaluator.streamsResult()) {
        streamingEvaluatorNums.add(i);
      }
    }
    return ArrayUtils.toPrimitive(streamingEvaluatorNums.toArray(new Integer[0]));
  }

  public TypeInfo[] getReducerBatchTypeInfos() {
    return reducerBatchTypeInfos;
  }

  public void setReducerBatchTypeInfos(TypeInfo[] reducerBatchTypeInfos) {
    this.reducerBatchTypeInfos = reducerBatchTypeInfos;
  }

  public boolean getIsPartitionOrderBy() {
    return isPartitionOrderBy;
  }

  public void setIsPartitionOrderBy(boolean isPartitionOrderBy) {
    this.isPartitionOrderBy = isPartitionOrderBy;
  }

  public String[] getEvaluatorFunctionNames() {
    return evaluatorFunctionNames;
  }

  public void setEvaluatorFunctionNames(String[] evaluatorFunctionNames) {
    this.evaluatorFunctionNames = evaluatorFunctionNames;
  }

  public WindowFrameDef[] getEvaluatorWindowFrameDefs() {
    return evaluatorWindowFrameDefs;
  }

  public void setEvaluatorWindowFrameDefs(WindowFrameDef[] evaluatorWindowFrameDefs) {
    this.evaluatorWindowFrameDefs = evaluatorWindowFrameDefs;
  }

  public List<ExprNodeDesc>[] getEvaluatorInputExprNodeDescLists() {
    return evaluatorInputExprNodeDescLists;
  }

  public void setEvaluatorInputExprNodeDescLists(List<ExprNodeDesc>[] evaluatorInputExprNodeDescLists) {
    this.evaluatorInputExprNodeDescLists = evaluatorInputExprNodeDescLists;
  }

  public ExprNodeDesc[] getOrderExprNodeDescs() {
    return orderExprNodeDescs;
  }

  public void setOrderExprNodeDescs(ExprNodeDesc[] orderExprNodeDescs) {
    this.orderExprNodeDescs = orderExprNodeDescs;
  }

  public ExprNodeDesc[] getPartitionExprNodeDescs() {
    return partitionExprNodeDescs;
  }

  public void setPartitionExprNodeDescs(ExprNodeDesc[] partitionExprNodeDescs) {
    this.partitionExprNodeDescs = partitionExprNodeDescs;
  }

  public String[] getOutputColumnNames() {
    return outputColumnNames;
  }

  public void setOutputColumnNames(String[] outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  public TypeInfo[] getOutputTypeInfos() {
    return outputTypeInfos;
  }

  public void setOutputTypeInfos(TypeInfo[] outputTypeInfos) {
    this.outputTypeInfos = outputTypeInfos;
  }

  public void setVectorPTFInfo(VectorPTFInfo vectorPTFInfo) {
    this.vectorPTFInfo = vectorPTFInfo;
  }

  public VectorPTFInfo getVectorPTFInfo() {
    return vectorPTFInfo;
  }

  public void setVectorizedPTFMaxMemoryBufferingBatchCount(
      int vectorizedPTFMaxMemoryBufferingBatchCount) {
    this.vectorizedPTFMaxMemoryBufferingBatchCount = vectorizedPTFMaxMemoryBufferingBatchCount;
  }

  public int getVectorizedPTFMaxMemoryBufferingBatchCount() {
    return vectorizedPTFMaxMemoryBufferingBatchCount;
  }

}
