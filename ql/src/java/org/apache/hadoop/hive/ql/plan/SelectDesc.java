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
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hive.ql.optimizer.signature.Signature;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;


/**
 * SelectDesc.
 *
 */
@Explain(displayName = "Select Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class SelectDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;
  private List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList;
  private List<java.lang.String> outputColumnNames;
  private boolean selectStar;
  private boolean selStarNoCompute;

  public SelectDesc() {
  }

  public SelectDesc(final boolean selStarNoCompute) {
    this.selStarNoCompute = selStarNoCompute;
  }

  public SelectDesc(
    final List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList,
    final List<java.lang.String> outputColumnNames) {
    this(colList, outputColumnNames, false);
  }

  public SelectDesc(
    final List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList,
    List<java.lang.String> outputColumnNames,
    final boolean selectStar) {
    this.colList = colList;
    this.selectStar = selectStar;
    this.outputColumnNames = outputColumnNames;
  }

  @Override
  public Object clone() {
    SelectDesc ret = new SelectDesc();
    ret.setColList(getColList() == null ? null : new ArrayList<ExprNodeDesc>(getColList()));
    ret.setOutputColumnNames(getOutputColumnNames() == null ? null :
      new ArrayList<String>(getOutputColumnNames()));
    ret.setSelectStar(selectStar);
    ret.setSelStarNoCompute(selStarNoCompute);
    return ret;
  }

  @Signature
  @Explain(displayName = "expressions")
  public String getColListString() {
    return PlanUtils.getExprListString(colList);
  }

  public List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> getColList() {
    return colList;
  }

  public void setColList(
    final List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList) {
    this.colList = colList;
  }

  @Signature
  @Explain(displayName = "outputColumnNames")
  public List<java.lang.String> getOutputColumnNames() {
    return outputColumnNames;
  }

  @Explain(displayName = "Output", explainLevels = { Level.USER })
  public List<java.lang.String> getUserLevelExplainOutputColumnNames() {
    return outputColumnNames;
  }

  public void setOutputColumnNames(
    List<java.lang.String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  @Signature
  @Explain(displayName = "SELECT * ")
  public String explainNoCompute() {
    if (isSelStarNoCompute()) {
      return "(no compute)";
    } else {
      return null;
    }
  }

  /**
   * @return the selectStar
   */
  public boolean isSelectStar() {
    return selectStar;
  }

  /**
   * @param selectStar
   *          the selectStar to set
   */
  public void setSelectStar(boolean selectStar) {
    this.selectStar = selectStar;
  }

  /**
   * @return the selStarNoCompute
   */
  public boolean isSelStarNoCompute() {
    return selStarNoCompute;
  }

  /**
   * @param selStarNoCompute
   *          the selStarNoCompute to set
   */
  public void setSelStarNoCompute(boolean selStarNoCompute) {
    this.selStarNoCompute = selStarNoCompute;
  }


  public class SelectOperatorExplainVectorization extends OperatorExplainVectorization {

    private final SelectDesc selectDesc;
    private final VectorSelectDesc vectorSelectDesc;

    public SelectOperatorExplainVectorization(SelectDesc selectDesc,
        VectorSelectDesc vectorSelectDesc) {
      // Native vectorization supported.
      super(vectorSelectDesc, true);
      this.selectDesc = selectDesc;
      this.vectorSelectDesc = vectorSelectDesc;
    }

    @Explain(vectorization = Vectorization.OPERATOR, displayName = "selectExpressions", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getSelectExpressions() {
      return vectorExpressionsToStringList(vectorSelectDesc.getSelectExpressions());
    }

    @Explain(vectorization = Vectorization.EXPRESSION, displayName = "projectedOutputColumnNums", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getProjectedOutputColumnNums() {
      return Arrays.toString(vectorSelectDesc.getProjectedOutputColumns());
    }
  }

  @Explain(vectorization = Vectorization.OPERATOR, displayName = "Select Vectorization", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public SelectOperatorExplainVectorization getSelectVectorization() {
    VectorSelectDesc vectorSelectDesc = (VectorSelectDesc) getVectorDesc();
    if (vectorSelectDesc == null) {
      return null;
    }
    return new SelectOperatorExplainVectorization(this, vectorSelectDesc);
  }

  @Override
  public boolean isSame(OperatorDesc other) {
    if (getClass().getName().equals(other.getClass().getName())) {
      SelectDesc otherDesc = (SelectDesc) other;
      return Objects.equals(getColListString(), otherDesc.getColListString()) &&
          Objects.equals(getOutputColumnNames(), otherDesc.getOutputColumnNames()) &&
          Objects.equals(explainNoCompute(), otherDesc.explainNoCompute());
    }
    return false;
  }

}
