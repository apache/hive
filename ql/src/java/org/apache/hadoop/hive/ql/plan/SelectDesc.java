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

package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.List;


/**
 * SelectDesc.
 *
 */
@Explain(displayName = "Select Operator")
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

  public SelectDesc(
    final List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList,
    final boolean selectStar, final boolean selStarNoCompute) {
    this.colList = colList;
    this.selectStar = selectStar;
    this.selStarNoCompute = selStarNoCompute;
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

  @Explain(displayName = "outputColumnNames")
  public List<java.lang.String> getOutputColumnNames() {
    return outputColumnNames;
  }

  public void setOutputColumnNames(
    List<java.lang.String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

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
}
