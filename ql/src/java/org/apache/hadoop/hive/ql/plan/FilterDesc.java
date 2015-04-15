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

import java.util.List;
import org.apache.hadoop.hive.ql.plan.Explain.Level;



/**
 * FilterDesc.
 *
 */
@Explain(displayName = "Filter Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class FilterDesc extends AbstractOperatorDesc {

  /**
   * sampleDesc is used to keep track of the sampling descriptor.
   */
  public static class SampleDesc implements Cloneable {
    // The numerator of the TABLESAMPLE clause
    private int numerator;

    // The denominator of the TABLESAMPLE clause
    private int denominator;

    // Input files can be pruned
    private boolean inputPruning;

    public SampleDesc() {
    }

    public SampleDesc(int numerator, int denominator,
                      List<String> tabBucketCols, boolean inputPruning) {
      this.numerator = numerator;
      this.denominator = denominator;
      this.inputPruning = inputPruning;
    }

    public int getNumerator() {
      return numerator;
    }

    public int getDenominator() {
      return denominator;
    }

    public boolean getInputPruning() {
      return inputPruning;
    }

    @Override
    public Object clone() {
      SampleDesc desc = new SampleDesc(numerator, denominator, null, inputPruning);
      return desc;
    }
    
    public String toString() {
      return inputPruning ? "BUCKET " + numerator + " OUT OF " + denominator: null;  
    }
  }

  private static final long serialVersionUID = 1L;
  private org.apache.hadoop.hive.ql.plan.ExprNodeDesc predicate;
  private boolean isSamplingPred;
  private transient SampleDesc sampleDescr;
  //Is this a filter that should perform a comparison for sorted searches
  private boolean isSortedFilter;

  public FilterDesc() {
  }

  public FilterDesc(
      final org.apache.hadoop.hive.ql.plan.ExprNodeDesc predicate,
      boolean isSamplingPred) {
    this.predicate = predicate;
    this.isSamplingPred = isSamplingPred;
    sampleDescr = null;
  }

  public FilterDesc(
      final org.apache.hadoop.hive.ql.plan.ExprNodeDesc predicate,
      boolean isSamplingPred, final SampleDesc sampleDescr) {
    this.predicate = predicate;
    this.isSamplingPred = isSamplingPred;
    this.sampleDescr = sampleDescr;
  }

  @Explain(displayName = "predicate", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getPredicateString() {
    StringBuffer sb = new StringBuffer();
    PlanUtils.addExprToStringBuffer(predicate, sb);
    return sb.toString();
  }
    
  public org.apache.hadoop.hive.ql.plan.ExprNodeDesc getPredicate() {
    return predicate;
  }

  public void setPredicate(
      final org.apache.hadoop.hive.ql.plan.ExprNodeDesc predicate) {
    this.predicate = predicate;
  }

  @Explain(displayName = "isSamplingPred", explainLevels = { Level.EXTENDED })
  public boolean getIsSamplingPred() {
    return isSamplingPred;
  }

  public void setIsSamplingPred(final boolean isSamplingPred) {
    this.isSamplingPred = isSamplingPred;
  }

  public SampleDesc getSampleDescr() {
    return sampleDescr;
  }

  public void setSampleDescr(final SampleDesc sampleDescr) {
    this.sampleDescr = sampleDescr;
  }

  @Explain(displayName = "sampleDesc", explainLevels = { Level.EXTENDED })
  public String getSampleDescExpr() {
    return sampleDescr == null ? null : sampleDescr.toString();
  }

  public boolean isSortedFilter() {
    return isSortedFilter;
  }

  public void setSortedFilter(boolean isSortedFilter) {
    this.isSortedFilter = isSortedFilter;
  }

  @Override
  public Object clone() {
    FilterDesc filterDesc = new FilterDesc(getPredicate().clone(), getIsSamplingPred());
    if (getIsSamplingPred()) {
      filterDesc.setSampleDescr(getSampleDescr());
    }
    filterDesc.setSortedFilter(isSortedFilter());
    return filterDesc;
  }
}
