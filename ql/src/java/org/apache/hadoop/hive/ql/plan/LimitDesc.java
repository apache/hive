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

import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;


/**
 * LimitDesc.
 *
 */
@Explain(displayName = "Limit", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class LimitDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;
  private int offset = 0;
  private int limit;
  private int leastRows = -1;

  public LimitDesc() {
  }

  public LimitDesc(final int limit) {
    this.limit = limit;
  }

  public LimitDesc(final int offset, final int limit) {
    this.offset = offset;
    this.limit = limit;
  }

  /**
   * not to print the offset if it is 0 we need to turn null.
   * use Integer instead of int.
   */
  @Explain(displayName = "Offset of rows", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Integer getOffset() {
    return (offset == 0) ? null : new Integer(offset);
  }

  public void setOffset(Integer offset) {
    this.offset = offset;
  }

  @Explain(displayName = "Number of rows", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public int getLimit() {
    return limit;
  }

  public void setLimit(final int limit) {
    this.limit = limit;
  }

  public int getLeastRows() {
    return leastRows;
  }

  public void setLeastRows(int leastRows) {
    this.leastRows = leastRows;
  }

  public class LimitOperatorExplainVectorization extends OperatorExplainVectorization {

    public LimitOperatorExplainVectorization(LimitDesc limitDesc, VectorDesc vectorDesc) {
      // Native vectorization supported.
      super(vectorDesc, true);
    }
  }

  @Explain(vectorization = Vectorization.OPERATOR, displayName = "Limit Vectorization", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public LimitOperatorExplainVectorization getLimitVectorization() {
    if (vectorDesc == null) {
      return null;
    }
    return new LimitOperatorExplainVectorization(this, vectorDesc);
  }
}
