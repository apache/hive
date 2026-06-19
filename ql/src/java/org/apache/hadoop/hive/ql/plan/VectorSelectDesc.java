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

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;

/**
 * VectorSelectDesc.
 *
 * Extra parameters beyond SelectDesc just for the VectorSelectOperator.
 *
 * We don't extend SelectDesc because the base OperatorDesc doesn't support
 * clone and adding it is a lot work for little gain.
 */
public class VectorSelectDesc extends AbstractVectorDesc  {

  private static final long serialVersionUID = 1L;

  private VectorExpression[] selectExpressions;
  private int[] projectedOutputColumns;

  public VectorSelectDesc() {
  }

  public void setSelectExpressions(VectorExpression[] selectExpressions) {
    this.selectExpressions = selectExpressions;
  }

  public VectorExpression[] getSelectExpressions() {
    return selectExpressions;
  }

  public void setProjectedOutputColumns(int[] projectedOutputColumns) {
    this.projectedOutputColumns = projectedOutputColumns;
  }

  public int[] getProjectedOutputColumns() {
    return projectedOutputColumns;
  }
}
