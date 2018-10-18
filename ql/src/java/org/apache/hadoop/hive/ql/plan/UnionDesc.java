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
import org.apache.hadoop.hive.ql.plan.Explain.Level;



/**
 * unionDesc is a empty class currently. However, union has more than one input
 * (as compared with forward), and therefore, we need a separate class.
 **/
@Explain(displayName = "Union", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class UnionDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;
  private transient int numInputs;
  // If this UnionOperator is inside the reduce side of an MR job generated
  // by Correlation Optimizer, which means all inputs of this UnionOperator are
  // from DemuxOperator. If so, we should not touch this UnionOperator in genMapRedTasks.
  private transient boolean allInputsInSameReducer;

  @SuppressWarnings("nls")
  public UnionDesc() {
    numInputs = 2;
    allInputsInSameReducer = false;
  }

  /**
   * @return the numInputs
   */
  public int getNumInputs() {
    return numInputs;
  }

  /**
   * @param numInputs
   *          the numInputs to set
   */
  public void setNumInputs(int numInputs) {
    this.numInputs = numInputs;
  }

  public boolean isAllInputsInSameReducer() {
    return allInputsInSameReducer;
  }

  public void setAllInputsInSameReducer(boolean allInputsInSameReducer) {
    this.allInputsInSameReducer = allInputsInSameReducer;
  }
}
