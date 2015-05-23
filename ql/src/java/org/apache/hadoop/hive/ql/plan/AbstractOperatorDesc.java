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


import java.util.Map;

import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

public class AbstractOperatorDesc implements OperatorDesc {

  protected boolean vectorMode = false;
  protected Statistics statistics;
  protected transient OpTraits opTraits;
  protected transient Map<String, String> opProps;

  static {
    PTFUtils.makeTransient(AbstractOperatorDesc.class, "opProps");
  }

  @Override
  @Explain(skipHeader = true, displayName = "Statistics", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Statistics getStatistics() {
    return statistics;
  }

  @Override
  public void setStatistics(Statistics statistics) {
    this.statistics = statistics;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    throw new CloneNotSupportedException("clone not supported");
  }

  public boolean getVectorMode() {
    return vectorMode;
  }

  public void setVectorMode(boolean vm) {
    this.vectorMode = vm;
  }

  public OpTraits getTraits() {
    return opTraits;
  }

  public void setTraits(OpTraits opTraits) {
    this.opTraits = opTraits;
  }

  public Map<String, String> getOpProps() {
    return opProps;
  }

  public void setOpProps(Map<String, String> props) {
    this.opProps = props;
  }
}
