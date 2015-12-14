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
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.LeadLagInfo;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.ptf.PTFInputDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowTableFunctionDef;
import org.apache.hadoop.hive.ql.udf.ptf.Noop;
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
}
