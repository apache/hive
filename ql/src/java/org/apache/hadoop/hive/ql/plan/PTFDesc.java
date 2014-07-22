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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.parse.LeadLagInfo;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFQueryInputType;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowTableFunctionDef;

@Explain(displayName = "PTF Operator")
public class PTFDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;
  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(PTFDesc.class.getName());

  PartitionedTableFunctionDef funcDef;
  LeadLagInfo llInfo;
  /*
   * is this PTFDesc for a Map-Side PTF Operation?
   */
  boolean isMapSide = false;

  transient Configuration cfg;

  static{
    PTFUtils.makeTransient(PTFDesc.class, "llInfo");
    PTFUtils.makeTransient(PTFDesc.class, "cfg");
  }

  public PartitionedTableFunctionDef getFuncDef() {
    return funcDef;
  }

  public void setFuncDef(PartitionedTableFunctionDef funcDef) {
    this.funcDef = funcDef;
  }

  public PartitionedTableFunctionDef getStartOfChain() {
    return funcDef == null ? null : funcDef.getStartOfChain();
  }

  public LeadLagInfo getLlInfo() {
    return llInfo;
  }

  public void setLlInfo(LeadLagInfo llInfo) {
    this.llInfo = llInfo;
  }

  public boolean forWindowing() {
    return funcDef != null && (funcDef instanceof WindowTableFunctionDef);
  }

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
