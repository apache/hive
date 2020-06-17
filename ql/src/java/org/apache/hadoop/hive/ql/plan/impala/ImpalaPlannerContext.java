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
package org.apache.hadoop.hive.ql.plan.impala;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.util.EventSequence;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ImpalaPlannerContext extends PlannerContext {

  private ImpalaBasicAnalyzer analyzer;
  private TExecRequest execRequest;
  private List<Expr> resultExprs;
  // a per-query cache of metastore table instance to HdfsTable instance
  private Map<Table, HdfsTable> hdfsTableMap;

  public ImpalaPlannerContext(TQueryCtx queryCtx, EventSequence timeline,
      ImpalaBasicAnalyzer analyzer) {
    super(queryCtx, timeline);
    this.analyzer = analyzer;
    this.hdfsTableMap = new HashMap<>();
  }

  @Override
  public Analyzer getRootAnalyzer() {return analyzer;}

  @Override
  public boolean hasTableSink() {
    // TODO: CDPD-13837: return true/false based on statement types
    return false;
  }

  public void setExecRequest(TExecRequest execRequest) {
    this.execRequest = execRequest;
  }

  public TExecRequest getExecRequest() {
    return execRequest;
  }

  public void setResultExprs(List<Expr> resultExprs) {
    this.resultExprs = resultExprs;
  }

  public List<Expr> getResultExprs() {
    Preconditions.checkNotNull(resultExprs);
    Preconditions.checkState(resultExprs.size() > 0);
    return resultExprs;
  }

  public HdfsTable getHdfsTable(Table msTbl) {
    return hdfsTableMap.get(msTbl);
  }

  public void addHdfsTable(Table msTbl, HdfsTable hdfsTable) {
    hdfsTableMap.put(msTbl, hdfsTable);
  }

}
