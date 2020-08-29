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
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.EventSequence;

import java.util.List;

public class ImpalaPlannerContext extends PlannerContext {
  private final ImpalaQueryContext queryContext;
  private List<Expr> resultExprs;
  private ImpalaTableLoader tableLoader;
  private FeTable targetTable;
  private Partition targetPartition;

  public ImpalaPlannerContext(ImpalaQueryContext queryContext, EventSequence timeline) {
    super(queryContext.getTQueryCtx(), timeline);
    this.queryContext = queryContext;
    this.tableLoader = new ImpalaTableLoader(timeline, queryContext);
  }

  public void setTargetTable(FeTable targetTable) {
    this.targetTable = targetTable;
    queryContext.getAnalyzer().getDescTbl().setTargetTable(targetTable);
  }

  public void initTxnId() {
    queryContext.initTxnId();
  }

  public FeTable getTargetTable() {
    return targetTable;
  }

  public void setTargetPartition(Partition targetPartition) {
    this.targetPartition = targetPartition;
  }

  public Partition getTargetPartition() {
    return targetPartition;
  }

  @Override
  public Analyzer getRootAnalyzer() {
    return queryContext.getAnalyzer();
  }

  @Override
  public boolean hasTableSink() {
    return targetTable != null;
  }

  public List<TNetworkAddress> getHostLocations() {
    return queryContext.getHostLocations();
  }

  public void setResultExprs(List<Expr> resultExprs) {
    this.resultExprs = resultExprs;
  }

  public List<Expr> getResultExprs() {
    Preconditions.checkNotNull(resultExprs);
    Preconditions.checkState(resultExprs.size() > 0);
    return resultExprs;
  }

  public ImpalaTableLoader getTableLoader() {
    return tableLoader;
  }

  public ImpalaQueryContext getQueryContext() {
    return queryContext;
  }


}
