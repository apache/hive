// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.hadoop.hive.ql.plan.impala;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TQueryCtx;

import java.util.List;

/**
 * Impala relies on the Analyzer for various semantic analysis of expressions
 * and plan nodes. Since Hive has already done most of this analysis we want
 * a basic analyzer that allows for analyzing/validating the final physical plan nodes, slots
 * and expressions. This BasicAnalyzer extends the Analyzer and overrides a few methods.
 */
public class ImpalaBasicAnalyzer extends Analyzer {

  private StmtMetadataLoader.StmtTableCache stmtTableCache;

  public ImpalaBasicAnalyzer(StmtMetadataLoader.StmtTableCache stmtTableCache,
      TQueryCtx queryCtx,
        AuthorizationFactory authzFactory,
        List<TNetworkAddress> hostLocations) {
    super(stmtTableCache, queryCtx, authzFactory, null);
    this.stmtTableCache = stmtTableCache;

    this.getHostIndex().populate(hostLocations);
  }

  // The createAuxEqPredicate is called internally by Impala to
  // create new predicates based on transitive equality. At this point,
  // we expect that such predicates should have already been created
  // by Hive and hence we make this a no-op
  @Override
  public void createAuxEqPredicate(Expr lhs, Expr rhs) {
  }

  public void setTable(TableName tableName, FeTable table) {
    stmtTableCache.tables.put(tableName, table);
  }

}
