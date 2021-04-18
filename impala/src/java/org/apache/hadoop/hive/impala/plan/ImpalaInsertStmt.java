/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.impala.plan;

import java.util.List;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.InsertStmt;
import org.apache.impala.catalog.FeTable;

/**
 * This is a mock InsertStmt implementation providing only necessary information
 * for {@link ImpalaPlanner}.
 */
public class ImpalaInsertStmt extends InsertStmt {

  private final FeTable table_;
  private final List<Expr> partitionKeyExprs_;

  protected ImpalaInsertStmt(FeTable targetTable, List<Expr> partitionKeyExprs) {
    super(null, null, false, null, null, null, null, null, false);
    this.table_ = targetTable;
    this.partitionKeyExprs_ = partitionKeyExprs;
  }

  @Override
  public FeTable getTargetTable() {
    return this.table_;
  }

  @Override
  public List<Expr> getPartitionKeyExprs() {
    return this.partitionKeyExprs_;
  }
}
