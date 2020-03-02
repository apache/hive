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

package org.apache.hadoop.hive.ql.plan.impala.expr;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;

/**
 * A BinaryPredicate that has most of the analysis done by Calcite.
 */
public class ImpalaBinaryCompExpr extends BinaryPredicate {
  public ImpalaBinaryCompExpr(Analyzer analyzer, Function fn, Operator op, Expr e1, Expr e2, Type retType)
      throws HiveException {
    super(op, e1, e2);
    try {
      this.fn_ = fn;
      this.type_ = retType;
      this.analyze(analyzer);
    } catch (AnalysisException e) {
      throw new HiveException("Exception in ImpalaBinaryCompExpr instantiation", e);
    }
  }

  public ImpalaBinaryCompExpr(ImpalaBinaryCompExpr other) {
    super(other);
    this.fn_ = other.fn_;
    this.type_ = other.type_;
  }

  @Override
  public Expr clone() {
    return new ImpalaBinaryCompExpr(this);
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
  }

  /**
   * We need to override resetAnalysisState so that Impala Analyzer doesn't
   * attempt to reanalyze this.
   */
  @Override
  protected void resetAnalysisState() {
  }
}
