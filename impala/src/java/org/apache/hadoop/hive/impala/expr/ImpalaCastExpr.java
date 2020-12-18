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

package org.apache.hadoop.hive.impala.expr;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.CastExpr;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.TypeDef;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;

/**
 * A CastExpr that has most of the analysis done by Calcite.
 * A CastExpr that is already marked as analyzed.
 */
public class ImpalaCastExpr extends CastExpr {
  private final Analyzer analyzer;

  public ImpalaCastExpr(Analyzer analyzer, Function fn, Type type, Expr param) throws HiveException {
    super(new TypeDef(type), param);
    this.fn_ = fn;
    this.type_ = type;
    this.analyzer = analyzer;
    try {
      this.analyze(analyzer);
    } catch (AnalysisException e) {
      throw new HiveException("Exception in ImpalaNullExpr instantiation", e);
    }
  }

  public ImpalaCastExpr(ImpalaCastExpr other) {
    super(other);

    this.fn_ = other.fn_;
    this.type_ = other.type_;
    this.analyzer = other.analyzer;
  }

  @Override
  public Expr clone() {
    return new ImpalaCastExpr(this);
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
  }

  /**
   * We need to override resetAnalysisState so that Impala Analyzer keeps
   * the Expr in its analyzed state.
   */
  @Override
  protected void resetAnalysisState() {
    try {
      super.resetAnalysisState();
      this.analyze(analyzer);
    } catch (AnalysisException e) {
      throw new RuntimeException("Exception reanalyzing expression.", e);
    }
  }
}
