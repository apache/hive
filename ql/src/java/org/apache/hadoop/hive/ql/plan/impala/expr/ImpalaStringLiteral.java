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
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.common.AnalysisException;

/**
 * A StringLiteral that has most of the analysis done by Calcite.
 */
public class ImpalaStringLiteral extends StringLiteral {
  public ImpalaStringLiteral(Analyzer analyzer, String value) throws HiveException {
    super(value);
    try {
      this.analyze(analyzer);
    } catch (AnalysisException e) {
      throw new HiveException("Exception in ImpalaStringLiteral instantiation", e);
    }
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
  }
}
