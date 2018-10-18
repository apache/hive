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

package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Exception from SemanticAnalyzer.
 */

public class CalciteSemanticException extends SemanticException {

  private static final long serialVersionUID = 1L;

  public enum UnsupportedFeature {
    Distinct_without_an_aggreggation, Duplicates_in_RR, Filter_expression_with_non_boolean_return_type,
    Having_clause_without_any_groupby, Invalid_column_reference, Invalid_decimal,
    Less_than_equal_greater_than, Others, Same_name_in_multiple_expressions,
    Schema_less_table, Select_alias_in_having_clause, Select_transform, Subquery,
    Table_sample_clauses, UDTF, Union_type, Unique_join,
    HighPrecissionTimestamp // CALCITE-1690
  };

  private UnsupportedFeature unsupportedFeature;

  public CalciteSemanticException() {
    super();
  }

  public CalciteSemanticException(String message) {
    super(message);
  }

  public CalciteSemanticException(String message, UnsupportedFeature feature) {
    super(message);
    this.setUnsupportedFeature(feature);
  }

  public CalciteSemanticException(Throwable cause) {
    super(cause);
  }

  public CalciteSemanticException(String message, Throwable cause) {
    super(message, cause);
  }

  public CalciteSemanticException(ErrorMsg errorMsg, String... msgArgs) {
    super(errorMsg, msgArgs);
  }

  public UnsupportedFeature getUnsupportedFeature() {
    return unsupportedFeature;
  }

  public void setUnsupportedFeature(UnsupportedFeature unsupportedFeature) {
    this.unsupportedFeature = unsupportedFeature;
  }

}
