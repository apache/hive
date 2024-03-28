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
    DISTINCT_WITHOUT_AN_AGGREGATION,
    DUPLICATES_IN_RR,
    FILTER_EXPRESSION_WITH_NON_BOOLEAN_RETURN_TYPE,
    HAVING_CLAUSE_WITHOUT_ANY_GROUPBY,
    INVALID_COLUMN_REFERENCE,
    INVALID_DECIMAL,
    LESS_THAN_EQUAL_GREATER_THAN,
    OTHERS,
    SAME_NAME_IN_MULTIPLE_EXPRESSIONS,
    SCHEMA_LESS_TABLE,
    SELECT_ALIAS_IN_HAVING_CLAUSE,
    SELECT_TRANSFORM,
    SUBQUERY,
    TABLE_SAMPLE_CLAUSES,
    UDTF,
    UNION_TYPE,
    UNIQUE_JOIN
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
