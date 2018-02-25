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
package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Common utilities for validation.
 *
 * ValidationUtility.
 *
 */
public final class ValidationUtility {

  /**
   * Utility class. No instance needs.
   */
  private ValidationUtility () {

  }

  /**
   * Validate skewed table information.
   * @param colNames column names
   * @param skewedColNames skewed column names
   * @param skewedColValues skewed column values
   * @throws SemanticException
   */
  public static void validateSkewedInformation(List<String> colNames, List<String> skewedColNames,
      List<List<String>> skewedColValues) throws SemanticException {
   if (skewedColNames.size() > 0) {
      /**
       * all columns in skewed column name are valid columns
       */
      validateSkewedColNames(colNames, skewedColNames);

      /**
       * find out duplicate skewed column name
       */
      validateSkewedColumnNameUniqueness(skewedColNames);

      if ((skewedColValues == null) || (skewedColValues.size() == 0)) {
        /**
         * skewed column value is empty but skewed col name is not empty. something is wrong
         */
        throw new SemanticException(
            ErrorMsg.SKEWED_TABLE_SKEWED_COL_NAME_VALUE_MISMATCH_2.getMsg());
      } else {
        /**
         * each skewed col value should have the same number as number of skewed column names
         */
        validateSkewedColNameValueNumberMatch(skewedColNames, skewedColValues);
      }
    } else if (skewedColValues.size() > 0) {
      /**
       * skewed column name is empty but skewed col value is not empty. something is wrong
       */
      throw new SemanticException(
          ErrorMsg.SKEWED_TABLE_SKEWED_COL_NAME_VALUE_MISMATCH_1.getMsg());
    }
  }

  /**
   * Skewed column name and value should match.
   *
   * @param skewedColNames
   * @param skewedColValues
   * @throws SemanticException
   */
  public static void validateSkewedColNameValueNumberMatch(List<String> skewedColNames,
      List<List<String>> skewedColValues) throws SemanticException {
    for (List<String> colValue : skewedColValues) {
      if (colValue.size() != skewedColNames.size()) {
        throw new SemanticException(
            ErrorMsg.SKEWED_TABLE_SKEWED_COL_NAME_VALUE_MISMATCH_3.getMsg()
                + skewedColNames.size() + " : "
                + colValue.size());
      }
    }
  }

  /**
   * Skewed column name should be a valid column defined.
   *
   * @param colNames
   * @param skewedColNames
   * @throws SemanticException
   */
  public static void validateSkewedColNames(List<String> colNames, List<String> skewedColNames)
      throws SemanticException {
    // make a copy
    List<String> copySkewedColNames = new ArrayList<String>(skewedColNames);
    // remove valid columns
    copySkewedColNames.removeAll(colNames);
    if (copySkewedColNames.size() > 0) {
      StringBuilder invalidColNames = new StringBuilder();
      for (String name : copySkewedColNames) {
        invalidColNames.append(name);
        invalidColNames.append(" ");
      }
      throw new SemanticException(
          ErrorMsg.SKEWED_TABLE_INVALID_COLUMN.getMsg(invalidColNames.toString()));
    }
  }

  /**
   * Find out duplicate name.
   *
   * @param names
   * @throws SemanticException
   */
  public static void validateSkewedColumnNameUniqueness(List<String> names)
      throws SemanticException {
    Set<String> lookup = new HashSet<String>();
    for (String name : names) {
      if (lookup.contains(name)) {
        throw new SemanticException(ErrorMsg.SKEWED_TABLE_DUPLICATE_COLUMN_NAMES
            .getMsg(name));
      } else {
        lookup.add(name);
      }
    }
  }
}
