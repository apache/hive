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

package org.apache.hadoop.hive.ql.exec.tez.monitoring;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public interface SkewedJoinMonitor {

  /**
   * Checks whether a skewed data event is detected during a merge join for a given alias and its
   * row count. If skew is detected, a warning is logged or an exception is thrown, depending on
   * the configured action.
   *
   * @param alias          the byte identifier of the join alias
   * @param rowCount       the number of rows accumulated for the current join key on this alias
   * @param joinKeyColumns the join key column name(s) resolved via RowSchema for the given alias
   *                       (schema info, not data values — e.g. "customer_id")
   * @param tableAlias     the table/subquery alias for the given alias (big or small table)
   * @throws HiveException if skew is detected and abort mode is enabled
   */
  void checkMergeJoinSkew(byte alias, long rowCount, String joinKeyColumns, String tableAlias) throws HiveException;
}
