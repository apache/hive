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

import java.io.Serial;
import java.io.Serializable;

import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * ColumnStatsDropWork implementation. ColumnStatsDropWork will drop the
 * colStats from the metastore. Work corresponds to statements like:
 * ALTER TABLE src_stat DROP STATISTICS for columns;
 */
@Explain(displayName = "Column Stats Drop Work", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED})
public record ColumnStatsDropWork(String dbName, String tableName) implements Serializable {
  @Serial
  private static final long serialVersionUID = 1L;
}
