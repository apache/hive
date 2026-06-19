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

package org.apache.hadoop.hive.ql.parse;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.session.SessionState;

public class TableHelper {
  /**
   * Get the unique names from a table list. The list may contain some cases where
   * both the dbname and tablename are provided and some cases where the dbname is
   * null, in which case we need to grab the dbname from the SessionState.
   */
  public static Set<String> getUniqueNames(List<Pair<String, String>> tables) {
    return tables.stream().map(TableHelper::getTableName)
        .collect(Collectors.toSet());
  }

  /**
   * Get the table name from a dbname/tablename pair. If the dbname is
   * null, use the SessionState to provide the dbname.
   */
  private static String getTableName(Pair<String, String> dbTablePair) {
    String dbName = dbTablePair.getLeft() == null ?
        SessionState.get().getCurrentDatabase() : dbTablePair.getLeft();
    return TableName.getDbTable(dbName, dbTablePair.getRight());
  }
}
