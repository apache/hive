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
package org.apache.hadoop.hive.ql.parse.rewrite.sql;

import org.apache.hadoop.hive.ql.Context;

public class COWWithClauseBuilder {

  public void appendWith(MultiInsertSqlBuilder sqlBuilder, String filePathCol, String whereClause) {
    sqlBuilder.append("WITH t AS (");
    sqlBuilder.append("\n");
    sqlBuilder.append("select ");
    sqlBuilder.appendAcidSelectColumnsForDeletedRecords(Context.Operation.DELETE);
    sqlBuilder.removeLastChar();
    sqlBuilder.append(" from (");
    sqlBuilder.append("\n");
    sqlBuilder.append("select ");
    sqlBuilder.appendAcidSelectColumnsForDeletedRecords(Context.Operation.DELETE);
    sqlBuilder.append(" row_number() OVER (partition by ").append(filePathCol).append(") rn");
    sqlBuilder.append(" from ");
    sqlBuilder.append(sqlBuilder.getTargetTableFullName());
    sqlBuilder.append("\n");
    sqlBuilder.append("where ").append(whereClause);
    sqlBuilder.append("\n");
    sqlBuilder.append(") q");
    sqlBuilder.append("\n");
    sqlBuilder.append("where rn=1\n)\n");
  }
}
