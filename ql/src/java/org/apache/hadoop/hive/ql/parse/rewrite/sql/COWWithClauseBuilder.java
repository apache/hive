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

  public void appendWith(MultiInsertSqlGenerator sqlGenerator, String filePathCol, String whereClause) {
    appendWith(sqlGenerator, null, filePathCol, whereClause, true);
  }
  public void appendWith(MultiInsertSqlGenerator sqlGenerator, String sourceName, String filePathCol, 
                         String whereClause, boolean skipPrefix) {
    sqlGenerator.newCteExpr();
    
    sqlGenerator.append("t AS (");
    sqlGenerator.append("\n").indent();
    sqlGenerator.append("select ");
    sqlGenerator.appendAcidSelectColumnsForDeletedRecords(Context.Operation.DELETE, skipPrefix);
    sqlGenerator.removeLastChar();
    sqlGenerator.append(" from (");
    sqlGenerator.append("\n").indent().indent();
    sqlGenerator.append("select ");
    sqlGenerator.appendAcidSelectColumnsForDeletedRecords(Context.Operation.DELETE, skipPrefix);
    sqlGenerator.append(" row_number() OVER (partition by ").append(filePathCol).append(") rn");
    sqlGenerator.append(" from ");
    sqlGenerator.append(sourceName == null ? sqlGenerator.getTargetTableFullName() : sourceName);
    sqlGenerator.append("\n").indent().indent();
    sqlGenerator.append("where ").append(whereClause);
    sqlGenerator.append("\n").indent();
    sqlGenerator.append(") q");
    sqlGenerator.append("\n").indent();
    sqlGenerator.append("where rn=1\n)\n");
    
    sqlGenerator.addCteExpr();
  }
}
