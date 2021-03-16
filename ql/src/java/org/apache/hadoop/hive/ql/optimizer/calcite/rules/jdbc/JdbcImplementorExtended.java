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

package org.apache.hadoop.hive.ql.optimizer.calcite.rules.jdbc;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortExchange;

public class JdbcImplementorExtended extends JdbcImplementor {

  public JdbcImplementorExtended(SqlDialect dialect, JavaTypeFactory typeFactory) {
    super(dialect, typeFactory);
  }

  public Result visit(RelNode e) {
    // throw an instance of RuntimeException for HiveSortExchange
    if (e instanceof HiveSortExchange) {
      throw new UnsupportedOperationException("Need to implement " + e.getClass().getName());
    } else {
      return super.visit(e);
    }
  }
}
