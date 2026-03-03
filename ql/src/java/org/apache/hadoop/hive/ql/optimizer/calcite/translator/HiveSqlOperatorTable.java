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
package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.Collections;
import java.util.List;

/**
 * Implementation of {@link SqlOperatorTable} for Hive operators.
 * <p>Implementation details: contrary to other implementations of an operator table
 * this does not follow the SINGLETON pattern since it is stateless thus very cheap to create.
 */
public final class HiveSqlOperatorTable implements SqlOperatorTable {

  @Override
  public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category, SqlSyntax syntax,
      List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {
    // If the operator is registered no need to go again through the registry
    String name = opName.getSimple();
    SqlOperator op = SqlFunctionConverter.hiveToCalcite.get(name.toLowerCase());
    if (op != null) {
      operatorList.add(op);
      return;
    }
    FunctionInfo fi;
    try {
      fi = FunctionRegistry.getFunctionInfo(name);
      if (fi == null) {
        return;
      }
      if (fi.isGenericUDF()) {
        operatorList.add(SqlFunctionConverter.getCalciteOperator(name, fi.getGenericUDF(), ImmutableList.of(), null));
      }
      if (fi.isGenericUDAF()) {
        operatorList.add(SqlFunctionConverter.getCalciteAggFn(name, Collections.emptyList(), null));
      }
    } catch (SemanticException e) {
      // If the lookup in the registry fails just return as if the function is not found
    }
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    // Currently we don't use this method so for simplicity just return empty.
    return Collections.emptyList();
  }
}
