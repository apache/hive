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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class that helps identify Hive-Jdbc functions gaps.
 */

public final class JDBCRexCallValidator {

  private static final Logger LOG = LoggerFactory.getLogger(JDBCRexCallValidator.class);

  private static final class JdbcRexCallValidatorVisitor extends RexVisitorImpl<Void> {
    private final SqlDialect dialect;

    private JdbcRexCallValidatorVisitor(SqlDialect dialect) {
      super(true);
      this.dialect = dialect;
    }

    boolean res = true;

    private boolean validRexCall(RexCall call) {
      if (call instanceof RexOver) {
        LOG.debug("RexOver operator push down is not supported for now with the following operator:" + call);
        return false;
      }
      final SqlOperator operator = call.getOperator();
      List <RexNode> operands = call.getOperands();
      RelDataType resType = call.getType();
      ArrayList<RelDataType> paramsListType = new ArrayList<RelDataType>();
      for (RexNode currNode : operands) {
        paramsListType.add(currNode.getType());
      }
      return dialect.supportsFunction(operator, resType, paramsListType);
    }

    @Override
    public Void visitCall(RexCall call) {
      if (res) {
        res = validRexCall(call);
        if (res) {
          return super.visitCall(call);
        }
      }
      return null;
    }

    private boolean go(RexNode cond) {
      cond.accept(this);
      return res;
    }
  }

  private JDBCRexCallValidator() {
  }

  public static boolean isValidJdbcOperation(RexNode cond, SqlDialect dialect) {
    return new JdbcRexCallValidatorVisitor(dialect).go(cond);
  }

};
