package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

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

class JDBCRexCallValidator {
  
  private static final Logger LOG = LoggerFactory.getLogger(JDBCRexCallValidator.class);
  
  static private class JdbcRexCallValidatorVisitor extends RexVisitorImpl<Void> {
    final private SqlDialect dialect; 

    public JdbcRexCallValidatorVisitor(SqlDialect dialect) {
      super (true);
      this.dialect = dialect;
    }

    boolean res = true;

    private boolean validRexCall (RexCall call) {
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
      if (res == true) {
        res = validRexCall (call);
        if (res == true) {
          return super.visitCall(call);
        }
      }
      return null;
    }

    private boolean go (RexNode cond) {
       cond.accept(this);
       return res;
    }
  }
  
  public static boolean isValidJdbcOperation(RexNode cond, SqlDialect dialect) {
    return new JdbcRexCallValidatorVisitor (dialect).go (cond);
  }

};
