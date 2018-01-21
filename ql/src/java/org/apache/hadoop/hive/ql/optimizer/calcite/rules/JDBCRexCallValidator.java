package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlDialect;
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
      return dialect.supportsFunction(call);
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
