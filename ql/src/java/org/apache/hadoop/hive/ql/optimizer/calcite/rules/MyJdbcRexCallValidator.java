package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyJdbcRexCallValidator {
  
  static Logger LOG = LoggerFactory.getLogger(MyJdbcRexCallValidator.class);
  
  static private class JdbcRexCallValidatorVisitor extends RexVisitorImpl<Void> {
    final private SqlDialect dialect; 
    
    JdbcRexCallValidatorVisitor () {
      super (true);
      dialect = null;
    }
    
    public JdbcRexCallValidatorVisitor(SqlDialect dialect) {
      super (true);
      this.dialect = dialect;
    }

    boolean res = true;
    
    @Override
    public Void visitCall(RexCall call) {
      if (res == true) {
        res = JethroOperatorsPredicate.validRexCall (call, dialect);
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
  
  static public boolean isValidJdbcOperation (RexNode cond) {
    return new JdbcRexCallValidatorVisitor ().go (cond);
  }

  public static boolean isValidJdbcOperation(RexNode cond, SqlDialect dialect) {
    return new JdbcRexCallValidatorVisitor (dialect).go (cond);
  }

};
