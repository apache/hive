package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyJdbcRexCallValidator {
  
  static Logger LOG = LoggerFactory.getLogger(MyJdbcRexCallValidator.class);
  
  static private class JdbcRexCallValidatorVisitor extends RexVisitorImpl<Void> {
    
    JdbcRexCallValidatorVisitor () {
      super (true);
    }
    
    boolean res = true;
    
    @Override
    public Void visitCall(RexCall call) {
      boolean isValidCall = JethroOperatorsPredicate.validRexCall (call);
      if (res == true) {
        res = isValidCall;
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

};
