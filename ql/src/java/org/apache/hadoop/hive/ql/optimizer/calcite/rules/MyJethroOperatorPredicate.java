package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JethroOperatorsPredicate {
  static Logger LOG = LoggerFactory.getLogger(JethroOperatorsPredicate.class);
  
  final static Set<String> allowedJethroOperators = new HashSet<>(Arrays.asList("=", "<>","!=", "<",">", "sqrt",
                                          "cast", "<>", "+", "-", "*", "/", "is not null", "and", "or", "not", "cast"));
  
  private static boolean isValidOperator (SqlOperator operator, RelDataType type, ArrayList<RelDataType> paramsList, SqlDialect dialect) {
    if (dialect != null) {
      return dialect.supportsFunction(operator, type, paramsList);
    }

    if (allowedJethroOperators.contains(operator.toString().toLowerCase())) {
      return true;//type.getSqlTypeName()
    }
    LOG.debug("JETHRO: Skipped push down for " + operator.toString() + ", due to unsupporetd function.");
    return false;
  }

  public static boolean validRexCall (RexCall call, SqlDialect dialect) {
    final SqlOperator operator = call.getOperator();
    //SqlKind kind = call.getKind();
    List <RexNode> operands = call.getOperands();
    RelDataType resType = call.getType();
    ArrayList<RelDataType> paramsListType = new ArrayList<RelDataType>();
    for (RexNode currNode : operands) {
      paramsListType.add(currNode.getType());
    }
    return isValidOperator(operator, resType, paramsListType, dialect);
  }
}
