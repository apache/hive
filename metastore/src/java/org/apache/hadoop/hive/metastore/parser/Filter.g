grammar Filter;

options
{
  k=3;
}


// Package headers
@header {
package org.apache.hadoop.hive.metastore.parser;

import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LeafNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.Operator;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LogicalOperator;
}
@lexer::header {package org.apache.hadoop.hive.metastore.parser;}

@members {
  public ExpressionTree tree = new ExpressionTree();

  public static String TrimQuotes (String input) {
    if (input.length () > 1) {
      if ((input.charAt (0) == '"' && input.charAt (input.length () - 1) == '"')
        || (input.charAt (0) == '\'' && input.charAt (input.length () - 1) == '\'')) {
        return input.substring (1, input.length () - 1);
      }
    }
    return input;
  }
}

@rulecatch{
  catch (RecognitionException e){
    throw e;
  }
}

//main rule
filter
    :
    orExpression 
    ;

orExpression
    :
    andExpression (KW_OR andExpression { tree.addIntermediateNode(LogicalOperator.OR); } )*
    ;

andExpression
    :
    expression (KW_AND expression  { tree.addIntermediateNode(LogicalOperator.AND); } )*
    ;

expression
    :
    LPAREN orExpression RPAREN
    |
    operatorExpression
    ;

operatorExpression 
@init { 
    boolean isReverseOrder = false;
}
    :
    (
       (key = Identifier op = operator  value = StringLiteral)
       | 
       (value = StringLiteral  op = operator key = Identifier) { isReverseOrder = true; }
    )
    {
        LeafNode node = new LeafNode();
        node.keyName = key.getText();
        node.value = TrimQuotes(value.getText());
        node.operator = op;
        node.isReverseOrder = isReverseOrder;

        tree.addLeafNode(node);
    };

operator returns [Operator op]
   :
   t = (LESSTHAN | LESSTHANOREQUALTO | GREATERTHAN | GREATERTHANOREQUALTO | KW_LIKE | EQUAL | NOTEQUAL)
   {
      $op = Operator.fromString(t.getText().toUpperCase());
   };

// Keywords
KW_AND : 'AND';
KW_OR : 'OR';
KW_LIKE : 'LIKE';

// Operators
LPAREN : '(' ;
RPAREN : ')' ;
EQUAL : '=';
NOTEQUAL : '<>';
LESSTHANOREQUALTO : '<=';
LESSTHAN : '<';
GREATERTHANOREQUALTO : '>=';
GREATERTHAN : '>';

// LITERALS
fragment
Letter
    : 'a'..'z' | 'A'..'Z'
    ;

fragment
Digit
    :
    '0'..'9'
    ;

StringLiteral
    :
    ( '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '\"' ( ~('\"'|'\\') | ('\\' .) )* '\"'
    )
    ;

Identifier
    :
    (Letter | Digit) (Letter | Digit | '_')*
    ;

WS  :   (' '|'\r'|'\t'|'\n')+ { skip(); } ;

