// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

@lexer::members {
  public String errorMsg;

  @Override
  public void emitErrorMessage(String msg) {
    // save for caller to detect invalid filter
	errorMsg = msg;
  }
}

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
    Object val = null;
}
    :
    (
       (
	       (key = Identifier op = operator  value = StringLiteral)
	       |
	       (value = StringLiteral  op = operator key = Identifier) { isReverseOrder = true; }
       ) { val = TrimQuotes(value.getText()); }
       |
       (
	       (key = Identifier op = operator value = IntLiteral)
	       |
	       (value = IntLiteral op = operator key = Identifier) { isReverseOrder = true; }
       ) { val = Integer.parseInt(value.getText()); }
    )
    {
        LeafNode node = new LeafNode();
        node.keyName = key.getText();
        node.value = val;
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
NOTEQUAL : '<>' | '!=';
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


IntLiteral
    :
    (Digit)+
    ;

Identifier
    :
    (Letter | Digit) (Letter | Digit | '_')*
    ;

WS  :   (' '|'\r'|'\t'|'\n')+ { skip(); } ;
