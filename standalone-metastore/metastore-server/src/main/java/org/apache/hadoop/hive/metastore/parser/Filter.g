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

@lexer::header {
package org.apache.hadoop.hive.metastore.parser;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
}

@lexer::members {
  public String errorMsg;

  private static final Pattern datePattern = Pattern.compile(".*(\\d\\d\\d\\d-\\d\\d-\\d\\d).*");
  private static final ThreadLocal<SimpleDateFormat> dateFormat =
       new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      SimpleDateFormat val = new SimpleDateFormat("yyyy-MM-dd");
      val.setLenient(false); // Without this, 2020-20-20 becomes 2021-08-20.
      val.setTimeZone(TimeZone.getTimeZone("UTC"));
      return val;
    };
  };

  public static java.sql.Date ExtractDate (String input) {
    Matcher m = datePattern.matcher(input);
    if (!m.matches()) {
      return null;
    }
    try {
      return new java.sql.Date(dateFormat.get().parse(m.group(1)).getTime());
    } catch (ParseException pe) {
      return null;
    }
  }

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
    :
    betweenExpression
    |
    inExpression
    |
    multiColInExpression
    |
    binOpExpression
    ;

binOpExpression
@init {
    boolean isReverseOrder = false;
    Object val = null;
}
    :
    (
       (
         (key = Identifier op = operator  value = DateLiteral)
         |
         (value = DateLiteral  op = operator key = Identifier) { isReverseOrder = true; }
       ) { val = FilterLexer.ExtractDate(value.getText()); }
       |
       (
         (key = Identifier op = operator  value = StringLiteral)
         |
         (value = StringLiteral  op = operator key = Identifier) { isReverseOrder = true; }
       ) { val = TrimQuotes(value.getText()); }
       |
       (
         (key = Identifier op = operator value = IntegralLiteral)
         |
         (value = IntegralLiteral op = operator key = Identifier) { isReverseOrder = true; }
       ) { val = Long.parseLong(value.getText()); }
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

betweenExpression
@init {
    Object leftV = null;
    Object rightV = null;
    boolean isPositive = true;
}
    :
    (
       key = Identifier (KW_NOT { isPositive = false; } )? BETWEEN
       (
         (left = DateLiteral KW_AND right = DateLiteral) {
            leftV = FilterLexer.ExtractDate(left.getText());
            rightV = FilterLexer.ExtractDate(right.getText());
         }
         |
         (left = StringLiteral KW_AND right = StringLiteral) { leftV = TrimQuotes(left.getText());
            rightV = TrimQuotes(right.getText());
         }
         |
         (left = IntegralLiteral KW_AND right = IntegralLiteral) { leftV = Long.parseLong(left.getText());
            rightV = Long.parseLong(right.getText());
         }
       )
    )
    {
        LeafNode leftNode = new LeafNode(), rightNode = new LeafNode();
        leftNode.keyName = rightNode.keyName = key.getText();
        leftNode.value = leftV;
        rightNode.value = rightV;
        leftNode.operator = isPositive ? Operator.GREATERTHANOREQUALTO : Operator.LESSTHAN;
        rightNode.operator = isPositive ? Operator.LESSTHANOREQUALTO : Operator.GREATERTHAN;
        tree.addLeafNode(leftNode);
        tree.addLeafNode(rightNode);
        tree.addIntermediateNode(isPositive ? LogicalOperator.AND : LogicalOperator.OR);
    };

inExpression
@init {
    List constants = new ArrayList();
    Object constantV = null;
    boolean isPositive = true;
}
    :
    (
        LPAREN key = Identifier RPAREN ( KW_NOT { isPositive = false; } )? IN LPAREN
        (
            (
                constant = DateLiteral
                {
                    constantV = FilterLexer.ExtractDate(constant.getText());
                    constants.add(constantV);
                }
                (
                    COMMA constant = DateLiteral
                    {
                        constantV = FilterLexer.ExtractDate(constant.getText());
                        constants.add(constantV);
                    }
                )*
            )
            |
            (
                constant = StringLiteral
                {
                    constantV = TrimQuotes(constant.getText());
                    constants.add(constantV);
                }
                (
                    COMMA constant = StringLiteral
                    {
                        constantV = TrimQuotes(constant.getText());
                        constants.add(constantV);
                    }
                )*
            )
            |
            (
                constant = IntegralLiteral
                {
                    constantV = Long.parseLong(constant.getText());
                    constants.add(constantV);
                }
                (
                    COMMA constant = IntegralLiteral
                    {
                        constantV = Long.parseLong(constant.getText());
                        constants.add(constantV);
                    }
                )*
            )
        ) RPAREN
    )
    {
        for (int i = 0; i < constants.size(); i++) {
            Object value = constants.get(i);
            LeafNode leaf = new LeafNode();
            leaf.keyName = key.getText();
            leaf.value = value;
            leaf.operator = isPositive ? Operator.EQUALS : Operator.NOTEQUALS2;
            tree.addLeafNode(leaf);
            if (i != 0) {
                tree.addIntermediateNode(isPositive ? LogicalOperator.OR : LogicalOperator.AND);
            }
        }
    };

multiColInExpression
@init {
    List<String> keyNames = new ArrayList<String>();
    List constants = new ArrayList();
    List partialConstants;
    String keyV = null;
    Object constantV = null;
    boolean isPositive = true;
}
    :
    (
        LPAREN
        (
            KW_STRUCT LPAREN key = Identifier
            {
                keyV = key.getText();
                keyNames.add(keyV);
            }
            (
                COMMA key = Identifier
                {
                    keyV = key.getText();
                    keyNames.add(keyV);
                }
            )* RPAREN
        ) RPAREN ( KW_NOT { isPositive = false; } )? IN LPAREN KW_CONST KW_STRUCT LPAREN
        {
            partialConstants = new ArrayList();
        }
        (
            constant = DateLiteral
            {
                constantV = FilterLexer.ExtractDate(constant.getText());
                partialConstants.add(constantV);
            }
            | constant = StringLiteral
            {
                constantV = TrimQuotes(constant.getText());
                partialConstants.add(constantV);
            }
            | constant = IntegralLiteral
            {
                constantV = Long.parseLong(constant.getText());
                partialConstants.add(constantV);
            }
        )
        (
            COMMA
            (
                constant = DateLiteral
                {
                    constantV = FilterLexer.ExtractDate(constant.getText());
                    partialConstants.add(constantV);
                }
                | constant = StringLiteral
                {
                    constantV = TrimQuotes(constant.getText());
                    partialConstants.add(constantV);
                }
                | constant = IntegralLiteral
                {
                    constantV = Long.parseLong(constant.getText());
                    partialConstants.add(constantV);
                }
            )
        )*
        {
            constants.add(partialConstants);
        }
        RPAREN
        (
            COMMA KW_CONST KW_STRUCT LPAREN
            {
                partialConstants = new ArrayList();
            }
            (
                constant = DateLiteral
                {
                    constantV = FilterLexer.ExtractDate(constant.getText());
                    partialConstants.add(constantV);
                }
                | constant = StringLiteral
                {
                    constantV = TrimQuotes(constant.getText());
                    partialConstants.add(constantV);
                }
                | constant = IntegralLiteral
                {
                    constantV = Long.parseLong(constant.getText());
                    partialConstants.add(constantV);
                }
            )
            (
                COMMA
                (
                    constant = DateLiteral
                    {
                        constantV = FilterLexer.ExtractDate(constant.getText());
                        partialConstants.add(constantV);
                    }
                    | constant = StringLiteral
                    {
                        constantV = TrimQuotes(constant.getText());
                        partialConstants.add(constantV);
                    }
                    | constant = IntegralLiteral
                    {
                        constantV = Long.parseLong(constant.getText());
                        partialConstants.add(constantV);
                    }
                )
            )*
            {
                constants.add(partialConstants);
            }
            RPAREN
        )* RPAREN
    )
    {
        for (int i = 0; i < constants.size(); i++) {
            List list = (List) constants.get(i);
            assert keyNames.size() == list.size();
            for (int j=0; j < list.size(); j++) {
                String keyName = keyNames.get(j);
                Object value = list.get(j);
                LeafNode leaf = new LeafNode();
                leaf.keyName = keyName;
                leaf.value = value;
                leaf.operator = isPositive ? Operator.EQUALS : Operator.NOTEQUALS2;
                tree.addLeafNode(leaf);
                if (j != 0) {
                    tree.addIntermediateNode(isPositive ? LogicalOperator.AND : LogicalOperator.OR);
                }
            }
            if (i != 0) {
                tree.addIntermediateNode(isPositive ? LogicalOperator.OR : LogicalOperator.AND);
            }
        }
    };

// Keywords
KW_NOT : 'NOT';
KW_AND : 'AND';
KW_OR : 'OR';
KW_LIKE : 'LIKE';
KW_DATE : 'date';
KW_CONST : 'CONST';
KW_STRUCT : 'STRUCT';

// Operators
LPAREN : '(' ;
RPAREN : ')' ;
COMMA : ',' ;
EQUAL : '=';
NOTEQUAL : '<>' | '!=';
LESSTHANOREQUALTO : '<=';
LESSTHAN : '<';
GREATERTHANOREQUALTO : '>=';
GREATERTHAN : '>';
BETWEEN : 'BETWEEN';
IN : 'IN';

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

fragment DateString
    :
    (Digit)(Digit)(Digit)(Digit) '-' (Digit)(Digit) '-' (Digit)(Digit)
    ;

/* When I figure out how to make lexer backtrack after validating predicate, dates would be able 
to support single quotes [( '\'' DateString '\'' ) |]. For now, what we do instead is have a hack
to parse the string in metastore code from StringLiteral. */
DateLiteral
    :
    KW_DATE? DateString { ExtractDate(getText()) != null }?
    ;

StringLiteral
    :
    ( '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '\"' ( ~('\"'|'\\') | ('\\' .) )* '\"'
    )
    ;

IntegralLiteral
    :
    ('-')? (Digit)+
    ;

Identifier
    :
    (Letter | Digit) (Letter | Digit | '_')*
    ;

WS  :   (' '|'\r'|'\t'|'\n')+ { skip(); } ;
