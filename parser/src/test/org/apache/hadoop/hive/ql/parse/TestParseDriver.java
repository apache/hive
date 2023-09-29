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
package org.apache.hadoop.hive.ql.parse;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.charset.Charset;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.google.common.io.Files;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestParseDriver {
  ParseDriver parseDriver = new ParseDriver();

  @Test
  public void atFirstWarmup() throws Exception {
    // this test method is here to do an initial call to parsedriver; and prevent any tests with timeouts to be the first.
    parseDriver.parse("select 1");
  }

  @Test
  public void testParse() throws Exception {
    String selectStr = "select field1, field2, sum(field3+field4)";
    String whereStr = "field5=1 and field6 in ('a', 'b')";
    String havingStr = "sum(field7) > 11";
    ASTNode tree = parseDriver.parse(selectStr + " from table1 where " + whereStr
        + " group by field1, field2 having  " + havingStr).getTree();
    assertEquals(tree.getType(), 0);
    assertEquals(tree.getChildCount(), 2);
    ASTNode queryTree = (ASTNode) tree.getChild(0);
    assertEquals(tree.getChild(1).getType(), HiveParser.EOF);
    assertEquals(queryTree.getChildCount(), 2);
    ASTNode fromAST = (ASTNode) queryTree.getChild(0);
    ASTNode insertAST = (ASTNode) queryTree.getChild(1);
    assertEquals(fromAST.getType(), HiveParser.TOK_FROM);
    assertEquals(fromAST.getChildCount(), 1);
    assertEquals(fromAST.getChild(0).getType(), HiveParser.TOK_TABREF);
    assertEquals(fromAST.getChild(0).getChildCount(), 1);
    assertEquals(fromAST.getChild(0).getChild(0).getType(), HiveParser.TOK_TABNAME);
    assertEquals(fromAST.getChild(0).getChild(0).getChildCount(), 1);
    assertEquals(fromAST.getChild(0).getChild(0).getChild(0).getType(), HiveParser.Identifier);
    assertEquals(fromAST.getChild(0).getChild(0).getChild(0).getText(), "table1");
    assertEquals(insertAST.getChildCount(), 5);
    assertEquals(insertAST.getChild(0).getType(), HiveParser.TOK_DESTINATION);
    assertTree((ASTNode) insertAST.getChild(1), parseDriver.parseSelect(selectStr, null).getTree());
    assertEquals(insertAST.getChild(2).getType(), HiveParser.TOK_WHERE);
    assertTree((ASTNode) insertAST.getChild(2).getChild(0), parseDriver.parseExpression(whereStr));
    assertEquals(insertAST.getChild(3).getType(), HiveParser.TOK_GROUPBY);
    assertEquals(insertAST.getChild(3).getChildCount(), 2);
    for (int i = 0; i < 2; i++) {
      assertEquals(insertAST.getChild(3).getChild(i).getType(), HiveParser.TOK_TABLE_OR_COL);
      assertEquals(insertAST.getChild(3).getChild(i).getChild(0).getType(), HiveParser.Identifier);
      assertEquals(insertAST.getChild(3).getChild(i).getChild(0).getText(), "field" + (i + 1));
    }
    assertEquals(insertAST.getChild(4).getType(), HiveParser.TOK_HAVING);
    assertEquals(insertAST.getChild(4).getChildCount(), 1);
    assertTree((ASTNode) insertAST.getChild(4).getChild(0), parseDriver.parseExpression(havingStr));
  }

  @Test
  public void testParseSelect() throws Exception {
    ASTNode tree = parseDriver.parseSelect("select field1, field2, sum(field3+field4)", null).getTree();
    assertEquals(tree.getType(), HiveParser.TOK_SELECT);
    assertEquals(tree.getChildCount(), 3);
    for (int i = 0; i < 3; i++) {
      assertEquals(((ASTNode) tree.getChild(i)).getType(), HiveParser.TOK_SELEXPR);
    }
    assertTree((ASTNode) tree.getChild(0).getChild(0), parseDriver.parseExpression("field1"));
    assertTree((ASTNode) tree.getChild(1).getChild(0), parseDriver.parseExpression("field2"));
    assertTree((ASTNode) tree.getChild(2).getChild(0), parseDriver.parseExpression("sum(field3+field4)"));
  }

  @Test
  public void testParseExpression() throws Exception {
    ASTNode plusNode = parseDriver.parseExpression("field3 + field4");
    assertEquals(plusNode.getType(), HiveParser.PLUS);
    assertEquals(plusNode.getChildCount(), 2);
    for (int i = 0; i < 2; i++) {
      assertEquals(plusNode.getChild(i).getType(), HiveParser.TOK_TABLE_OR_COL);
      assertEquals(plusNode.getChild(i).getChildCount(), 1);
      assertEquals(plusNode.getChild(i).getChild(0).getType(), HiveParser.Identifier);
      assertEquals(plusNode.getChild(i).getChild(0).getText(), "field" + (i + 3));
    }

    ASTNode sumNode = parseDriver.parseExpression("sum(field3 + field4)");
    assertEquals(sumNode.getType(), HiveParser.TOK_FUNCTION);
    assertEquals(sumNode.getChildCount(), 2);
    assertEquals(sumNode.getChild(0).getType(), HiveParser.Identifier);
    assertEquals(sumNode.getChild(0).getText(), "sum");
    assertTree((ASTNode) sumNode.getChild(1), plusNode);

    ASTNode tree = parseDriver.parseExpression("case when field1 = 1 then sum(field3 + field4) when field1 != 2 then " +
        "sum(field3-field4) else sum(field3 * field4) end");
    assertEquals(tree.getChildCount(), 6);
    assertEquals(tree.getChild(0).getType(), HiveParser.KW_WHEN);
    assertEquals(tree.getChild(1).getType(), HiveParser.EQUAL);
    assertTree((ASTNode) tree.getChild(2), sumNode);
    assertEquals(tree.getChild(3).getType(), HiveParser.NOTEQUAL);
    assertTree((ASTNode) tree.getChild(4), parseDriver.parseExpression("sum(field3-field4)"));
    assertTree((ASTNode) tree.getChild(5), parseDriver.parseExpression("sum(field3*field4)"));
  }

  private void assertTree(ASTNode astNode1, ASTNode astNode2) {
    assertEquals(astNode1.getType(), astNode2.getType());
    assertEquals(astNode1.getChildCount(), astNode2.getChildCount());
    for (int i = 0; i < astNode1.getChildCount(); i++) {
      assertTree((ASTNode) astNode1.getChild(i), (ASTNode) astNode2.getChild(i));
    }
  }

  @Test(timeout = 1000)
  public void testNestedFunctionCalls() throws Exception {
    // Expectation here is not to run into a timeout
    parseDriver.parse(
        "select greatest(1,greatest(1,greatest(1,greatest(1,greatest(1,greatest(1,greatest(1,"
            + "greatest(1,greatest(1,greatest(1,greatest(1,greatest(1,greatest(1,greatest(1,"
            + "greatest(1,greatest(1,(greatest(1,greatest(1,2)))))))))))))))))))");
  }

  @Test(timeout = 1000)
  public void testHIVE18624() throws Exception {
    // Expectation here is not to run into a timeout
    parseDriver.parse("EXPLAIN\n" +
        "SELECT DISTINCT\n" +
        "\n" +
        "\n" +
        "  IF(lower('a') <= lower('a')\n" +
        "  ,'a'\n" +
        "  ,IF(('a' IS NULL AND from_unixtime(UNIX_TIMESTAMP()) <= 'a')\n" +
        "  ,'a'\n" +
        "  ,IF(if('a' = 'a', TRUE, FALSE) = 1\n" +
        "  ,'a'\n" +
        "  ,IF(('a' = 1 and lower('a') NOT IN ('a', 'a')\n" +
        "       and lower(if('a' = 'a','a','a')) <= lower('a'))\n" +
        "      OR ('a' like 'a' OR 'a' like 'a')\n" +
        "      OR 'a' in ('a','a')\n" +
        "  ,'a'\n" +
        "  ,IF(if(lower('a') in ('a', 'a') and 'a'='a', TRUE, FALSE) = 1\n" +
        "  ,'a'\n" +
        "  ,IF('a'='a' and unix_timestamp(if('a' = 'a',cast('a' as string),coalesce('a',cast('a' as string),from_unixtime(unix_timestamp())))) <= unix_timestamp(concat_ws('a',cast(lower('a') as string),'00:00:00')) + 9*3600\n"
        +
        "  ,'a'\n" +
        "\n" +
        "  ,If(lower('a') <= lower('a')\n" +
        "      and if(lower('a') in ('a', 'a') and 'a'<>'a', TRUE, FALSE) <> 1\n" +
        "  ,'a'\n" +
        "  ,IF('a'=1 AND 'a'=1\n" +
        "  ,'a'\n" +
        "  ,IF('a' = 1 and COALESCE(cast('a' as int),0) = 0\n" +
        "  ,'a'\n" +
        "  ,IF('a' = 'a'\n" +
        "  ,'a'\n" +
        "\n" +
        "  ,If('a' = 'a' AND lower('a')>lower(if(lower('a')<1830,'a',cast(date_add('a',1) as timestamp)))\n" +
        "  ,'a'\n" +
        "\n" +
        "\n" +
        "\n" +
        "  ,IF('a' = 1\n" +
        "\n" +
        "  ,IF('a' in ('a', 'a') and ((unix_timestamp('a')-unix_timestamp('a')) / 60) > 30 and 'a' = 1\n" +
        "\n" +
        "\n" +
        "  ,'a', 'a')\n" +
        "\n" +
        "\n" +
        "  ,IF(if('a' = 'a', FALSE, TRUE ) = 1 AND 'a' IS NULL\n" +
        "  ,'a'\n" +
        "  ,IF('a' = 1 and 'a'>0\n" +
        "  , 'a'\n" +
        "\n" +
        "  ,IF('a' = 1 AND 'a' ='a'\n" +
        "  ,'a'\n" +
        "  ,IF('a' is not null and 'a' is not null and 'a' > 'a'\n" +
        "  ,'a'\n" +
        "  ,IF('a' = 1\n" +
        "  ,'a'\n" +
        "\n" +
        "  ,IF('a' = 'a'\n" +
        "  ,'a'\n" +
        "\n" +
        "  ,If('a' = 1\n" +
        "  ,'a'\n" +
        "  ,IF('a' = 1\n" +
        "  ,'a'\n" +
        "  ,IF('a' = 1\n" +
        "  ,'a'\n" +
        "\n" +
        "  ,IF('a' ='a' and 'a' ='a' and cast(unix_timestamp('a') as  int) + 93600 < cast(unix_timestamp()  as int)\n" +
        "  ,'a'\n" +
        "  ,IF('a' = 'a'\n" +
        "  ,'a'\n" +
        "  ,IF('a' = 'a' and 'a' in ('a','a','a')\n" +
        "  ,'a'\n" +
        "  ,IF('a' = 'a'\n" +
        "  ,'a','a'))\n" +
        "      )))))))))))))))))))))))\n" +
        "AS test_comp_exp");
  }

  static class ExoticQueryBuilder {
    StringBuilder sb = new StringBuilder();

    public void recursiveSJS(int depth) {
      sb.append("select ");
      addColumns(30);
      sb.append(" from \n");
      tablePart(depth);
      sb.append(" join \n");
      tablePart(depth);
      sb.append(" on ( ");
      wherePart(10);
      sb.append(" ) ");
      sb.append(" where ");
      wherePart(10);

    }

    private void tablePart(int depth) {
      if (depth == 0) {
        sb.append(" baseTable ");
      } else {
        sb.append("(");
        recursiveSJS(depth - 1);
        sb.append(") aa");
      }
    }

    private void wherePart(int num) {
      for (int i = 0; i < num - 1; i++) {
        sb.append("x = ");
        sb.append(i);
        sb.append(" or ");
      }
      sb.append("x = -1");

    }

    private void addColumns(int num) {
      for (int i = 0; i < num - 1; i++) {
        sb.append("c");
        sb.append(i);
        sb.append(" + 2*sqrt(11)+");
        sb.append(i);
        sb.append(",");
      }
      sb.append("cE");
    }

    public String getQuery() {
      return sb.toString();
    }
  }

  @org.junit.Ignore("HIVE-26083")
  @Test(timeout = 10000)
  public void testExoticSJSSubQuery() throws Exception {
    ExoticQueryBuilder eqb = new ExoticQueryBuilder();
    eqb.recursiveSJS(10);
    String q = eqb.getQuery();
    System.out.println(q);
    parseDriver.parse(q);
  }

  @Test
  public void testJoinResulInBraces() throws Exception {
    String q =
        "explain select a.key, b.value from"
            + "( (select key from src)a join (select value from src)b on a.key=b.value)";
    System.out.println(q);

    ASTNode root = parseDriver.parse(q).getTree();
    System.out.println(root.dump());

  }

  @Test
  public void testFromSubqueryIsSetop() throws Exception {
    String q =
        "explain select key from ((select key from src) union (select key from src))subq ";
    System.out.println(q);

    ASTNode root = parseDriver.parse(q).getTree();
    System.out.println(root.dump());

  }

  @Test
  public void testSubQueryWithSetOpSupportsOrderBy() throws Exception {
    String q = "SELECT a FROM ((SELECT a FROM t1 ORDER BY a) UNION ALL (SELECT a FROM t2 DISTRIBUTE BY a)) B";
    System.out.println(q);

    ASTNode root = parseDriver.parse(q).getTree();
    System.out.println(root.dump());
  }

  @Test
  public void testParseCreateScheduledQuery() throws Exception {
    parseDriver.parse("create scheduled query asd cron '123' as select 1");
    parseDriver.parse("create scheduled query asd cron '123' executed as 'x' as select 1");
    parseDriver.parse("create scheduled query asd cron '123' executed as 'x' defined as select 1");
    parseDriver.parse("create scheduled query asd cron '123' executed as 'x' disabled defined as select 1");
  }

  @Test
  public void testParseAlterScheduledQuery() throws Exception {
    parseDriver.parse("alter scheduled query asd cron '123'");
    parseDriver.parse("alter scheduled query asd disabled");
    parseDriver.parse("alter scheduled query asd defined as select 22");
    parseDriver.parse("alter scheduled query asd executed as 'joe'");
  }

  @Test
  public void testParseDropScheduledQuery() throws Exception {
    parseDriver.parse("drop scheduled query asd");
  }

}
