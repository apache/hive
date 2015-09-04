/**
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

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Parser tests for SQL11 Reserved KeyWords. Please find more information in
 * HIVE-6617. Total number : 74 + 2 (MySQL)
 */
public class TestSQL11ReservedKeyWordsPositive {
  private static HiveConf conf;

  private ParseDriver pd;

  @BeforeClass
  public static void initialize() {
    conf = new HiveConf(SemanticAnalyzer.class);
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_SQL11_RESERVED_KEYWORDS, false);
    SessionState.start(conf);
  }

  @Before
  public void setup() throws SemanticException, IOException {
    pd = new ParseDriver();
  }

  ASTNode parse(String query) throws ParseException {
    ASTNode nd = null;
    try {
      nd = pd.parse(query, new Context(conf));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return (ASTNode) nd.getChild(0);
  }

  @Test
  public void testSQL11ReservedKeyWords_ALL() throws ParseException {
    ASTNode ast = parse("CREATE TABLE ALL (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME ALL) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_ALTER() throws ParseException {
    ASTNode ast = parse("CREATE TABLE ALTER (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME ALTER) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_ARRAY() throws ParseException {
    ASTNode ast = parse("CREATE TABLE ARRAY (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME ARRAY) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_AS() throws ParseException {
    ASTNode ast = parse("CREATE TABLE AS (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME AS) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_AUTHORIZATION() throws ParseException {
    ASTNode ast = parse("CREATE TABLE AUTHORIZATION (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME AUTHORIZATION) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_BETWEEN() throws ParseException {
    ASTNode ast = parse("CREATE TABLE BETWEEN (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME BETWEEN) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_BIGINT() throws ParseException {
    ASTNode ast = parse("CREATE TABLE BIGINT (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME BIGINT) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_BINARY() throws ParseException {
    ASTNode ast = parse("CREATE TABLE BINARY (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME BINARY) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_BOOLEAN() throws ParseException {
    ASTNode ast = parse("CREATE TABLE BOOLEAN (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME BOOLEAN) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_BOTH() throws ParseException {
    ASTNode ast = parse("CREATE TABLE BOTH (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME BOTH) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_BY() throws ParseException {
    ASTNode ast = parse("CREATE TABLE BY (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME BY) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_CREATE() throws ParseException {
    ASTNode ast = parse("CREATE TABLE CREATE (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME CREATE) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_CUBE() throws ParseException {
    ASTNode ast = parse("CREATE TABLE CUBE (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME CUBE) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_CURRENT_DATE() throws ParseException {
    ASTNode ast = parse("CREATE TABLE CURRENT_DATE (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME CURRENT_DATE) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_CURRENT_TIMESTAMP() throws ParseException {
    ASTNode ast = parse("CREATE TABLE CURRENT_TIMESTAMP (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME CURRENT_TIMESTAMP) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_CURSOR() throws ParseException {
    ASTNode ast = parse("CREATE TABLE CURSOR (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME CURSOR) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_DATE() throws ParseException {
    ASTNode ast = parse("CREATE TABLE DATE (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME DATE) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_DECIMAL() throws ParseException {
    ASTNode ast = parse("CREATE TABLE DECIMAL (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME DECIMAL) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_DELETE() throws ParseException {
    ASTNode ast = parse("CREATE TABLE DELETE (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME DELETE) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_DESCRIBE() throws ParseException {
    ASTNode ast = parse("CREATE TABLE DESCRIBE (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME DESCRIBE) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_DOUBLE() throws ParseException {
    ASTNode ast = parse("CREATE TABLE DOUBLE (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME DOUBLE) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_DROP() throws ParseException {
    ASTNode ast = parse("CREATE TABLE DROP (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME DROP) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_EXISTS() throws ParseException {
    ASTNode ast = parse("CREATE TABLE EXISTS (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME EXISTS) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_EXTERNAL() throws ParseException {
    ASTNode ast = parse("CREATE TABLE EXTERNAL (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME EXTERNAL) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_FALSE() throws ParseException {
    ASTNode ast = parse("CREATE TABLE FALSE (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME FALSE) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_FETCH() throws ParseException {
    ASTNode ast = parse("CREATE TABLE FETCH (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME FETCH) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_FLOAT() throws ParseException {
    ASTNode ast = parse("CREATE TABLE FLOAT (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME FLOAT) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_FOR() throws ParseException {
    ASTNode ast = parse("CREATE TABLE FOR (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME FOR) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_FULL() throws ParseException {
    ASTNode ast = parse("CREATE TABLE FULL (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME FULL) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_GRANT() throws ParseException {
    ASTNode ast = parse("CREATE TABLE GRANT (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME GRANT) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_GROUP() throws ParseException {
    ASTNode ast = parse("CREATE TABLE GROUP (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME GROUP) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_GROUPING() throws ParseException {
    ASTNode ast = parse("CREATE TABLE GROUPING (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME GROUPING) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_IMPORT() throws ParseException {
    ASTNode ast = parse("CREATE TABLE IMPORT (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME IMPORT) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_IN() throws ParseException {
    ASTNode ast = parse("CREATE TABLE IN (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME IN) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_INNER() throws ParseException {
    ASTNode ast = parse("CREATE TABLE INNER (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME INNER) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_INSERT() throws ParseException {
    ASTNode ast = parse("CREATE TABLE INSERT (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME INSERT) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_INT() throws ParseException {
    ASTNode ast = parse("CREATE TABLE INT (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME INT) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_INTERSECT() throws ParseException {
    ASTNode ast = parse("CREATE TABLE INTERSECT (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME INTERSECT) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_INTO() throws ParseException {
    ASTNode ast = parse("CREATE TABLE INTO (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME INTO) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_IS() throws ParseException {
    ASTNode ast = parse("CREATE TABLE IS (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME IS) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_LATERAL() throws ParseException {
    ASTNode ast = parse("CREATE TABLE LATERAL (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME LATERAL) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_LEFT() throws ParseException {
    ASTNode ast = parse("CREATE TABLE LEFT (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME LEFT) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_LIKE() throws ParseException {
    ASTNode ast = parse("CREATE TABLE LIKE (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME LIKE) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_LOCAL() throws ParseException {
    ASTNode ast = parse("CREATE TABLE LOCAL (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME LOCAL) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_NONE() throws ParseException {
    ASTNode ast = parse("CREATE TABLE NONE (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME NONE) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_NULL() throws ParseException {
    ASTNode ast = parse("CREATE TABLE NULL (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME NULL) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_OF() throws ParseException {
    ASTNode ast = parse("CREATE TABLE OF (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME OF) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_ORDER() throws ParseException {
    ASTNode ast = parse("CREATE TABLE ORDER (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME ORDER) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_OUT() throws ParseException {
    ASTNode ast = parse("CREATE TABLE OUT (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME OUT) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_OUTER() throws ParseException {
    ASTNode ast = parse("CREATE TABLE OUTER (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME OUTER) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_PARTITION() throws ParseException {
    ASTNode ast = parse("CREATE TABLE PARTITION (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME PARTITION) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_PERCENT() throws ParseException {
    ASTNode ast = parse("CREATE TABLE PERCENT (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME PERCENT) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_PROCEDURE() throws ParseException {
    ASTNode ast = parse("CREATE TABLE PROCEDURE (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME PROCEDURE) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_RANGE() throws ParseException {
    ASTNode ast = parse("CREATE TABLE RANGE (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME RANGE) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_READS() throws ParseException {
    ASTNode ast = parse("CREATE TABLE READS (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME READS) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_REVOKE() throws ParseException {
    ASTNode ast = parse("CREATE TABLE REVOKE (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME REVOKE) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_RIGHT() throws ParseException {
    ASTNode ast = parse("CREATE TABLE RIGHT (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME RIGHT) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_ROLLUP() throws ParseException {
    ASTNode ast = parse("CREATE TABLE ROLLUP (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME ROLLUP) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_ROW() throws ParseException {
    ASTNode ast = parse("CREATE TABLE ROW (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME ROW) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_ROWS() throws ParseException {
    ASTNode ast = parse("CREATE TABLE ROWS (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME ROWS) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_SET() throws ParseException {
    ASTNode ast = parse("CREATE TABLE SET (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME SET) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_SMALLINT() throws ParseException {
    ASTNode ast = parse("CREATE TABLE SMALLINT (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME SMALLINT) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_TABLE() throws ParseException {
    ASTNode ast = parse("CREATE TABLE TABLE (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME TABLE) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_TIMESTAMP() throws ParseException {
    ASTNode ast = parse("CREATE TABLE TIMESTAMP (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME TIMESTAMP) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_TO() throws ParseException {
    ASTNode ast = parse("CREATE TABLE TO (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME TO) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_TRIGGER() throws ParseException {
    ASTNode ast = parse("CREATE TABLE TRIGGER (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME TRIGGER) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_TRUE() throws ParseException {
    ASTNode ast = parse("CREATE TABLE TRUE (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME TRUE) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_TRUNCATE() throws ParseException {
    ASTNode ast = parse("CREATE TABLE TRUNCATE (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME TRUNCATE) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_UNION() throws ParseException {
    ASTNode ast = parse("CREATE TABLE UNION (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME UNION) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_UPDATE() throws ParseException {
    ASTNode ast = parse("CREATE TABLE UPDATE (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME UPDATE) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_USER() throws ParseException {
    ASTNode ast = parse("CREATE TABLE USER (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME USER) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_USING() throws ParseException {
    ASTNode ast = parse("CREATE TABLE USING (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME USING) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_VALUES() throws ParseException {
    ASTNode ast = parse("CREATE TABLE VALUES (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME VALUES) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_WITH() throws ParseException {
    ASTNode ast = parse("CREATE TABLE WITH (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME WITH) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  // MySQL reserved keywords.
  @Test
  public void testSQL11ReservedKeyWords_RLIKE() throws ParseException {
    ASTNode ast = parse("CREATE TABLE RLIKE (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME RLIKE) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }

  @Test
  public void testSQL11ReservedKeyWords_REGEXP() throws ParseException {
    ASTNode ast = parse("CREATE TABLE REGEXP (col STRING)");
    Assert
        .assertEquals(
            "AST doesn't match",
            "(TOK_CREATETABLE (TOK_TABNAME REGEXP) TOK_LIKETABLE (TOK_TABCOLLIST (TOK_TABCOL col TOK_STRING)))",
            ast.toStringTree());
  }
}
