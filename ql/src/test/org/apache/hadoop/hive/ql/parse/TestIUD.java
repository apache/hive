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
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * various Parser tests for INSERT/UPDATE/DELETE
 */
public class TestIUD {
  private static HiveConf conf;

  private ParseDriver pd;

  @BeforeClass
  public static void initialize() {
    conf = new HiveConf(SemanticAnalyzer.class);
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
  public void testDeleteNoWhere() throws ParseException {
    ASTNode ast = parse("DELETE FROM src");
    Assert.assertEquals("AST doesn't match", 
      "(TOK_DELETE_FROM " +
        "(TOK_TABNAME src))", ast.toStringTree());
  }
  @Test
  public void testDeleteWithWhere() throws ParseException {
    ASTNode ast = parse("DELETE FROM src WHERE key IS NOT NULL AND src.value < 0");
    Assert.assertEquals("AST doesn't match", 
      "(TOK_DELETE_FROM " +
        "(TOK_TABNAME src) " +
        "(TOK_WHERE " +
          "(AND " +
            "(TOK_FUNCTION TOK_ISNOTNULL (TOK_TABLE_OR_COL key)) " +
            "(< (. (TOK_TABLE_OR_COL src) value) 0))))",
      ast.toStringTree());
  }
  @Test
  public void testUpdateNoWhereSingleSet() throws ParseException {
    ASTNode ast = parse("UPDATE src set key = 3");
    Assert.assertEquals("AST doesn't match",
      "(TOK_UPDATE_TABLE " +
        "(TOK_TABNAME src) " +
        "(TOK_SET_COLUMNS_CLAUSE " +
          "(= " +
            "(TOK_TABLE_OR_COL key) 3)))",
      ast.toStringTree());
  }
  @Test
  public void testUpdateNoWhereMultiSet() throws ParseException {
    ASTNode ast = parse("UPDATE src set key = 3, value = 8");
    Assert.assertEquals("AST doesn't match", 
      "(TOK_UPDATE_TABLE " +
        "(TOK_TABNAME src) " +
        "(TOK_SET_COLUMNS_CLAUSE " +
          "(= " +
            "(TOK_TABLE_OR_COL key) 3) " +
          "(= " +
            "(TOK_TABLE_OR_COL value) 8)))",
      ast.toStringTree());
  }
  @Test
  public void testUpdateWithWhereSingleSet() throws ParseException {
    ASTNode ast = parse("UPDATE src SET key = 3 WHERE value IS NULL");
    Assert.assertEquals("AST doesn't match",
      "(TOK_UPDATE_TABLE " +
        "(TOK_TABNAME src) " +
        "(TOK_SET_COLUMNS_CLAUSE " +
          "(= " +
            "(TOK_TABLE_OR_COL key) 3)) " +
        "(TOK_WHERE (TOK_FUNCTION TOK_ISNULL (TOK_TABLE_OR_COL value))))",
      ast.toStringTree());
  }
  @Test
  public void testUpdateWithWhereSingleSetExpr() throws ParseException {
    ASTNode ast = parse("UPDATE src SET key = -3+(5*9)%8, val = cast(6.1 + c as INT), d = d - 1 WHERE value IS NULL");
    Assert.assertEquals("AST doesn't match",
      "(TOK_UPDATE_TABLE (TOK_TABNAME src) " +
        "(TOK_SET_COLUMNS_CLAUSE " +
        "(= (TOK_TABLE_OR_COL key) (+ (- 3) (% (* 5 9) 8))) " +
        "(= (TOK_TABLE_OR_COL val) (TOK_FUNCTION TOK_INT (+ 6.1 (TOK_TABLE_OR_COL c)))) " +
        "(= (TOK_TABLE_OR_COL d) (- (TOK_TABLE_OR_COL d) 1))) " +
        "(TOK_WHERE (TOK_FUNCTION TOK_ISNULL (TOK_TABLE_OR_COL value))))",
      ast.toStringTree());
  }
  @Test
  public void testUpdateWithWhereMultiSet() throws ParseException {
    ASTNode ast = parse("UPDATE src SET key = 3, value = 8 WHERE VALUE = 1230997");
    Assert.assertEquals("AST doesn't match", 
      "(TOK_UPDATE_TABLE " +
        "(TOK_TABNAME src) " +
        "(TOK_SET_COLUMNS_CLAUSE " +
        "(= " +
          "(TOK_TABLE_OR_COL key) 3) " +
        "(= " +
          "(TOK_TABLE_OR_COL value) 8)) " +
        "(TOK_WHERE (= (TOK_TABLE_OR_COL VALUE) 1230997)))",
      ast.toStringTree());
  }
  @Test
  public void testStandardInsertIntoTable() throws ParseException {
    ASTNode ast = parse("INSERT into TABLE page_view SELECT pvs.viewTime, pvs.userid from page_view_stg pvs where pvs.userid is null");
    Assert.assertEquals("AST doesn't match",
      "(TOK_QUERY " +
        "(TOK_FROM " +
          "(TOK_TABREF (TOK_TABNAME page_view_stg) pvs)) " +
        "(TOK_INSERT (TOK_INSERT_INTO (TOK_TAB (TOK_TABNAME page_view))) " +
        "(TOK_SELECT " +
          "(TOK_SELEXPR (. (TOK_TABLE_OR_COL pvs) viewTime)) " +
          "(TOK_SELEXPR (. (TOK_TABLE_OR_COL pvs) userid))) " +
        "(TOK_WHERE (TOK_FUNCTION TOK_ISNULL (. (TOK_TABLE_OR_COL pvs) userid)))))",
      ast.toStringTree());
  }
  @Test
  public void testSelectStarFromAnonymousVirtTable1Row() throws ParseException {
    try {
      parse("select * from `values` (3,4)");
      Assert.assertFalse("Expected ParseException", true);
    }
    catch(ParseException ex) {
      Assert.assertEquals("Failure didn't match.", "line 1:23 missing EOF at '(' near 'values'",ex.getMessage());
    }
  }
  @Test
  public void testSelectStarFromVirtTable1Row() throws ParseException {
    ASTNode ast = parse("select * from (values (3,4)) as VC(a,b)");
    Assert.assertEquals("AST doesn't match",
      "(TOK_QUERY " +
        "(TOK_FROM " +
          "(TOK_VIRTUAL_TABLE " +
            "(TOK_VIRTUAL_TABREF (TOK_TABNAME VC) (TOK_COL_NAME a b)) " +
            "(TOK_VALUES_TABLE (TOK_VALUE_ROW 3 4)))) " +
        "(TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR TOK_ALLCOLREF))))",
      ast.toStringTree());
  }
  @Test
  public void testSelectStarFromVirtTable2Row() throws ParseException {
    ASTNode ast = parse("select * from (values (1,2),(3,4)) as VC(a,b)");
    Assert.assertEquals("AST doesn't match",
      "(TOK_QUERY " +
        "(TOK_FROM " +
          "(TOK_VIRTUAL_TABLE " +
            "(TOK_VIRTUAL_TABREF (TOK_TABNAME VC) (TOK_COL_NAME a b)) " +
            "(TOK_VALUES_TABLE (TOK_VALUE_ROW 1 2) (TOK_VALUE_ROW 3 4)))) " +
        "(TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR TOK_ALLCOLREF))))",
      ast.toStringTree());
  }
  @Test
  public void testSelectStarFromVirtTable2RowNamedProjections() throws ParseException {
    ASTNode ast = parse("select a as c, b as d from (values (1,2),(3,4)) as VC(a,b)");
    Assert.assertEquals("AST doesn't match",
      "(TOK_QUERY " +
        "(TOK_FROM " +
        "(TOK_VIRTUAL_TABLE " +
          "(TOK_VIRTUAL_TABREF (TOK_TABNAME VC) (TOK_COL_NAME a b)) " +
          "(TOK_VALUES_TABLE (TOK_VALUE_ROW 1 2) (TOK_VALUE_ROW 3 4)))) " +
        "(TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) " +
          "(TOK_SELECT (TOK_SELEXPR (TOK_TABLE_OR_COL a) c) (TOK_SELEXPR (TOK_TABLE_OR_COL b) d))))",
      ast.toStringTree());
  }
  @Test
  public void testInsertIntoTableAsSelectFromNamedVirtTable() throws ParseException {
    ASTNode ast = parse("insert into page_view select a,b as c from (values (1,2),(3,4)) as VC(a,b) where b = 9");
    Assert.assertEquals("AST doesn't match",
      "(TOK_QUERY " +
        "(TOK_FROM " +
          "(TOK_VIRTUAL_TABLE " +
            "(TOK_VIRTUAL_TABREF (TOK_TABNAME VC) (TOK_COL_NAME a b)) " +
            "(TOK_VALUES_TABLE (TOK_VALUE_ROW 1 2) (TOK_VALUE_ROW 3 4)))) " +
        "(TOK_INSERT (TOK_INSERT_INTO (TOK_TAB (TOK_TABNAME page_view))) " +
          "(TOK_SELECT " +
            "(TOK_SELEXPR (TOK_TABLE_OR_COL a)) " +
            "(TOK_SELEXPR (TOK_TABLE_OR_COL b) c)) " +
          "(TOK_WHERE (= (TOK_TABLE_OR_COL b) 9))))",
      ast.toStringTree());
  }
  /**
   * same as testInsertIntoTableAsSelectFromNamedVirtTable but with column list on target table
   * @throws ParseException
   */
  @Test
  public void testInsertIntoTableAsSelectFromNamedVirtTableNamedCol() throws ParseException {
    ASTNode ast = parse("insert into page_view(c1,c2) select a,b as c from (values (1,2),(3,4)) as VC(a,b) where b = 9");
    Assert.assertEquals("AST doesn't match",
      "(TOK_QUERY " +
        "(TOK_FROM " +
        "(TOK_VIRTUAL_TABLE " +
        "(TOK_VIRTUAL_TABREF (TOK_TABNAME VC) (TOK_COL_NAME a b)) " +
        "(TOK_VALUES_TABLE (TOK_VALUE_ROW 1 2) (TOK_VALUE_ROW 3 4)))) " +
        "(TOK_INSERT (TOK_INSERT_INTO (TOK_TAB (TOK_TABNAME page_view)) (TOK_TABCOLNAME c1 c2)) " +
        "(TOK_SELECT " +
        "(TOK_SELEXPR (TOK_TABLE_OR_COL a)) " +
        "(TOK_SELEXPR (TOK_TABLE_OR_COL b) c)) " +
        "(TOK_WHERE (= (TOK_TABLE_OR_COL b) 9))))",
      ast.toStringTree());
  }
  @Test
  public void testInsertIntoTableFromAnonymousTable1Row() throws ParseException {
    ASTNode ast = parse("insert into page_view values(1,2)");
    Assert.assertEquals("AST doesn't match",
      "(TOK_QUERY " +
        "(TOK_FROM " +
        "(TOK_VIRTUAL_TABLE " +
        "(TOK_VIRTUAL_TABREF TOK_ANONYMOUS) " +
        "(TOK_VALUES_TABLE (TOK_VALUE_ROW 1 2)))) " +
        "(TOK_INSERT (TOK_INSERT_INTO (TOK_TAB (TOK_TABNAME page_view))) " +
        "(TOK_SELECT (TOK_SELEXPR TOK_ALLCOLREF))))",
      ast.toStringTree());
  }
  /**
   * Same as testInsertIntoTableFromAnonymousTable1Row but with column list on target table
   * @throws ParseException
   */
  @Test
  public void testInsertIntoTableFromAnonymousTable1RowNamedCol() throws ParseException {
    ASTNode ast = parse("insert into page_view(a,b) values(1,2)");
    Assert.assertEquals("AST doesn't match",
      "(TOK_QUERY " +
        "(TOK_FROM " +
          "(TOK_VIRTUAL_TABLE " +
            "(TOK_VIRTUAL_TABREF TOK_ANONYMOUS) " +
            "(TOK_VALUES_TABLE (TOK_VALUE_ROW 1 2))" +
          ")" +
        ") " +
        "(TOK_INSERT " +
          "(TOK_INSERT_INTO " +
            "(TOK_TAB (TOK_TABNAME page_view)) " +
            "(TOK_TABCOLNAME a b)" +//this is "extra" piece we get vs previous query
          ") " +
          "(TOK_SELECT " +
            "(TOK_SELEXPR TOK_ALLCOLREF)" +
          ")" +
        ")" +
      ")", ast.toStringTree());
  }
  @Test
  public void testInsertIntoTableFromAnonymousTable() throws ParseException {
    ASTNode ast = parse("insert into table page_view values(-1,2),(3,+4)");
    Assert.assertEquals("AST doesn't match",
      "(TOK_QUERY " +
        "(TOK_FROM " +
          "(TOK_VIRTUAL_TABLE " +
          "(TOK_VIRTUAL_TABREF TOK_ANONYMOUS) " +
          "(TOK_VALUES_TABLE (TOK_VALUE_ROW (- 1) 2) (TOK_VALUE_ROW 3 (+ 4))))) " +
        "(TOK_INSERT (TOK_INSERT_INTO (TOK_TAB (TOK_TABNAME page_view))) " +
          "(TOK_SELECT (TOK_SELEXPR TOK_ALLCOLREF))))",
      ast.toStringTree());
    //same query as above less the "table" keyword KW_TABLE
    ast = parse("insert into page_view values(-1,2),(3,+4)");
    Assert.assertEquals("AST doesn't match",
      "(TOK_QUERY " +
        "(TOK_FROM " +
        "(TOK_VIRTUAL_TABLE " +
        "(TOK_VIRTUAL_TABREF TOK_ANONYMOUS) " +
        "(TOK_VALUES_TABLE (TOK_VALUE_ROW (- 1) 2) (TOK_VALUE_ROW 3 (+ 4))))) " +
        "(TOK_INSERT (TOK_INSERT_INTO (TOK_TAB (TOK_TABNAME page_view))) " +
        "(TOK_SELECT (TOK_SELEXPR TOK_ALLCOLREF))))",
      ast.toStringTree());
  }
  @Test
  public void testMultiInsert() throws ParseException {
    ASTNode ast = parse("from S insert into T1 select a, b insert into T2 select c, d");
    Assert.assertEquals("AST doesn't match", "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME S))) " +
      "(TOK_INSERT (TOK_INSERT_INTO (TOK_TAB (TOK_TABNAME T1))) (TOK_SELECT (TOK_SELEXPR (TOK_TABLE_OR_COL a)) (TOK_SELEXPR (TOK_TABLE_OR_COL b)))) " +
      "(TOK_INSERT (TOK_INSERT_INTO (TOK_TAB (TOK_TABNAME T2))) (TOK_SELECT (TOK_SELEXPR (TOK_TABLE_OR_COL c)) (TOK_SELEXPR (TOK_TABLE_OR_COL d)))))", ast.toStringTree());
  }
}
