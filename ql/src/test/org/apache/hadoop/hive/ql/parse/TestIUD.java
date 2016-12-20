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
    return parse(query, pd, conf);
  }
  static ASTNode parse(String query, ParseDriver pd, HiveConf conf) throws ParseException {
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
      "(tok_delete_from " +
        "(tok_tabname src))", ast.toStringTree());
  }
  @Test
  public void testDeleteWithWhere() throws ParseException {
    ASTNode ast = parse("DELETE FROM src WHERE key IS NOT NULL AND src.value < 0");
    Assert.assertEquals("AST doesn't match",
      "(tok_delete_from " +
        "(tok_tabname src) " +
        "(tok_where " +
          "(and " +
            "(tok_function tok_isnotnull (tok_table_or_col key)) " +
            "(< (. (tok_table_or_col src) value) 0))))",
      ast.toStringTree());
  }
  @Test
  public void testUpdateNoWhereSingleSet() throws ParseException {
    ASTNode ast = parse("UPDATE src set key = 3");
    Assert.assertEquals("AST doesn't match",
      "(tok_update_table " +
        "(tok_tabname src) " +
        "(tok_set_columns_clause " +
          "(= " +
            "(tok_table_or_col key) 3)))",
      ast.toStringTree());
  }
  @Test
  public void testUpdateNoWhereMultiSet() throws ParseException {
    ASTNode ast = parse("UPDATE src set key = 3, value = 8");
    Assert.assertEquals("AST doesn't match",
      "(tok_update_table " +
        "(tok_tabname src) " +
        "(tok_set_columns_clause " +
          "(= " +
            "(tok_table_or_col key) 3) " +
          "(= " +
            "(tok_table_or_col value) 8)))",
      ast.toStringTree());
  }
  @Test
  public void testUpdateWithWhereSingleSet() throws ParseException {
    ASTNode ast = parse("UPDATE src SET key = 3 WHERE value IS NULL");
    Assert.assertEquals("AST doesn't match",
      "(tok_update_table " +
        "(tok_tabname src) " +
        "(tok_set_columns_clause " +
          "(= " +
            "(tok_table_or_col key) 3)) " +
        "(tok_where (tok_function tok_isnull (tok_table_or_col value))))",
      ast.toStringTree());
  }
  @Test
  public void testUpdateWithWhereSingleSetExpr() throws ParseException {
    ASTNode ast = parse("UPDATE src SET key = -3+(5*9)%8, val = cast(6.1 + c as INT), d = d - 1 WHERE value IS NULL");
    Assert.assertEquals("AST doesn't match",
      "(tok_update_table (tok_tabname src) " +
        "(tok_set_columns_clause " +
        "(= (tok_table_or_col key) (+ (- 3) (% (* 5 9) 8))) " +
        "(= (tok_table_or_col val) (tok_function tok_int (+ 6.1 (tok_table_or_col c)))) " +
        "(= (tok_table_or_col d) (- (tok_table_or_col d) 1))) " +
        "(tok_where (tok_function tok_isnull (tok_table_or_col value))))",
      ast.toStringTree());
  }
  @Test
  public void testUpdateWithWhereMultiSet() throws ParseException {
    ASTNode ast = parse("UPDATE src SET key = 3, value = 8 WHERE VALUE = 1230997");
    Assert.assertEquals("AST doesn't match",
      "(tok_update_table " +
        "(tok_tabname src) " +
        "(tok_set_columns_clause " +
        "(= " +
          "(tok_table_or_col key) 3) " +
        "(= " +
          "(tok_table_or_col value) 8)) " +
        "(tok_where (= (tok_table_or_col value) 1230997)))",
      ast.toStringTree());
  }
  @Test
  public void testStandardInsertIntoTable() throws ParseException {
    ASTNode ast = parse("INSERT into TABLE page_view SELECT pvs.viewTime, pvs.userid from page_view_stg pvs where pvs.userid is null");
    Assert.assertEquals("AST doesn't match",
      "(tok_query " +
        "(tok_from " +
          "(tok_tabref (tok_tabname page_view_stg) pvs)) " +
        "(tok_insert (tok_insert_into (tok_tab (tok_tabname page_view))) " +
        "(tok_select " +
          "(tok_selexpr (. (tok_table_or_col pvs) viewtime)) " +
          "(tok_selexpr (. (tok_table_or_col pvs) userid))) " +
        "(tok_where (tok_function tok_isnull (. (tok_table_or_col pvs) userid)))))",
      ast.toStringTree());
  }
  @Test
  public void testSelectStarFromAnonymousVirtTable1Row() throws ParseException {
    try {
      parse("select * from `values` (3,4)");
      Assert.assertFalse("Expected ParseException", true);
    }
    catch(ParseException ex) {
      Assert.assertEquals("Failure didn't match.",
          "line 1:24 cannot recognize input near 'values' '(' '3' in joinSource",
          ex.getMessage());
    }
  }
  @Test
  public void testSelectStarFromVirtTable1Row() throws ParseException {
    ASTNode ast = parse("select * from (values (3,4)) as vc(a,b)");
    Assert.assertEquals("AST doesn't match",
      "(tok_query " +
        "(tok_from " +
          "(tok_virtual_table " +
            "(tok_virtual_tabref (tok_tabname vc) (tok_col_name a b)) " +
            "(tok_values_table (tok_value_row 3 4)))) " +
        "(tok_insert (tok_destination (tok_dir tok_tmp_file)) (tok_select (tok_selexpr tok_allcolref))))",
      ast.toStringTree());
  }
  @Test
  public void testSelectStarFromVirtTable2Row() throws ParseException {
    ASTNode ast = parse("select * from (values (1,2),(3,4)) as vc(a,b)");
    Assert.assertEquals("AST doesn't match",
      "(tok_query " +
        "(tok_from " +
          "(tok_virtual_table " +
            "(tok_virtual_tabref (tok_tabname vc) (tok_col_name a b)) " +
            "(tok_values_table (tok_value_row 1 2) (tok_value_row 3 4)))) " +
        "(tok_insert (tok_destination (tok_dir tok_tmp_file)) (tok_select (tok_selexpr tok_allcolref))))",
      ast.toStringTree());
  }
  @Test
  public void testSelectStarFromVirtTable2RowNamedProjections() throws ParseException {
    ASTNode ast = parse("select a as c, b as d from (values (1,2),(3,4)) as vc(a,b)");
    Assert.assertEquals("AST doesn't match",
      "(tok_query " +
        "(tok_from " +
        "(tok_virtual_table " +
          "(tok_virtual_tabref (tok_tabname vc) (tok_col_name a b)) " +
          "(tok_values_table (tok_value_row 1 2) (tok_value_row 3 4)))) " +
        "(tok_insert (tok_destination (tok_dir tok_tmp_file)) " +
          "(tok_select (tok_selexpr (tok_table_or_col a) c) (tok_selexpr (tok_table_or_col b) d))))",
      ast.toStringTree());
  }
  @Test
  public void testInsertIntoTableAsSelectFromNamedVirtTable() throws ParseException {
    ASTNode ast = parse("insert into page_view select a,b as c from (values (1,2),(3,4)) as vc(a,b) where b = 9");
    Assert.assertEquals("AST doesn't match",
      "(tok_query " +
        "(tok_from " +
          "(tok_virtual_table " +
            "(tok_virtual_tabref (tok_tabname vc) (tok_col_name a b)) " +
            "(tok_values_table (tok_value_row 1 2) (tok_value_row 3 4)))) " +
        "(tok_insert (tok_insert_into (tok_tab (tok_tabname page_view))) " +
          "(tok_select " +
            "(tok_selexpr (tok_table_or_col a)) " +
            "(tok_selexpr (tok_table_or_col b) c)) " +
          "(tok_where (= (tok_table_or_col b) 9))))",
      ast.toStringTree());
  }
  /**
   * same as testInsertIntoTableAsSelectFromNamedVirtTable but with column list on target table
   * @throws ParseException
   */
  @Test
  public void testInsertIntoTableAsSelectFromNamedVirtTableNamedCol() throws ParseException {
    ASTNode ast = parse("insert into page_view(c1,c2) select a,b as c from (values (1,2),(3,4)) as vc(a,b) where b = 9");
    Assert.assertEquals("AST doesn't match",
      "(tok_query " +
        "(tok_from " +
        "(tok_virtual_table " +
        "(tok_virtual_tabref (tok_tabname vc) (tok_col_name a b)) " +
        "(tok_values_table (tok_value_row 1 2) (tok_value_row 3 4)))) " +
        "(tok_insert (tok_insert_into (tok_tab (tok_tabname page_view)) (tok_tabcolname c1 c2)) " +
        "(tok_select " +
        "(tok_selexpr (tok_table_or_col a)) " +
        "(tok_selexpr (tok_table_or_col b) c)) " +
        "(tok_where (= (tok_table_or_col b) 9))))",
      ast.toStringTree());
  }
  @Test
  public void testInsertIntoTableFromAnonymousTable1Row() throws ParseException {
    ASTNode ast = parse("insert into page_view values(1,2)");
    Assert.assertEquals("AST doesn't match",
      "(tok_query " +
        "(tok_from " +
        "(tok_virtual_table " +
        "(tok_virtual_tabref tok_anonymous) " +
        "(tok_values_table (tok_value_row 1 2)))) " +
        "(tok_insert (tok_insert_into (tok_tab (tok_tabname page_view))) " +
        "(tok_select (tok_selexpr tok_allcolref))))",
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
      "(tok_query " +
        "(tok_from " +
          "(tok_virtual_table " +
            "(tok_virtual_tabref tok_anonymous) " +
            "(tok_values_table (tok_value_row 1 2))" +
          ")" +
        ") " +
        "(tok_insert " +
          "(tok_insert_into " +
            "(tok_tab (tok_tabname page_view)) " +
            "(tok_tabcolname a b)" +//this is "extra" piece we get vs previous query
          ") " +
          "(tok_select " +
            "(tok_selexpr tok_allcolref)" +
          ")" +
        ")" +
      ")", ast.toStringTree());
  }
  @Test
  public void testInsertIntoTableFromAnonymousTable() throws ParseException {
    ASTNode ast = parse("insert into table page_view values(-1,2),(3,+4)");
    Assert.assertEquals("AST doesn't match",
      "(tok_query " +
        "(tok_from " +
          "(tok_virtual_table " +
          "(tok_virtual_tabref tok_anonymous) " +
          "(tok_values_table (tok_value_row (- 1) 2) (tok_value_row 3 (+ 4))))) " +
        "(tok_insert (tok_insert_into (tok_tab (tok_tabname page_view))) " +
          "(tok_select (tok_selexpr tok_allcolref))))",
      ast.toStringTree());
    //same query as above less the "table" keyword KW_table
    ast = parse("insert into page_view values(-1,2),(3,+4)");
    Assert.assertEquals("AST doesn't match",
      "(tok_query " +
        "(tok_from " +
        "(tok_virtual_table " +
        "(tok_virtual_tabref tok_anonymous) " +
        "(tok_values_table (tok_value_row (- 1) 2) (tok_value_row 3 (+ 4))))) " +
        "(tok_insert (tok_insert_into (tok_tab (tok_tabname page_view))) " +
        "(tok_select (tok_selexpr tok_allcolref))))",
      ast.toStringTree());
  }
  @Test
  public void testMultiInsert() throws ParseException {
    ASTNode ast = parse("from S insert into T1 select a, b insert into T2 select c, d");
    Assert.assertEquals("AST doesn't match", "(tok_query (tok_from (tok_tabref (tok_tabname s))) " +
      "(tok_insert (tok_insert_into (tok_tab (tok_tabname t1))) (tok_select (tok_selexpr (tok_table_or_col a)) (tok_selexpr (tok_table_or_col b)))) " +
      "(tok_insert (tok_insert_into (tok_tab (tok_tabname t2))) (tok_select (tok_selexpr (tok_table_or_col c)) (tok_selexpr (tok_table_or_col d)))))", ast.toStringTree());
  }
}
