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

import org.antlr.runtime.tree.RewriteEmptyStreamException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

/**
 * Testing parsing for SQL Merge statement
 */
public class TestMergeStatement {
  private static Configuration conf;
  private ParseDriver pd;

  @BeforeClass
  public static void initialize() {
    conf = new Configuration();
  }

  @Before
  public void setup() throws SemanticException, IOException {
    pd = new ParseDriver();
  }

  ASTNode parse(String query) throws ParseException {
    return TestIUD.parse(query, pd, conf);
  }
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void test() throws ParseException {
    ASTNode ast = parse(//using target.a breaks this
      "MERGE INTO target USING source ON target.pk = source.pk WHEN MATCHED THEN UPDATE set a = source.b, c=d+1");
    Assert.assertEquals(
      "(tok_merge " +
        "(tok_tabref (tok_tabname target)) " +
        "(tok_tabref (tok_tabname source)) " +
        "(= (. (tok_table_or_col target) pk) (. (tok_table_or_col source) pk)) " +
        "(tok_matched " +
          "(tok_update " +
            "(tok_set_columns_clause " +
              "(= (tok_table_or_col a) (. (tok_table_or_col source) b)) " +
              "(= (tok_table_or_col c) (+ (tok_table_or_col d) 1))" +
            ")" +
          ")" +
        ")" +
      ")", ast.toStringTree());
  }
  @Test
  public void test1() throws ParseException {
    //testing MATCHED AND with CASE statement
    ASTNode ast = parse(//using target.a breaks this
      "MERGE INTO target USING source ON target.pk = source.pk WHEN MATCHED " +
        "AND source.c2 < current_time() " +
        "THEN UPDATE set a = source.b, b = case when c1 is null then c1 else c1 end");
    Assert.assertEquals(
      "(tok_merge " +
        "(tok_tabref (tok_tabname target)) (tok_tabref (tok_tabname source)) (= (. (tok_table_or_col target) pk) (. (tok_table_or_col source) pk)) " +
          "(tok_matched " +
            "(tok_update " +
              "(tok_set_columns_clause " +
                "(= (tok_table_or_col a) (. (tok_table_or_col source) b)) " +
                "(= (tok_table_or_col b) (tok_function when (tok_function isnull (tok_table_or_col c1)) (tok_table_or_col c1) (tok_table_or_col c1)))" +
              ")" +
            ") " +
          "(< (. (tok_table_or_col source) c2) (tok_function current_time)))" +
        ")", ast.toStringTree());
  }
  @Test
  public void test2() throws ParseException {
    ASTNode
      ast = parse("MERGE INTO target USING source ON target.pk = source.pk WHEN MATCHED THEN DELETE");
    Assert.assertEquals(
      "(tok_merge " +
        "(tok_tabref (tok_tabname target)) (tok_tabref (tok_tabname source)) (= (. (tok_table_or_col target) pk) (. (tok_table_or_col source) pk)) " +
        "(tok_matched " +
        "tok_delete)" +
        ")", ast.toStringTree());
  }
  @Test
  public void test3() throws ParseException {
    ASTNode
      ast = parse("MERGE INTO target USING source ON target.pk = source.pk WHEN MATCHED AND target.a + source.b > 8 THEN DELETE");
    Assert.assertEquals(
      "(tok_merge " +
        "(tok_tabref (tok_tabname target)) (tok_tabref (tok_tabname source)) (= (. (tok_table_or_col target) pk) (. (tok_table_or_col source) pk)) " +
        "(tok_matched " +
        "tok_delete " +
        "(> (+ (. (tok_table_or_col target) a) (. (tok_table_or_col source) b)) 8))" +
        ")", ast.toStringTree());
  }
  @Test
  public void test4() throws ParseException {
    ASTNode
      ast = parse(
      "MERGE INTO target USING source ON target.pk = source.pk WHEN NOT MATCHED THEN INSERT VALUES(source.a, case when source.b is null then target.b else source.b end)");
    Assert.assertEquals(
      "(tok_merge " +
        "(tok_tabref (tok_tabname target)) (tok_tabref (tok_tabname source)) (= (. (tok_table_or_col target) pk) (. (tok_table_or_col source) pk)) " +
        "(tok_not_matched " +
          "(tok_insert " +
            "(tok_function struct " +
              "(. (tok_table_or_col source) a) " +
                "(tok_function when " +
                  "(tok_function isnull (. (tok_table_or_col source) b)) (. (tok_table_or_col target) b) " +
                  "(. (tok_table_or_col source) b)" +
                ")" +
              ")" +
          ")" +
        ")" +
      ")", ast.toStringTree());

  }
  /**
   * both UPDATE and INSERT
   * @throws ParseException
   */
  @Test
  public void test5() throws ParseException {
    ASTNode
      ast = parse(
      "MERGE INTO target USING source ON target.pk = source.pk WHEN MATCHED THEN UPDATE set a = source.b, c=d+1 WHEN NOT MATCHED THEN INSERT VALUES(source.a, 2, current_date())");
    Assert.assertEquals(
      "(tok_merge " +
        "(tok_tabref (tok_tabname target)) (tok_tabref (tok_tabname source)) (= (. (tok_table_or_col target) pk) (. (tok_table_or_col source) pk)) " +
        "(tok_matched " +
          "(tok_update " +
            "(tok_set_columns_clause (= (tok_table_or_col a) (. (tok_table_or_col source) b)) (= (tok_table_or_col c) (+ (tok_table_or_col d) 1)))" +
          ")" +
        ") " +
        "(tok_not_matched " +
          "(tok_insert " +
            "(tok_function struct " +
              "(. (tok_table_or_col source) a) " +
              "2 " +
              "(tok_function current_date)" +
            ")" +
          ")" +
        ")" + 
      ")", ast.toStringTree());

  }
  @Test
  public void testNegative() throws ParseException {
    expectedException.expect(ParseException.class);
    expectedException.expectMessage("line 1:74 cannot recognize input near 'INSERT' '<EOF>' '<EOF>' in WHEN MATCHED THEN clause");
    ASTNode ast = parse("MERGE INTO target USING source ON target.pk = source.pk WHEN MATCHED THEN INSERT");
  }
  @Test
  public void testNegative1() throws ParseException {
    expectedException.expect(ParseException.class);
    expectedException.expectMessage("line 1:78 mismatched input 'DELETE' expecting INSERT near 'THEN' in WHEN NOT MATCHED clause");
    ASTNode ast = parse("MERGE INTO target USING source ON target.pk = source.pk WHEN NOT MATCHED THEN DELETE");
  }
  @Test
  public void test8() throws ParseException {
    ASTNode ast = parse("MERGE INTO target USING source ON target.pk = source.pk WHEN MATCHED AND a = 1 THEN UPDATE set a = b WHEN MATCHED THEN DELETE");
  }
  @Test
  public void test9() throws ParseException {
    ASTNode ast = parse("MERGE INTO target USING source ON target.pk = source.pk " +
      "WHEN MATCHED AND a = 1 THEN UPDATE set a = b " +
      "WHEN MATCHED THEN DELETE " +
      "WHEN NOT MATCHED AND d < e THEN INSERT VALUES(1,2)");
  }
  @Test
  public void test10() throws ParseException {
    ASTNode ast = parse("MERGE INTO target USING source ON target.pk = source.pk " +
      "WHEN MATCHED AND a = 1 THEN DELETE " +
      "WHEN MATCHED THEN UPDATE set a = b " +
      "WHEN NOT MATCHED AND d < e THEN INSERT VALUES(1,2)");
  }
  /**
   * we always expect 0 or 1 update/delete WHEN clause and 0 or 1 insert WHEN clause (and 1 or 2 WHEN clauses altogether)
   * @throws ParseException
   */
  @Test
  public void testNegative3() throws ParseException {
    expectedException.expect(ParseException.class);
    expectedException.expectMessage("line 1:119 cannot recognize input near 'INSERT' 'VALUES' '(' in WHEN MATCHED THEN clause");
    ASTNode ast = parse("MERGE INTO target USING source ON target.pk = source.pk WHEN MATCHED AND a = 1 THEN UPDATE set a = b WHEN MATCHED THEN INSERT VALUES(1,2)");
  }
  /**
   * here we reverse the order of WHEN MATCHED/WHEN NOT MATCHED - should we allow it?
   * @throws ParseException
   */
  @Test
  public void testNegative4() throws ParseException {
    expectedException.expect(ParseException.class);
    expectedException.expectMessage("line 1:104 missing EOF at 'WHEN' near ')'");
    ASTNode ast = parse(
      "MERGE INTO target USING source ON target.pk = source.pk WHEN NOT MATCHED THEN INSERT VALUES(a,source.b) WHEN MATCHED THEN DELETE");
  }

  @Test
  public void test5_1() throws ParseException {
    ASTNode ast = parse(
      "MERGE INTO target USING source ON target.pk = source.pk WHEN NOT MATCHED THEN INSERT VALUES(a,source.b + 1)");
  }
  @Test
  public void test6() throws ParseException {
    ASTNode ast = parse(
      "MERGE INTO target USING source ON target.pk = source.pk WHEN NOT MATCHED THEN INSERT VALUES(a,(source.b + 1))");
  }
  @Test
  public void testNegative6() throws ParseException {
    expectedException.expect(RewriteEmptyStreamException.class);
    expectedException.expectMessage("rule whenClauses");
    ASTNode ast = parse(
      "MERGE INTO target USING source ON target.pk = source.pk");
  }
  @Test
  public void test7() throws ParseException {
    ASTNode ast = parse("merge into acidTbl" + 
      " using nonAcidPart2 source ON acidTbl.a = source.a2 " +
      "WHEN MATCHED THEN UPDATE set b = source.b2 " +
      "WHEN NOT MATCHED THEN INSERT VALUES(source.a2, source.b2)");
    Assert.assertEquals(ast.toStringTree(), 
      "(tok_merge " +
        "(tok_tabref (tok_tabname acidtbl)) (tok_tabref (tok_tabname nonacidpart2) source) " +
        "(= (. (tok_table_or_col acidtbl) a) (. (tok_table_or_col source) a2)) " +
        "(tok_matched " +
          "(tok_update " +
            "(tok_set_columns_clause (= (tok_table_or_col b) (. (tok_table_or_col source) b2))))) " +
        "(tok_not_matched " +
          "(tok_insert " +
            "(tok_function struct (. (tok_table_or_col source) a2) (. (tok_table_or_col source) b2)))))");
  }
}
