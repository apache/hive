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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestQBSubQuery {
  static QueryState queryState;
  static HiveConf conf;

  private static String IN_QUERY = " select * " +
  		"from src " +
  		"where src.key in (select key from src s1 where s1.key > '9' and s1.value > '9') ";

  private static String IN_QUERY2 = " select * " +
      "from src " +
      "where src.key in (select key from src s1 where s1.key > '9' and s1.value > '9') and value > '9'";

  private static String QUERY3 = "select p_mfgr, min(p_size), rank() over(partition by p_mfgr) as r from part group by p_mfgr";

  ParseDriver pd;
  SemanticAnalyzer sA;

  @BeforeClass
  public static void initialize() {
    queryState = new QueryState(new HiveConf(SemanticAnalyzer.class));
    conf = queryState.getConf();
    SessionState.start(conf);
  }

  @Before
  public void setup() throws SemanticException {
    pd = new ParseDriver();
    sA = new CalcitePlanner(queryState);
  }

  ASTNode parse(String query) throws ParseException {
    ASTNode nd = pd.parse(query);
    return (ASTNode) nd.getChild(0);
  }

  @Test
  public void testExtractSubQueries() throws Exception {
    ASTNode ast = parse(IN_QUERY);
    ASTNode where = where(ast);
    List<ASTNode> sqs = SubQueryUtils.findSubQueries((ASTNode) where.getChild(0));
    Assert.assertEquals(1,sqs.size());

    ASTNode sq = sqs.get(0);
    Assert.assertEquals("(tok_subquery_expr (tok_subquery_op kw_in) (tok_query (tok_from (tok_tabref (tok_tabname src) s1)) (tok_insert (tok_destination (tok_dir tok_tmp_file)) (tok_select (tok_selexpr (tok_table_or_col key))) (tok_where (and (> (. (tok_table_or_col s1) key) '9') (> (. (tok_table_or_col s1) value) '9'))))) (. (tok_table_or_col src) key))"
        ,sq.toStringTree());
  }

  @Test
  public void testExtractConjuncts() throws Exception {
    ASTNode ast = parse(IN_QUERY);
    ASTNode where = where(ast);
    List<ASTNode> sqs = SubQueryUtils.findSubQueries((ASTNode) where.getChild(0));
    ASTNode sq = sqs.get(0);

    ASTNode sqWhere = where((ASTNode) sq.getChild(1));

    List<ASTNode> conjuncts = new ArrayList<ASTNode>();
    SubQueryUtils.extractConjuncts((ASTNode) sqWhere.getChild(0), conjuncts);
    Assert.assertEquals(2, conjuncts.size());

    Assert.assertEquals("(> (. (tok_table_or_col s1) key) '9')", conjuncts.get(0).toStringTree());
    Assert.assertEquals("(> (. (tok_table_or_col s1) value) '9')", conjuncts.get(1).toStringTree());
  }

  @Test
  public void testRewriteOuterQueryWhere() throws Exception {
    ASTNode ast = parse(IN_QUERY);
    ASTNode where = where(ast);
    List<ASTNode> sqs = SubQueryUtils.findSubQueries((ASTNode) where.getChild(0));
    ASTNode sq = sqs.get(0);

    ASTNode newWhere = SubQueryUtils.rewriteParentQueryWhere((ASTNode) where.getChild(0), sq);
    Assert.assertEquals("(= 1 1)",newWhere.toStringTree());
  }

  @Test
  public void testRewriteOuterQueryWhere2() throws Exception {
    ASTNode ast = parse(IN_QUERY2);
    ASTNode where = where(ast);
    List<ASTNode> sqs = SubQueryUtils.findSubQueries((ASTNode) where.getChild(0));
    ASTNode sq = sqs.get(0);

    ASTNode newWhere = SubQueryUtils.rewriteParentQueryWhere((ASTNode) where.getChild(0), sq);
    Assert.assertEquals("(> (tok_table_or_col value) '9')",newWhere.toStringTree());
  }

  @Test
  public void testCheckAggOrWindowing() throws Exception {
    ASTNode ast = parse(QUERY3);
    ASTNode select = select(ast);

    Assert.assertEquals(0, SubQueryUtils.checkAggOrWindowing((ASTNode) select.getChild(0)));
    Assert.assertEquals(1, SubQueryUtils.checkAggOrWindowing((ASTNode) select.getChild(1)));
    Assert.assertEquals(3, SubQueryUtils.checkAggOrWindowing((ASTNode) select.getChild(2)));
  }

  private ASTNode where(ASTNode qry) {
    return (ASTNode) qry.getChild(1).getChild(2);
  }

  private ASTNode select(ASTNode qry) {
    return (ASTNode) qry.getChild(1).getChild(1);
  }

}
