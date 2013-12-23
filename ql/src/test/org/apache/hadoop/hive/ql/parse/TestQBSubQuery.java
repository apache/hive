package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestQBSubQuery {
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
    conf = new HiveConf(SemanticAnalyzer.class);
    SessionState.start(conf);
  }

  @Before
  public void setup() throws SemanticException {
    pd = new ParseDriver();
    sA = new SemanticAnalyzer(conf);
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
    Assert.assertEquals(sqs.size(), 1);
    
    ASTNode sq = sqs.get(0);
    Assert.assertEquals(sq.toStringTree(),
        "(TOK_SUBQUERY_EXPR (TOK_SUBQUERY_OP in) (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src) s1)) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_TABLE_OR_COL key))) (TOK_WHERE (and (> (. (TOK_TABLE_OR_COL s1) key) '9') (> (. (TOK_TABLE_OR_COL s1) value) '9'))))) (. (TOK_TABLE_OR_COL src) key))"
        );
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
    Assert.assertEquals(conjuncts.size(), 2);
    
    Assert.assertEquals(conjuncts.get(0).toStringTree(), "(> (. (TOK_TABLE_OR_COL s1) key) '9')");
    Assert.assertEquals(conjuncts.get(1).toStringTree(), "(> (. (TOK_TABLE_OR_COL s1) value) '9')");
  }
  
  @Test
  public void testRewriteOuterQueryWhere() throws Exception {
    ASTNode ast = parse(IN_QUERY);
    ASTNode where = where(ast);
    List<ASTNode> sqs = SubQueryUtils.findSubQueries((ASTNode) where.getChild(0));    
    ASTNode sq = sqs.get(0);
    
    ASTNode newWhere = SubQueryUtils.rewriteParentQueryWhere((ASTNode) where.getChild(0), sq);
    Assert.assertEquals(newWhere.toStringTree(), "(= 1 1)");
  }
  
  @Test
  public void testRewriteOuterQueryWhere2() throws Exception {
    ASTNode ast = parse(IN_QUERY2);
    ASTNode where = where(ast);
    List<ASTNode> sqs = SubQueryUtils.findSubQueries((ASTNode) where.getChild(0));    
    ASTNode sq = sqs.get(0);
    
    ASTNode newWhere = SubQueryUtils.rewriteParentQueryWhere((ASTNode) where.getChild(0), sq);
    Assert.assertEquals(newWhere.toStringTree(), "(> (TOK_TABLE_OR_COL value) '9')");
  }
  
  @Test
  public void testCheckAggOrWindowing() throws Exception {
    ASTNode ast = parse(QUERY3);
    ASTNode select = select(ast);
    
    Assert.assertEquals(SubQueryUtils.checkAggOrWindowing((ASTNode) select.getChild(0)), 0);
    Assert.assertEquals(SubQueryUtils.checkAggOrWindowing((ASTNode) select.getChild(1)), 1);
    Assert.assertEquals(SubQueryUtils.checkAggOrWindowing((ASTNode) select.getChild(2)), 2);
  }
  
  private ASTNode where(ASTNode qry) {
    return (ASTNode) qry.getChild(1).getChild(2);
  }
  
  private ASTNode select(ASTNode qry) {
    return (ASTNode) qry.getChild(1).getChild(1);
  }

}
