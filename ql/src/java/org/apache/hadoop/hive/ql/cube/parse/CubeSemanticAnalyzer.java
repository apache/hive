package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class CubeSemanticAnalyzer extends SemanticAnalyzer {
  private final HiveConf conf;
  private final List<ValidationRule> validationRules = new ArrayList<ValidationRule>();
  private CubeQueryContext cubeQl;

  public CubeSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
    this.conf = conf;
    setupRules();
  }

  private void setupRules() {
    validationRules.add(new CheckTableNames(conf));
    validationRules.add(new CheckDateRange(conf));
    validationRules.add(new CheckColumnMapping(conf));
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    reset();
    QB qb = new QB(null, null, false);
    // do not allow create table/view commands
    // TODO Move this to a validation rule
    if (ast.getToken().getType() == HiveParser.TOK_CREATETABLE ||
        ast.getToken().getType() == HiveParser.TOK_CREATEVIEW) {
      throw new SemanticException("Create table/view is not allowed");
    }

    //analyzing from the ASTNode.
    if (!doPhase1(ast, qb, initPhase1Ctx())) {
      // if phase1Result false return
      return;
    }
    cubeQl = new CubeQueryContext(ast, qb, conf);
    //cubeQl.init();
    //validate();

    // TODO Move this to a validation Rule
    //QBParseInfo qbp = qb.getParseInfo();
    //TreeSet<String> ks = new TreeSet<String>(qbp.getClauseNames());
    //if (ks.size() > 1) {
    //  throw new SemanticException("nested/sub queries not allowed yet");
    //}
    //Operator sinkOp = genPlan(qb);
    //System.out.println(sinkOp.toString());
  }

  @Override
  public void validate() throws SemanticException {
    for (ValidationRule rule : validationRules) {
      if (!rule.validate(cubeQl)) {
        break;
      }
    }
  }

  public CubeQueryContext getQueryContext() {
    return cubeQl;
  }
}
