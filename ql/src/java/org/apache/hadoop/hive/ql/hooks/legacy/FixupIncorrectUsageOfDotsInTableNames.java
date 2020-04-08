package org.apache.hadoop.hive.ql.hooks.legacy;

import java.lang.reflect.Field;
import java.util.List;

import org.antlr.runtime.ClassicToken;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FixupIncorrectUsageOfDotsInTableNames implements HiveSemanticAnalyzerHook {

  protected static final Logger LOG = LoggerFactory.getLogger(FixupIncorrectUsageOfDotsInTableNames.class.getName());
  private final int HiveParser_TOK_TABNAME;
  private final int HiveParser_Identifier;

  public FixupIncorrectUsageOfDotsInTableNames() {
    HiveParser_TOK_TABNAME = getHiveParserToken("TOK_TABNAME");
    HiveParser_Identifier = getHiveParserToken("Identifier");
  }

  private int getHiveParserToken(String name) {
    try {
      Field f = HiveParser.class.getField(name);
      Class<?> t = f.getType();
      return f.getInt(null);
    } catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e) {
      throw new RuntimeException("unexpected during initialziation", e);
    }
  }

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast) throws SemanticException {
    walkTree(ast);
    return ast;
  }

  private void walkTree(ASTNode ast) {
    if (ast.getType() == HiveParser_TOK_TABNAME) {
      fixTableName(ast);
    } else {
      if (ast.getChildCount() > 0) {
        for (Node c : ast.getChildren()) {
          walkTree((ASTNode) c);
        }
      }
    }
  }

  private void fixTableName(ASTNode ast) {
    if (ast.getChildCount() == 1) {
      ASTNode oldChild = (ASTNode) ast.getChild(0);
      String str = oldChild.getText();
      String[] parts = str.split("\\.");
      if (parts.length != 2)
        return;
      LOG.error("Translating invalid tableName {} to reference database: {} and table {}", str, parts[0], parts[1]);
      ast.deleteChild(0);
      ast.addChild(new ASTNode(new ClassicToken(HiveParser_Identifier, parts[0])));
      ast.addChild(new ASTNode(new ClassicToken(HiveParser_Identifier, parts[1])));
    }
  }

  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context, List<Task<?>> rootTasks) throws SemanticException {
  }

}
