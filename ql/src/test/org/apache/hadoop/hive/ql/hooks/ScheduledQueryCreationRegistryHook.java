package org.apache.hadoop.hive.ql.hooks;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * This hook helps to do some cleanup after tests are creating scheduled queries.
 */
public class ScheduledQueryCreationRegistryHook extends AbstractSemanticAnalyzerHook {

  private static Set<String> knownSchedules = new HashSet<String>();

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast) throws SemanticException {
    int type = ast.getType();
    switch (type) {
    case HiveParser.TOK_CREATE_SCHEDULED_QUERY:
      registerCreated(ast.getChild(0).getText());
    }

    return super.preAnalyze(context, ast);
  }

  private void registerCreated(String scheduleName) {
    knownSchedules.add(scheduleName);
  }

  public static Set<String> getSchedules() {
    return knownSchedules;
  }

}
