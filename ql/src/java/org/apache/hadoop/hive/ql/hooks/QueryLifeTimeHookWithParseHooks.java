package org.apache.hadoop.hive.ql.hooks;


/**
 * Extension of {@link QueryLifeTimeHook} that has hooks for pre and post parsing of a query.
 */
public interface QueryLifeTimeHookWithParseHooks extends QueryLifeTimeHook {

  /**
   * Invoked before a query enters the parse phase.
   *
   * @param ctx the context for the hook
   */
  void beforeParse(QueryLifeTimeHookContext ctx);

  /**
   * Invoked after a query parsing. Note: if 'hasError' is true,
   * the query won't enter the following compilation phase.
   *
   * @param ctx the context for the hook
   * @param hasError whether any error occurred during compilation.
   */
  void afterParse(QueryLifeTimeHookContext ctx, boolean hasError);
}
