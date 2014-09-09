package org.apache.hadoop.hive.ql.io.sarg;

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

/**
 * A factory for creating SearchArguments.
 */
public class SearchArgumentFactory {
  public static SearchArgument create(ExprNodeGenericFuncDesc expression) {
    return new SearchArgumentImpl(expression);
  }

  public static Builder newBuilder() {
    return SearchArgumentImpl.newBuilder();
  }

  public static SearchArgument create(String kryo) {
    return SearchArgumentImpl.fromKryo(kryo);
  }
}
