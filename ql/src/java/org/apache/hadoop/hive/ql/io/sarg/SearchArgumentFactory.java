package org.apache.hadoop.hive.ql.io.sarg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;

/**
 * A factory for creating SearchArguments.
 */
public class SearchArgumentFactory {
  public static final String SARG_PUSHDOWN = "sarg.pushdown";

  public static SearchArgument create(ExprNodeGenericFuncDesc expression) {
    return new SearchArgumentImpl(expression);
  }

  public static Builder newBuilder() {
    return SearchArgumentImpl.newBuilder();
  }

  public static SearchArgument create(String kryo) {
    return SearchArgumentImpl.fromKryo(kryo);
  }

  public static SearchArgument createFromConf(Configuration conf) {
    String sargString = null;
    if ((sargString = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR)) != null) {
      return create(Utilities.deserializeExpression(sargString));
    } else if ((sargString = conf.get(SARG_PUSHDOWN)) != null) {
      return create(sargString);
    }
    return null;
  }
}
