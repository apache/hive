package org.apache.hadoop.hive.ql.cube.parse;

import org.apache.hadoop.conf.Configuration;

public class AggregateResolver implements ContextRewriter {

  public AggregateResolver(Configuration conf) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) {
    // TODO
    // replace select and having columns with default aggregate functions on
    // them, if default aggregate is defined
  }

}
