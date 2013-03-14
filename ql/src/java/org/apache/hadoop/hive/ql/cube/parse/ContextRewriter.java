package org.apache.hadoop.hive.ql.cube.parse;

import org.apache.hadoop.hive.ql.parse.SemanticException;

public interface ContextRewriter {
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException;
}
