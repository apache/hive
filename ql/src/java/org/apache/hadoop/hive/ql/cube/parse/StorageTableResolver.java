package org.apache.hadoop.hive.ql.cube.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class StorageTableResolver implements ContextRewriter {

  public StorageTableResolver(Configuration conf) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql)
      throws SemanticException {
    // TODO
    //Find candidate tables wrt supported storages
  }

}
