package org.apache.hadoop.hive.ql.cube.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class GroupbyResolver implements ContextRewriter {

  public GroupbyResolver(Configuration conf) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    // TODO Auto-generated method stub
    // Process Aggregations by making sure that all group by keys are projected;
    // and all projection fields are added to group by keylist;
  }

}
