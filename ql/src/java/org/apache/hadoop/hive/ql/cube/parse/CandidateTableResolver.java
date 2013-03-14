package org.apache.hadoop.hive.ql.cube.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class CandidateTableResolver implements ContextRewriter {

  public CandidateTableResolver(Configuration conf) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    // TODO Auto-generated method stub
    // Find the candidate fact tables who can answer the query and
    // add them to cubeql
  }

}
