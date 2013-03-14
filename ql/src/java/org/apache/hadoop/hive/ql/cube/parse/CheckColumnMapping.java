package org.apache.hadoop.hive.ql.cube.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class CheckColumnMapping extends ValidationRule {

  public CheckColumnMapping(Configuration conf) {
    super(conf);
  }

  @Override
  public boolean validate(CubeQueryContext ctx) throws SemanticException {
    // TODO
    return true;
  }

}
