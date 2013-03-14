package org.apache.hadoop.hive.ql.cube.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public abstract class ValidationRule {
  Configuration conf;
  String error;

  public ValidationRule(Configuration conf) {
    this.conf = conf;
  }

  public abstract boolean validate(CubeQueryContext ctx) throws SemanticException;

  public String getErrorMessage() {
    return error;
  }
}
