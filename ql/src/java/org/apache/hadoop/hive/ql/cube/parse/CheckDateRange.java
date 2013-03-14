package org.apache.hadoop.hive.ql.cube.parse;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;

public class CheckDateRange extends ValidationRule {

  public CheckDateRange(Configuration conf) {
    super(conf);
  }

  @Override
  public boolean validate(CubeQueryContext ctx) {
    Date from = ctx.getFromDate();
    Date to = ctx.getToDate();

    if (from == null || to == null) {
      error = "From or to date is missing";
      return false;
    }

    if (from.compareTo(to) > 0) {
      error = "From date is after the to date";
      return false;
    }

    return true;
  }

}
