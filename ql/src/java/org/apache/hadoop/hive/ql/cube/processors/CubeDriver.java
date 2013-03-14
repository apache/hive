package org.apache.hadoop.hive.ql.cube.processors;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryContext;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryRewriter;

public class CubeDriver extends Driver {

  public CubeDriver(HiveConf conf) {
    super(conf);
  }

  public CubeDriver() {
    super();
  }

  @Override
  public int compile(String command) {
    // compile the cube query and rewrite it to HQL query
    CubeQueryRewriter rewriter = new CubeQueryRewriter(getConf());
    CubeQueryContext finalQuery;
    try {
      // 1. rewrite query to get summary tables and joins
      CubeQueryContext phase1Query = rewriter.rewritePhase1(command);
      finalQuery = rewriter.rewritePhase2(phase1Query,
          getSupportedStorages(getConf()));
    } catch (Exception e) {
      ErrorMsg error = ErrorMsg.getErrorMsg(e.getMessage());
      errorMessage = "FAILED: " + e.getClass().getSimpleName();
      if (error != ErrorMsg.GENERIC_ERROR) {
        errorMessage += " [Error "  + error.getErrorCode()  + "]:";
      }
      errorMessage += " " + e.getMessage();
      SQLState = error.getSQLState();
      console.printError(errorMessage, "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return error.getErrorCode();

    }
    return super.compile(finalQuery.toHQL());
  }

  private List<String> getSupportedStorages(HiveConf conf) {
    // TODO Auto-generated method stub
    return null;
  }
}
