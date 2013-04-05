package org.apache.hadoop.hive.ql.cube.processors;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryContext;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryRewriter;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class CubeDriver extends Driver {

  public CubeDriver(HiveConf conf) {
    super(conf);
  }

  public CubeDriver() {
    super();
  }

  public static String CUBE_QUERY_PFX = "CUBE ";
  private Context ctx;

  @Override
  public int compile(String command) {
    String query;
    try {
      query = compileCubeQuery(command.substring(CUBE_QUERY_PFX.length()));
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
    return super.compile(query);
  }

  String compileCubeQuery(String query)
      throws SemanticException, ParseException, IOException {
    System.out.println("Query :" + query);
    ctx = new Context(getConf());
    ParseDriver pd = new ParseDriver();
    ASTNode tree = pd.parse(query, ctx);
    tree = ParseUtils.findRootNonNullToken(tree);
    // compile the cube query and rewrite it to HQL query
    CubeQueryRewriter rewriter = new CubeQueryRewriter(getConf());
    // 1. rewrite query to get summary tables and joins
    CubeQueryContext phase1Query = rewriter.rewritePhase1(tree);
    CubeQueryContext finalQuery = rewriter.rewritePhase2(phase1Query,
        getSupportedStorages(getConf()));
    return finalQuery.toHQL();
  }

  private List<String> getSupportedStorages(HiveConf conf) {
    String[] storages = conf.getStrings(
        HiveConf.ConfVars.HIVE_DRIVER_SUPPORTED_STORAGES.toString());
    if (storages != null) {
      return Arrays.asList(storages);
    }
    return null;
  }
}
