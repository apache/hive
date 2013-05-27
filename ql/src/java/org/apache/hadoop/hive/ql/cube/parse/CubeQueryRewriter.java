package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class CubeQueryRewriter {
  private final Configuration conf;
  private final List<ContextRewriter> rewriters =
      new ArrayList<ContextRewriter>();
  public CubeQueryRewriter(Configuration conf) {
    this.conf = conf;
    setupRewriters();
  }

  private void setupRewriters() {
    // Resolve joins and generate base join tree
    rewriters.add(new JoinResolver(conf));
    // Rewrite base trees (groupby, having, orderby, limit) using aliases
    rewriters.add(new AliasReplacer(conf));
    // Resolve aggregations and generate base select tree
    rewriters.add(new AggregateResolver(conf));
    rewriters.add(new GroupbyResolver(conf));
    // Resolve storage partitions and table names
    rewriters.add(new StorageTableResolver(conf));
    rewriters.add(new LeastPartitionResolver(conf));
    rewriters.add(new LightestFactResolver(conf));
    rewriters.add(new LeastDimensionResolver(conf));
  }

  public CubeQueryContext rewrite(ASTNode astnode)
      throws SemanticException, ParseException {
    CubeSemanticAnalyzer analyzer = new CubeSemanticAnalyzer(
        new HiveConf(conf, HiveConf.class));
    analyzer.analyzeInternal(astnode);
    CubeQueryContext ctx = analyzer.getQueryContext();
    rewrite(rewriters, ctx);
    return ctx;
  }

  private void rewrite(List<ContextRewriter> rewriters, CubeQueryContext ctx)
      throws SemanticException {
    for (ContextRewriter rewriter : rewriters) {
      rewriter.rewriteContext(ctx);
    }
  }
}
