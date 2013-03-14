package org.apache.hadoop.hive.ql.cube.parse;

import static org.apache.hadoop.hive.ql.parse.HiveParser.Identifier;
import static org.apache.hadoop.hive.ql.parse.HiveParser.KW_AND;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_FUNCTION;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.JoinCond;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

public class CubeQueryContext {
  public static final String TIME_RANGE_FUNC = "time_range_in";
  public static final String NOW = "now";
  public static final String DEFAULT_TABLE = "_default_";

  private final ASTNode ast;
  private final QB qb;
  private final HiveConf conf;
  private String fromDateRaw;
  private String toDateRaw;
  private Cube cube;
  private Set<CubeDimensionTable> dimensions = new HashSet<CubeDimensionTable>();
  private Set<CubeFactTable> candidateFactTables = new HashSet<CubeFactTable>();
  private final Map<String, List<String>> tblToColumns = new HashMap<String, List<String>>();
  private Date timeFrom;
  private Date timeTo;
  private String clauseName = null;
  private Map<String, List<String>> partitionCols;

  public CubeQueryContext(ASTNode ast, QB qb, HiveConf conf)
      throws SemanticException {
    this.ast = ast;
    this.qb = qb;
    this.conf = conf;
    //extractTimeRange();
    //extractMetaTables();
  }

  public CubeQueryContext(CubeQueryContext other) {
    this.ast = other.ast;
    this.qb = other.cloneqb();
    this.conf = other.conf;
    this.fromDateRaw = other.fromDateRaw;
    this.toDateRaw = other.toDateRaw;
    this.dimensions = other.dimensions;
    this.candidateFactTables = other.candidateFactTables;
    this.timeFrom = other.timeFrom;
    this.timeTo = other.timeTo;
    this.partitionCols = other.partitionCols;
  }

  private QB cloneqb() {
    //TODO do deep copy of QB
    return qb;
  }

  private void extractMetaTables() throws SemanticException {
    try {
      CubeMetastoreClient client;
        client = CubeMetastoreClient.getInstance(conf);
      List<String> tabAliases = new ArrayList<String>(qb.getTabAliases());
      for (String alias :  tabAliases) {
        String tblName = qb.getTabNameForAlias(alias);
        if (client.isCube(tblName)) {
          if (cube != null) {
            if (cube.getName() != tblName) {
              throw new SemanticException("More than one cube accessed in query");
            }
          }
          cube = client.getCube(tblName);
        } else if (client.isDimensionTable(tblName)) {
          dimensions.add(client.getDimensionTable(tblName));
        }
      }
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  private String getClause() {
    if (clauseName == null) {
      TreeSet<String> ks = new TreeSet<String>(qb.getParseInfo().getClauseNames());
      clauseName = ks.first();
    }
    return clauseName;
  }

  private void extractTimeRange() throws SemanticException {
    // get time range -
    // Time range should be direct child of where condition
    // TOK_WHERE.TOK_FUNCTION.Identifier Or, it should be right hand child of
    // AND condition TOK_WHERE.KW_AND.TOK_FUNCTION.Identifier
    ASTNode whereTree = qb.getParseInfo().getWhrForClause(clauseName);
    if (whereTree == null || whereTree.getChildCount() < 1) {
      throw new SemanticException("No filter specified");
    }
    ASTNode timenode = null;
    if (TOK_FUNCTION == whereTree.getChild(0).getType()) {
      // expect only time range
      timenode = HQLParser.findNodeByPath(whereTree, TOK_FUNCTION);
    } else if (KW_AND == whereTree.getChild(0).getType()){
      // expect time condition as the right sibling of KW_AND
      timenode = HQLParser.findNodeByPath(whereTree, KW_AND, TOK_FUNCTION);
    }

    if (timenode == null) {
      throw new SemanticException("Unable to get time range");
    }

    ASTNode fname = HQLParser.findNodeByPath(timenode, Identifier);
    if (!TIME_RANGE_FUNC.equalsIgnoreCase(fname.getText())) {
      throw new SemanticException("Expected time range as " + TIME_RANGE_FUNC);
    }

    fromDateRaw =
        PlanUtils.stripQuotes(timenode.getChild(1).getText());
    if (timenode.getChildCount() > 2) {
      toDateRaw = PlanUtils.stripQuotes(
          timenode.getChild(2).getText());
    }
    Date now = new Date();

    try {
      timeFrom = DateUtils.resolveDate(fromDateRaw, now);
      timeTo = DateUtils.resolveDate(toDateRaw, now);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

/*  private void extractColumns() {
    //columnAliases = new ArrayList<String>();
    tblToColumns = new LinkedHashMap<String, List<String>>();
    ASTNode selectTree = qb.getParseInfo().getSelForClause(clauseName);

    // Check if its 'select *  from...'
    if (selectTree.getChildCount() == 1) {
      ASTNode star = HQLParser.findNodeByPath(selectTree, TOK_SELEXPR,
          TOK_ALLCOLREF);
      if (star == null) {
        star = HQLParser.findNodeByPath(selectTree, TOK_SELEXPR,
            TOK_FUNCTIONSTAR);
      }

      if (star != null) {
        int starType = star.getToken().getType();
        if (TOK_FUNCTIONSTAR == starType || TOK_ALLCOLREF == starType) {
          selectAllColumns = true;
        }
      }
    }

    // Traverse select, where, groupby, having and orderby trees to get column
    // names
    ASTNode trees[] = { selectTree, whereTree, groupbyTree,
        havingTree, orderByTree};

    for (ASTNode tree : trees) {
      if (tree == null) {
        continue;
      }
      // Traverse the tree to get column names
      // We are doing a complete traversal so that expressions of columns
      // are also captured ex: f(cola + colb/tab1.colc)
      HQLParser.bft(tree, new ASTNodeVisitor() {
        @Override
        public void visit(TreeNode visited) {
          ASTNode node = visited.getNode();
          ASTNode parent = null;
          if (visited.getParent() != null) {
            parent = visited.getParent().getNode();
          }

          if (node.getToken().getType() == TOK_TABLE_OR_COL
              && (parent != null && parent.getToken().getType() != DOT)) {
            // Take child ident.totext
            ASTNode ident = (ASTNode) node.getChild(0);
            List<String> colList = tblToColumns.get(DEFAULT_TABLE);
            if (colList == null) {
              colList = new ArrayList<String>();
              tblToColumns.put(DEFAULT_TABLE, colList);
            }
            if (!colList.contains(ident.getText())) {
              colList.add(ident.getText());
            }
          } else if (node.getToken().getType() == DOT) {
            // This is for the case where column name is prefixed by table name
            // or table alias
            // For example 'select fact.id, dim2.id ...'
            // Right child is the column name, left child.ident is table name
            ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL,
                Identifier);
            ASTNode colIdent = (ASTNode) node.getChild(1);

            String column = colIdent.getText();
            String table = tabident.getText();
            List<String> colList = tblToColumns.get(table);
            if (colList == null) {
              colList = new ArrayList<String>();
              tblToColumns.put(table, colList);
            }
            if (!colList.contains(column)) {
              colList.add(column);
            }
          } else if (node.getToken().getType() == TOK_SELEXPR) {
            // Extract column aliases for the result set, only applies to select
            // trees
            ASTNode alias = HQLParser.findNodeByPath(node, Identifier);
            if (alias != null) {
              columnAliases.add(alias.getText());
            }
          }
        }
      });
    }
  }
*/
  public String getFromDateRaw() {
    return fromDateRaw;
  }

  public Cube getCube() {
    return cube;
  }

  public String getToDateRaw() {
    return toDateRaw;
  }

  public Date getFromDate() {
    return timeFrom;
  }

  public Date getToDate() {
    return timeTo;
  }

  public QB getQB() {
    return qb;
  }

  public Set<CubeFactTable> getFactTables() {
    return candidateFactTables;
  }

  public Set<CubeDimensionTable> getDimensionTables() {
    return dimensions;
  }

  public void print() {
    StringBuilder builder = new StringBuilder();
    builder.append("ASTNode:" + ast.dump() + "\n");
    builder.append("QB:");
    builder.append("\n numJoins:" + qb.getNumJoins());
    builder.append("\n numGbys:" + qb.getNumGbys());
    builder.append("\n numSels:" + qb.getNumSels());
    builder.append("\n numSelDis:" + qb.getNumSelDi());
    builder.append("\n aliasToTabs:");
    Set<String> tabAliases = qb.getTabAliases();
    for (String alias : tabAliases) {
      builder.append("\n\t" + alias + ":" + qb.getTabNameForAlias(alias));
    }
    builder.append("\n aliases:");
    for (String alias : qb.getAliases()) {
      builder.append(alias);
      builder.append(", ");
    }
    builder.append("id:" + qb.getId());
    builder.append("isQuery:" + qb.getIsQuery());
    builder.append("\n QBParseInfo");
    QBParseInfo parseInfo = qb.getParseInfo();
    builder.append("\n isSubQ: " + parseInfo.getIsSubQ());
    builder.append("\n alias: " + parseInfo.getAlias());
    if (parseInfo.getJoinExpr() != null) {
      builder.append("\n joinExpr: " + parseInfo.getJoinExpr().dump());
    }
    builder.append("\n hints: " + parseInfo.getHints());
    builder.append("\n aliasToSrc: ");
    for (String alias : tabAliases) {
      builder.append("\n\t" + alias +": " + parseInfo.getSrcForAlias(alias).dump());
    }
    TreeSet<String> clauses = new TreeSet<String>(parseInfo.getClauseNames());
    for (String clause : clauses) {
      builder.append("\n\t" + clause + ": " + parseInfo.getClauseNamesForDest());
    }
    String clause = clauses.first();
    if (parseInfo.getWhrForClause(clause) != null) {
      builder.append("\n whereexpr: " + parseInfo.getWhrForClause(clause).dump());
    }
    if (parseInfo.getGroupByForClause(clause) != null) {
      builder.append("\n groupby expr: " + parseInfo.getGroupByForClause(clause).dump());
    }
    if (parseInfo.getSelForClause(clause) != null) {
      builder.append("\n sel expr: " + parseInfo.getSelForClause(clause).dump());
    }
    if (parseInfo.getHavingForClause(clause) != null) {
      builder.append("\n having expr: " + parseInfo.getHavingForClause(clause).dump());
    }
    if (parseInfo.getDestLimit(clause) != null) {
      builder.append("\n limit: " + parseInfo.getDestLimit(clause));
    }
    if (parseInfo.getAllExprToColumnAlias() != null && !parseInfo.getAllExprToColumnAlias().isEmpty()) {
      builder.append("\n exprToColumnAlias:");
      for (Map.Entry<ASTNode, String> entry : parseInfo.getAllExprToColumnAlias().entrySet()) {
        builder.append("\n\t expr: " + entry.getKey().dump() + " ColumnAlias: " + entry.getValue());
      }
    }
    //builder.append("\n selectStar: " + parseInfo.isSelectStarQuery());
    if (parseInfo.getAggregationExprsForClause(clause) != null) {
      builder.append("\n aggregateexprs:");
      for (Map.Entry<String, ASTNode> entry : parseInfo.getAggregationExprsForClause(clause).entrySet()) {
        builder.append("\n\t key: " + entry.getKey() + " expr: " + entry.getValue().dump());
      }
    }
    if (parseInfo.getDistinctFuncExprsForClause(clause) != null) {
      builder.append("\n distinctFuncExprs:");
      for (ASTNode entry : parseInfo.getDistinctFuncExprsForClause(clause)) {
        builder.append("\n\t expr: " + entry.dump());
      }
    }

    if(qb.getQbJoinTree() != null) {
      builder.append("\n\n JoinTree");
      QBJoinTree joinTree = qb.getQbJoinTree();
      printJoinTree(joinTree, builder);
    }
    System.out.println(builder.toString());
  }

  void printJoinTree(QBJoinTree joinTree, StringBuilder builder) {
    builder.append("leftAlias:" + joinTree.getLeftAlias());
    if (joinTree.getLeftAliases() != null) {
      builder.append("\n leftAliases:");
      for (String alias: joinTree.getLeftAliases()) {
        builder.append("\n\t " + alias);
      }
    }
    if (joinTree.getRightAliases() != null) {
      builder.append("\n rightAliases:");
      for (String alias: joinTree.getRightAliases()) {
        builder.append("\n\t " + alias);
      }
    }
    if (joinTree.getJoinSrc() != null) {
      builder.append("\n JoinSrc: {");
      printJoinTree(joinTree.getJoinSrc(), builder);
      builder.append("\n }");
    }
    if (joinTree.getBaseSrc() != null) {
      builder.append("\n baseSrcs:");
      for (String src: joinTree.getBaseSrc()) {
        builder.append("\n\t " + src);
      }
    }
    builder.append("\n noOuterJoin: " + joinTree.getNoOuterJoin());
    builder.append("\n noSemiJoin: " + joinTree.getNoSemiJoin());
    builder.append("\n mapSideJoin: " + joinTree.isMapSideJoin());
    if (joinTree.getJoinCond() != null) {
      builder.append("\n joinConds:");
      for (JoinCond cond: joinTree.getJoinCond()) {
        builder.append("\n\t left: " + cond.getLeft() + " right: "
            + cond.getRight() + " type:" + cond.getJoinType()
            + " preserved:" + cond.getPreserved());
      }
    }
    if (joinTree.getExpressions() != null) {
      builder.append("\n join expressions:");
      for (ArrayList<ASTNode> exprs: joinTree.getExpressions()) {
        builder.append("\n\t exprs:");
        for (ASTNode expr : exprs) {
          builder.append("\n\t\t expr:" + expr.dump());
        }
      }
    }
    if (joinTree.getFilters() != null) {
      builder.append("\n join filters:");
      for (ArrayList<ASTNode> exprs: joinTree.getFilters()) {
        builder.append("\n\t filters:");
        for (ASTNode expr : exprs) {
          builder.append("\n\t\t expr:" + expr.dump());
        }
      }
    }
    if (joinTree.getFiltersForPushing() != null) {
      builder.append("\n join filtersForPushing: ");
      for (ArrayList<ASTNode> exprs: joinTree.getFiltersForPushing()) {
        builder.append("\n\t filters:");
        for (ASTNode expr : exprs) {
          builder.append("\n\t\t expr:" + expr.dump());
        }
      }
    }

    if (joinTree.getNullSafes() != null) {
      builder.append("\n join nullsafes: ");
      for (Boolean bool: joinTree.getNullSafes()) {
        builder.append("\n\t " + bool);
      }
    }
    if (joinTree.getMapAliases() != null) {
      builder.append("\n join mapaliases: ");
      for (String alias : joinTree.getMapAliases()) {
        builder.append("\n\t " + alias);
      }
    }
    if (joinTree.getStreamAliases() != null) {
      builder.append("\n join streamaliases: ");
      for (String alias : joinTree.getStreamAliases()) {
        builder.append("\n\t " + alias);
      }
    }
  }

  public String toHQL() {
    // TODO Auto-generated method stub
    return null;
  }
}
