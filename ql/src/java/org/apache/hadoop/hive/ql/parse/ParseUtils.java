/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.parse;

import com.google.common.base.Preconditions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.google.common.collect.Lists;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.table.partition.PartitionUtils;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTBuilder;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner.ASTSearcher;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.parse.type.TypeCheckCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.getTypeStringFromAST;
import static org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.unescapeIdentifier;


/**
 * Library of utility functions used in the parse code.
 *
 */
public final class ParseUtils {
  /** Parses the Hive query. */
  private static final Logger LOG = LoggerFactory.getLogger(ParseUtils.class);

  /** Parses the Hive query. */
  public static ASTNode parse(String command, Context ctx) throws ParseException {
    return parse(command, ctx, null);
  }

  /** Parses the Hive query. */
  public static ASTNode parse(
      String command, Context ctx, String viewFullyQualifiedName) throws ParseException {
    ParseDriver pd = new ParseDriver();
    Configuration configuration = ctx != null ? ctx.getConf() : null;
    ParseResult parseResult = pd.parse(command, configuration);
    if (ctx != null) {
      if (viewFullyQualifiedName == null) {
        // Top level query
        ctx.setTokenRewriteStream(parseResult.getTokenRewriteStream());
      } else {
        // It is a view
        ctx.addViewTokenRewriteStream(viewFullyQualifiedName, parseResult.getTokenRewriteStream());
      }
      ctx.setParsedTables(parseResult.getTables());
    }
    ASTNode tree = parseResult.getTree();
    tree = findRootNonNullToken(tree);
    handleSetColRefs(tree, ctx);
    return tree;
  }

  /**
   * Tests whether the parse tree node is a join token.
   *
   * @param node
   *          The parse tree node
   * @return boolean
   */
  public static boolean isJoinToken(ASTNode node) {
    switch (node.getToken().getType()) {
    case HiveParser.TOK_JOIN:
    case HiveParser.TOK_LEFTOUTERJOIN:
    case HiveParser.TOK_RIGHTOUTERJOIN:
    case HiveParser.TOK_FULLOUTERJOIN:
      return true;
    default:
      return false;
    }
  }

  /**
   * Performs a descent of the leftmost branch of a tree, stopping when either a
   * node with a non-null token is found or the leaf level is encountered.
   *
   * @param tree
   *          candidate node from which to start searching
   *
   * @return node at which descent stopped
   */
  private static ASTNode findRootNonNullToken(ASTNode tree) {
    while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
      tree = (ASTNode) tree.getChild(0);
    }
    return tree;
  }

  private ParseUtils() {
    // prevent instantiation
  }

  public static List<String> validateColumnNameUniqueness(
      List<FieldSchema> fieldSchemas) throws SemanticException {

    // no duplicate column names
    // currently, it is a simple n*n algorithm - this can be optimized later if
    // need be
    // but it should not be a major bottleneck as the number of columns are
    // anyway not so big
    Iterator<FieldSchema> iterCols = fieldSchemas.iterator();
    List<String> colNames = new ArrayList<String>();
    while (iterCols.hasNext()) {
      String colName = iterCols.next().getName();
      Iterator<String> iter = colNames.iterator();
      while (iter.hasNext()) {
        String oldColName = iter.next();
        if (colName.equalsIgnoreCase(oldColName)) {
          throw new SemanticException(ErrorMsg.DUPLICATE_COLUMN_NAMES
              .getMsg(oldColName));
        }
      }
      colNames.add(colName);
    }
    return colNames;
  }

  public static VarcharTypeInfo getVarcharTypeInfo(ASTNode node)
      throws SemanticException {
    if (node.getChildCount() != 1) {
      throw new SemanticException("Bad params for type varchar");
    }

    String lengthStr = node.getChild(0).getText();
    return TypeInfoFactory.getVarcharTypeInfo(Integer.parseInt(lengthStr));
  }

  public static CharTypeInfo getCharTypeInfo(ASTNode node)
      throws SemanticException {
    if (node.getChildCount() != 1) {
      throw new SemanticException("Bad params for type char");
    }

    String lengthStr = node.getChild(0).getText();
    return TypeInfoFactory.getCharTypeInfo(Integer.parseInt(lengthStr));
  }

  static int getIndex(String[] list, String elem) {
    for(int i=0; i < list.length; i++) {
      if (list[i] != null && list[i].toLowerCase().equals(elem)) {
        return i;
      }
    }
    return -1;
  }

  /*
   * if the given filterCondn refers to only 1 table alias in the QBJoinTree,
   * we return that alias's position. Otherwise we return -1
   */
  static int checkJoinFilterRefersOneAlias(String[] tabAliases, ASTNode filterCondn) {

    switch(filterCondn.getType()) {
    case HiveParser.TOK_TABLE_OR_COL:
      String tableOrCol = unescapeIdentifier(filterCondn.getChild(0).getText()
          .toLowerCase());
      return getIndex(tabAliases, tableOrCol);
    case HiveParser.Identifier:
    case HiveParser.Number:
    case HiveParser.StringLiteral:
    case HiveParser.IntegralLiteral:
    case HiveParser.NumberLiteral:
    case HiveParser.TOK_STRINGLITERALSEQUENCE:
    case HiveParser.TOK_CHARSETLITERAL:
    case HiveParser.TOK_DATELITERAL:
    case HiveParser.KW_TRUE:
    case HiveParser.KW_FALSE:
    case HiveParser.TOK_NULL:
      return -1;
    default:
      int idx = -1;
      int i = filterCondn.getType() == HiveParser.TOK_FUNCTION ? 1 : 0;
      for (; i < filterCondn.getChildCount(); i++) {
        int cIdx = checkJoinFilterRefersOneAlias(tabAliases, (ASTNode) filterCondn.getChild(i));
        if (cIdx != idx) {
          if (idx != -1 && cIdx != -1) {
            return -1;
          }
          idx = idx == -1 ? cIdx : idx;
        }
      }
      return idx;
    }
  }

  public static DecimalTypeInfo getDecimalTypeTypeInfo(ASTNode node)
      throws SemanticException {
    if (node.getChildCount() > 2) {
      throw new SemanticException("Bad params for type decimal");
    }

    int precision = HiveDecimal.USER_DEFAULT_PRECISION;
    int scale = HiveDecimal.USER_DEFAULT_SCALE;

    if (node.getChildCount() >= 1) {
      String precStr = node.getChild(0).getText();
      precision = Integer.parseInt(precStr);
    }

    if (node.getChildCount() == 2) {
      String scaleStr = node.getChild(1).getText();
      scale = Integer.parseInt(scaleStr);
    }

    return TypeInfoFactory.getDecimalTypeInfo(precision, scale);
  }

  public static String ensureClassExists(String className)
      throws SemanticException {
    if (className == null) {
      return null;
    }
    try {
      Class.forName(className, true, Utilities.getSessionSpecifiedClassLoader());
    } catch (ClassNotFoundException e) {
      throw new SemanticException("Cannot find class '" + className + "'", e);
    }
    return className;
  }

  public static boolean containsTokenOfType(ASTNode root, Integer ... tokens) {
    final Set<Integer> tokensToMatch = new HashSet<Integer>();
    for (Integer tokenTypeToMatch : tokens) {
      tokensToMatch.add(tokenTypeToMatch);
    }

    return ParseUtils.containsTokenOfType(root, new PTFUtils.Predicate<ASTNode>() {
      @Override
      public boolean apply(ASTNode node) {
        return tokensToMatch.contains(node.getType());
      }
    });
  }

  public static boolean containsTokenOfType(ASTNode root, PTFUtils.Predicate<ASTNode> predicate) {
    Queue<ASTNode> queue = new ArrayDeque<ASTNode>();

    // BFS
    queue.add(root);
    while (!queue.isEmpty())  {
      ASTNode current = queue.remove();
      // If the predicate matches, then return true.
      // Otherwise visit the next set of nodes that haven't been seen.
      if (predicate.apply(current)) {
        return true;
      } else {
        // Guard because ASTNode.getChildren.iterator returns null if no children available (bug).
        if (current.getChildCount() > 0) {
          for (Node child : current.getChildren()) {
            queue.add((ASTNode) child);
          }
        }
      }
    }

    return false;
  }

  private static void handleSetColRefs(ASTNode tree, Context ctx) {
    CalcitePlanner.ASTSearcher astSearcher = new CalcitePlanner.ASTSearcher();
    while (true) {
      astSearcher.reset();
      ASTNode setCols = astSearcher.depthFirstSearch(tree, HiveParser.TOK_SETCOLREF);
      if (setCols == null) {
        break;
      }
      processSetColsNode(setCols, astSearcher, ctx);
    }
  }

  /**
   * Replaces a spurious TOK_SETCOLREF added by parser with column names referring to the query
   * in e.g. a union. This is to maintain the expectations that some code, like order by position
   * alias, might have about not having ALLCOLREF. If it cannot find the columns with confidence
   * it will just replace SETCOLREF with ALLCOLREF. Most of the cases where that happens are
   * easy to work around in the query (e.g. by adding column aliases in the union).
   * @param setCols TOK_SETCOLREF ASTNode.
   * @param searcher AST searcher to reuse.
   */
  private static void processSetColsNode(ASTNode setCols, ASTSearcher searcher, Context ctx) {
    searcher.reset();
    CommonTree rootNode = setCols;
    while (rootNode != null && rootNode.getType() != HiveParser.TOK_INSERT) {
      rootNode = rootNode.parent;
    }
    if (rootNode == null || rootNode.parent == null) {
      // Couldn't find the parent insert; replace with ALLCOLREF.
      LOG.debug("Replacing SETCOLREF with ALLCOLREF because we couldn't find the root INSERT");
      setCols.token.setType(HiveParser.TOK_ALLCOLREF);
      return;
    }
    rootNode = rootNode.parent; // TOK_QUERY above insert
    Tree fromNode = null;
    for (int j = 0; j < rootNode.getChildCount(); ++j) {
      Tree child = rootNode.getChild(j);
      if (child.getType() == HiveParser.TOK_FROM) {
        fromNode = child;
        break;
      }
    }
    if (!(fromNode instanceof ASTNode)) {
      // Couldn't find the from that contains subquery; replace with ALLCOLREF.
      LOG.debug("Replacing SETCOLREF with ALLCOLREF because we couldn't find the FROM");
      setCols.token.setType(HiveParser.TOK_ALLCOLREF);
      return;
    }
    // We are making what we are trying to do more explicit if there's a union alias; so
    // that if we do something we didn't expect to do, it'd be more likely to fail.
    String alias = null;
    if (fromNode.getChildCount() > 0) {
      Tree fromWhat = fromNode.getChild(0);
      if (fromWhat.getType() == HiveParser.TOK_SUBQUERY && fromWhat.getChildCount() > 1) {
        Tree child = fromWhat.getChild(fromWhat.getChildCount() - 1);
        if (child.getType() == HiveParser.Identifier) {
          alias = child.getText();
        }
      }
    }
    // Note: we assume that this isn't an already malformed query;
    //       we don't check for that here - it will fail later anyway.
    // First, we find the SELECT closest to the top.
    ASTNode select = searcher.simpleBreadthFirstSearchAny((ASTNode)fromNode,
        HiveParser.TOK_SELECT, HiveParser.TOK_SELECTDI);
    if (select == null) {
      // Couldn't find the from that contains subquery; replace with ALLCOLREF.
      LOG.debug("Replacing SETCOLREF with ALLCOLREF because we couldn't find the SELECT");
      setCols.token.setType(HiveParser.TOK_ALLCOLREF);
      return;
    }

    // Then, find the leftmost logical sibling select, because that's what Hive uses for aliases.
    while (true) {
      CommonTree queryOfSelect = select.parent;
      while (queryOfSelect != null && queryOfSelect.getType() != HiveParser.TOK_QUERY) {
        queryOfSelect = queryOfSelect.parent;
      }
      // We should have some QUERY; and also its parent because by supposition we are in subq.
      if (queryOfSelect == null || queryOfSelect.parent == null) {
        LOG.debug("Replacing SETCOLREF with ALLCOLREF because we couldn't find the QUERY");
        setCols.token.setType(HiveParser.TOK_ALLCOLREF);
        return;
      }
      if (queryOfSelect.childIndex == 0) {
        break; // We are the left-most child.
      }
      Tree moreToTheLeft = queryOfSelect.parent.getChild(0);
      Preconditions.checkState(moreToTheLeft != queryOfSelect);
      ASTNode newSelect = searcher.simpleBreadthFirstSearchAny((ASTNode)moreToTheLeft,
          HiveParser.TOK_SELECT, HiveParser.TOK_SELECTDI);
      Preconditions.checkState(newSelect != select);
      select = newSelect;
      // Repeat the procedure for the new select.
    }

    // Find the proper columns.
    List<ASTNode> newChildren = new ArrayList<>(select.getChildCount());
    Set<String> aliases = new HashSet<>();
    for (int i = 0; i < select.getChildCount(); ++i) {
      Tree selExpr = select.getChild(i);
      if (selExpr.getType() == HiveParser.QUERY_HINT) {
        continue;
      }
      assert selExpr.getType() == HiveParser.TOK_SELEXPR;
      assert selExpr.getChildCount() > 0;
      // we can have functions which generate multiple aliases (e.g. explode(map(x, y)) as (key, val))
      boolean isFunctionWithMultipleParameters =
          selExpr.getChild(0).getType() == HiveParser.TOK_FUNCTION && selExpr.getChildCount() > 2;
      // if so let's skip the function token buth then examine all its parameters - otherwise check only the last item
      int start = isFunctionWithMultipleParameters ? 1 : selExpr.getChildCount() - 1;
      for (int j = start; j < selExpr.getChildCount(); ++j) {
        Tree child = selExpr.getChild(j);
        switch (child.getType()) {
        case HiveParser.TOK_SETCOLREF:
          // We have a nested setcolref. Process that and start from scratch TODO: use stack?
          processSetColsNode((ASTNode) child, searcher, ctx);
          processSetColsNode(setCols, searcher, ctx);
          return;
        case HiveParser.TOK_ALLCOLREF:
          // We should find an alias of this insert and do (alias).*. This however won't fix e.g.
          // positional order by alias case, cause we'd still have a star on the top level. Bail.
          LOG.debug("Replacing SETCOLREF with ALLCOLREF because of nested ALLCOLREF");
          setCols.token.setType(HiveParser.TOK_ALLCOLREF);
          return;
        case HiveParser.TOK_TABLE_OR_COL:
          Tree idChild = child.getChild(0);
          assert idChild.getType() == HiveParser.Identifier : idChild;
          if (!createChildColumnRef(idChild, alias, newChildren, aliases, ctx)) {
            setCols.token.setType(HiveParser.TOK_ALLCOLREF);
            return;
          }
          break;
        case HiveParser.Identifier:
          if (!createChildColumnRef(child, alias, newChildren, aliases, ctx)) {
            setCols.token.setType(HiveParser.TOK_ALLCOLREF);
            return;
          }
          break;
        case HiveParser.DOT:
          Tree colChild = child.getChild(child.getChildCount() - 1);
          assert colChild.getType() == HiveParser.Identifier : colChild;
          if (!createChildColumnRef(colChild, alias, newChildren, aliases, ctx)) {
            setCols.token.setType(HiveParser.TOK_ALLCOLREF);
            return;
          }
          break;
        default:
          // Not really sure how to refer to this (or if we can).
          // TODO: We could find a different from branch for the union, that might have an alias?
          //       Or we could add an alias here to refer to, but that might break other branches.
          LOG.debug("Replacing SETCOLREF with ALLCOLREF because of the nested node "
              + child.getType() + " " + child.getText());
          setCols.token.setType(HiveParser.TOK_ALLCOLREF);
          return;
        }
      }
    }
    // Insert search in the beginning would have failed if these parents didn't exist.
    ASTNode parent = (ASTNode)setCols.parent.parent;
    int t = parent.getType();
    assert t == HiveParser.TOK_SELECT || t == HiveParser.TOK_SELECTDI : t;
    int ix = setCols.parent.childIndex;
    parent.deleteChild(ix);
    for (ASTNode node : newChildren) {
      parent.insertChild(ix++, node);
    }
  }

  private static boolean createChildColumnRef(Tree child, String alias,
      List<ASTNode> newChildren, Set<String> aliases, Context ctx) {
    String colAlias = child.getText();
    if (SemanticAnalyzer.isRegex(colAlias, (HiveConf)ctx.getConf())) {
      LOG.debug("Skip creating child column reference because of regexp used as alias: " + colAlias);
      return false;
    }
    if (!aliases.add(colAlias)) {
      // TODO: if a side of the union has 2 columns with the same name, none on the higher
      //       level can refer to them. We could change the alias in the original node.
      LOG.debug("Replacing SETCOLREF with ALLCOLREF because of duplicate alias " + colAlias);
      return false;
    }
    ASTBuilder selExpr = ASTBuilder.construct(HiveParser.TOK_SELEXPR, "TOK_SELEXPR");
    ASTBuilder toc = ASTBuilder.construct(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL");
    ASTBuilder id = ASTBuilder.construct(HiveParser.Identifier, colAlias);
    if (alias == null) {
      selExpr = selExpr.add(toc.add(id));
    } else {
      ASTBuilder dot = ASTBuilder.construct(HiveParser.DOT, ".");
      ASTBuilder aliasNode = ASTBuilder.construct(HiveParser.Identifier, alias);
      selExpr = selExpr.add(dot.add(toc.add(aliasNode)).add(id));
    }
    newChildren.add(selExpr.node());
    return true;
  }

  public static String getKeywords(Set<String> excludes) {
    StringBuilder sb = new StringBuilder();
    for (Field f : HiveLexer.class.getDeclaredFields()) {
      if (!Modifier.isStatic(f.getModifiers())) {
        continue;
      }
      String name = f.getName();
      if (!name.startsWith("KW_")) {
        continue;
      }
      name = name.substring(3);
      if (excludes != null && excludes.contains(name)) {
        continue;
      }
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(name);
    }
    return sb.toString();
  }

  public static CBOPlan parseQuery(Context ctx, String viewQuery)
      throws SemanticException, ParseException {
    final ASTNode ast = parse(viewQuery, ctx);
    final CalcitePlanner analyzer = getAnalyzer((HiveConf) ctx.getConf(), ctx);
    RelNode logicalPlan = analyzer.genLogicalPlan(ast);
    return new CBOPlan(
        ast, logicalPlan, analyzer.getMaterializationValidationResult().getSupportedRewriteAlgorithms());
  }

  public static List<FieldSchema> parseQueryAndGetSchema(HiveConf conf, String viewQuery)
      throws SemanticException, ParseException {
    final Context ctx = new Context(conf);
    ctx.setIsLoadingMaterializedView(true);
    final ASTNode ast = parse(viewQuery, ctx);
    final CalcitePlanner analyzer = getAnalyzer(conf, ctx);
    analyzer.analyze(ast, ctx);
    return analyzer.getResultSchema();
  }

  private static CalcitePlanner getAnalyzer(HiveConf conf, Context ctx) throws SemanticException {
    final QueryState qs = new QueryState.Builder().withHiveConf(conf).build();
    final CalcitePlanner analyzer = new CalcitePlanner(qs);
    analyzer.initCtx(ctx);
    analyzer.init(false);
    return analyzer;
  }

  /**
   * Get the partition specs from the tree. This stores the full specification
   * with the comparator operator into the output list.
   *
   * @return Map of partitions by prefix length. Most of the time prefix length will
   *         be the same for all partition specs, so we can just OR the expressions.
   */
  public static Map<Integer, List<ExprNodeGenericFuncDesc>> getFullPartitionSpecs(
      CommonTree ast, Table table, Configuration conf, boolean canGroupExprs) throws SemanticException {
    String defaultPartitionName = HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULT_PARTITION_NAME);
    Map<String, String> colTypes = new HashMap<>();
    List<FieldSchema> partitionKeys = table.getStorageHandler() != null && table.getStorageHandler().alwaysUnpartitioned() ?
            table.getStorageHandler().getPartitionKeys(table) : table.getPartitionKeys();
    for (FieldSchema fs : partitionKeys) {
      colTypes.put(fs.getName().toLowerCase(), fs.getType());
    }

    Map<Integer, List<ExprNodeGenericFuncDesc>> result = new HashMap<>();
    for (int childIndex = 0; childIndex < ast.getChildCount(); childIndex++) {
      Tree partSpecTree = ast.getChild(childIndex);
      if (partSpecTree.getType() != HiveParser.TOK_PARTSPEC) {
        continue;
      }

      ExprNodeGenericFuncDesc expr = null;
      Set<String> names = new HashSet<>(partSpecTree.getChildCount());
      for (int i = 0; i < partSpecTree.getChildCount(); ++i) {
        CommonTree partSpecSingleKey = (CommonTree) partSpecTree.getChild(i);
        assert (partSpecSingleKey.getType() == HiveParser.TOK_PARTVAL);
        String key = stripIdentifierQuotes(partSpecSingleKey.getChild(0).getText()).toLowerCase();
        String operator = partSpecSingleKey.getChild(1).getText();
        ASTNode partValNode = (ASTNode)partSpecSingleKey.getChild(2);
        TypeCheckCtx typeCheckCtx = new TypeCheckCtx(null);
        ExprNodeConstantDesc valExpr =
            (ExprNodeConstantDesc) ExprNodeTypeCheck.genExprNode(partValNode, typeCheckCtx).get(partValNode);
        Object val = valExpr.getValue();

        boolean isDefaultPartitionName = val.equals(defaultPartitionName);

        String type = colTypes.get(key);
        if (type == null) {
          throw new SemanticException("Column " + key + " is not a partition key");
        }
        PrimitiveTypeInfo pti = TypeInfoFactory.getPrimitiveTypeInfo(type);
        // Create the corresponding hive expression to filter on partition columns.
        if (!isDefaultPartitionName) {
          if (!valExpr.getTypeString().equals(type)) {
            ObjectInspectorConverters.Converter converter = ObjectInspectorConverters.getConverter(
                TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(valExpr.getTypeInfo()),
                TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(pti));
            val = converter.convert(valExpr.getValue());
          }
        }

        ExprNodeColumnDesc column = new ExprNodeColumnDesc(pti, key, null, true);
        ExprNodeGenericFuncDesc op;
        if (!isDefaultPartitionName) {
          op = PartitionUtils.makeBinaryPredicate(operator, column, new ExprNodeConstantDesc(pti, val));
        } else {
          GenericUDF originalOp = FunctionRegistry.getFunctionInfo(operator).getGenericUDF();
          String fnName;
          if (FunctionRegistry.isEq(originalOp)) {
            fnName = "isnull";
          } else if (FunctionRegistry.isNeq(originalOp)) {
            fnName = "isnotnull";
          } else {
            throw new SemanticException(
                "Cannot use " + operator + " in a default partition spec; only '=' and '!=' are allowed.");
          }
          op = PartitionUtils.makeUnaryPredicate(fnName, column);
        }
        // If it's multi-expr filter (e.g. a='5', b='2012-01-02'), AND with previous exprs.
        expr = (expr == null) ? op : PartitionUtils.makeBinaryPredicate("and", expr, op);
        names.add(key);
      }

      if (expr == null) {
        continue;
      }

      // We got the expr for one full partition spec. Determine the prefix length.
      int prefixLength = calculatePartPrefix(table, names);
      List<ExprNodeGenericFuncDesc> orExpr = result.get(prefixLength);
      // We have to tell apart partitions resulting from spec with different prefix lengths.
      // So, if we already have smth for the same prefix length, we can OR the two.
      // If we don't, create a new separate filter. In most cases there will only be one.
      if (orExpr == null) {
        result.put(prefixLength, Lists.newArrayList(expr));
      } else if (canGroupExprs) {
        orExpr.set(0, PartitionUtils.makeBinaryPredicate("or", expr, orExpr.get(0)));
      } else {
        orExpr.add(expr);
      }
    }
    return result;
  }

  /**
   * Calculates the partition prefix length based on the drop spec.
   * This is used to avoid deleting archived partitions with lower level.
   * For example, if, for A and B key cols, drop spec is A=5, B=6, we shouldn't drop
   * archived A=5/, because it can contain B-s other than 6.
   */
  private static int calculatePartPrefix(Table tbl, Set<String> partSpecKeys) {
    int partPrefixToDrop = 0;
    for (FieldSchema fs : tbl.getPartCols()) {
      if (!partSpecKeys.contains(fs.getName())) {
        break;
      }
      ++partPrefixToDrop;
    }
    return partPrefixToDrop;
  }

  public static String stripIdentifierQuotes(String val) {
    if ((val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`')) {
      val = val.substring(1, val.length() - 1);
    }
    return val;
  }

  public static ReparseResult parseRewrittenQuery(Context ctx, StringBuilder rewrittenQueryStr)
      throws SemanticException {
    return parseRewrittenQuery(ctx, rewrittenQueryStr.toString());
  }

  /**
   * Parse the newly generated SQL statement to get a new AST.
   */
  public static ReparseResult parseRewrittenQuery(Context ctx,
      String rewrittenQueryStr)
      throws SemanticException {
    // Set dynamic partitioning to nonstrict so that queries do not need any partition
    // references.
    // TODO: this may be a perf issue as it prevents the optimizer.. or not
    HiveConf.setVar(ctx.getConf(), HiveConf.ConfVars.DYNAMIC_PARTITIONING_MODE, "nonstrict");
    // Disable LLAP IO wrapper; doesn't propagate extra ACID columns correctly.
    HiveConf.setBoolVar(ctx.getConf(), HiveConf.ConfVars.LLAP_IO_ROW_WRAPPER_ENABLED, false);
    // Parse the rewritten query string
    Context rewrittenCtx;
    rewrittenCtx = new Context(ctx.getConf());
    rewrittenCtx.setHDFSCleanup(true);
    // We keep track of all the contexts that are created by this query
    // so we can clear them when we finish execution
    ctx.addSubContext(rewrittenCtx);
    rewrittenCtx.setExplainConfig(ctx.getExplainConfig());
    rewrittenCtx.setExplainPlan(ctx.isExplainPlan());
    rewrittenCtx.setStatsSource(ctx.getStatsSource());
    rewrittenCtx.setPlanMapper(ctx.getPlanMapper());
    rewrittenCtx.setIsUpdateDeleteMerge(true);
    rewrittenCtx.setCmd(rewrittenQueryStr);

    ASTNode rewrittenTree;
    try {
      LOG.info("Going to reparse <{}> as \n<{}>", ctx.getCmd(), rewrittenQueryStr);
      rewrittenTree = ParseUtils.parse(rewrittenQueryStr, rewrittenCtx);
    } catch (ParseException e) {
      throw new SemanticException(ErrorMsg.UPDATEDELETE_PARSE_ERROR.getMsg(), e);
    }
    return new ReparseResult(rewrittenTree, rewrittenCtx);
  }

  public static final class ReparseResult {
    public final ASTNode rewrittenTree;
    public final Context rewrittenCtx;
    ReparseResult(ASTNode n, Context c) {
      rewrittenTree = n;
      rewrittenCtx = c;
    }
  }

  public static TypeInfo getComplexTypeTypeInfo(ASTNode typeNode) throws SemanticException {
    switch (typeNode.getType()) {
      case HiveParser.TOK_LIST:
        ListTypeInfo listTypeInfo = new ListTypeInfo();
        listTypeInfo.setListElementTypeInfo(getComplexTypeTypeInfo((ASTNode) typeNode.getChild(0)));
        return listTypeInfo;
      case HiveParser.TOK_MAP:
        MapTypeInfo mapTypeInfo = new MapTypeInfo();
        String keyTypeString = getTypeStringFromAST((ASTNode) typeNode.getChild(0));
        mapTypeInfo.setMapKeyTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo(keyTypeString));
        mapTypeInfo.setMapValueTypeInfo(getComplexTypeTypeInfo((ASTNode) typeNode.getChild(1)));
        return mapTypeInfo;
      case HiveParser.TOK_STRUCT:
        StructTypeInfo structTypeInfo = new StructTypeInfo();
        Map<String, TypeInfo> fields = collectStructFieldNames(typeNode);
        structTypeInfo.setAllStructFieldNames(new ArrayList<>(fields.keySet()));
        structTypeInfo.setAllStructFieldTypeInfos(new ArrayList<>(fields.values()));
        return structTypeInfo;
      default:
        String typeString = getTypeStringFromAST(typeNode);
        return TypeInfoFactory.getPrimitiveTypeInfo(typeString);
    }
  }

  private static Map<String, TypeInfo> collectStructFieldNames(ASTNode structTypeNode) throws SemanticException {
    ASTNode fieldListNode = (ASTNode) structTypeNode.getChild(0);
    assert fieldListNode.getType() == HiveParser.TOK_TABCOLLIST;

    Map<String, TypeInfo> result = new LinkedHashMap<>(fieldListNode.getChildCount());
    for (int i = 0; i < fieldListNode.getChildCount(); i++) {
      ASTNode child = (ASTNode) fieldListNode.getChild(i);

      String attributeIdentifier = unescapeIdentifier(child.getChild(0).getText());
      if (result.containsKey(attributeIdentifier)) {
        throw new SemanticException(ErrorMsg.AMBIGUOUS_STRUCT_ATTRIBUTE, attributeIdentifier);
      } else {
        result.put(attributeIdentifier, getComplexTypeTypeInfo((ASTNode) child.getChild(1)));
      }
    }
    return result;
  }
}
