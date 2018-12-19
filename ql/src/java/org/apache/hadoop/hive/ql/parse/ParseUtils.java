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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTBuilder;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner.ASTSearcher;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Library of utility functions used in the parse code.
 *
 */
public final class ParseUtils {
  /** Parses the Hive query. */
  private static final Logger LOG = LoggerFactory.getLogger(ParseUtils.class);
  public static ASTNode parse(String command) throws ParseException {
    return parse(command, null);
  }

  /** Parses the Hive query. */
  public static ASTNode parse(String command, Context ctx) throws ParseException {
    return parse(command, ctx, null);
  }

  /** Parses the Hive query. */
  public static ASTNode parse(
      String command, Context ctx, String viewFullyQualifiedName) throws ParseException {
    ParseDriver pd = new ParseDriver();
    ASTNode tree = pd.parse(command, ctx, viewFullyQualifiedName);
    tree = findRootNonNullToken(tree);
    handleSetColRefs(tree);
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

  /**
   * @param column  column expression to convert
   * @param tableFieldTypeInfo TypeInfo to convert to
   * @return Expression converting column to the type specified by tableFieldTypeInfo
   */
  public static ExprNodeDesc createConversionCast(ExprNodeDesc column, PrimitiveTypeInfo tableFieldTypeInfo)
      throws SemanticException {
    // Get base type, since type string may be parameterized
    String baseType = TypeInfoUtils.getBaseName(tableFieldTypeInfo.getTypeName());

    // If the type cast UDF is for a parameterized type, then it should implement
    // the SettableUDF interface so that we can pass in the params.
    // Not sure if this is the cleanest solution, but there does need to be a way
    // to provide the type params to the type cast.
    return TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDescWithUdfData(baseType,
        tableFieldTypeInfo, column);
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
      String tableOrCol = SemanticAnalyzer.unescapeIdentifier(filterCondn.getChild(0).getText()
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
        if ( cIdx != idx ) {
          if ( idx != -1 && cIdx != -1 ) {
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

    public static boolean sameTree(ASTNode node, ASTNode otherNode) {
      if (node == null && otherNode == null) {
        return true;
      }
      if ((node == null && otherNode != null) ||
              (node != null && otherNode == null)) {
        return false;
      }

      Stack<Tree> stack = new Stack<Tree>();
      stack.push(node);
      Stack<Tree> otherStack = new Stack<Tree>();
      otherStack.push(otherNode);

      while (!stack.empty() && !otherStack.empty()) {
        Tree p = stack.pop();
        Tree otherP = otherStack.pop();

        if (p.isNil() != otherP.isNil()) {
          return false;
        }
        if (!p.isNil()) {
          if (!p.toString().equals(otherP.toString())) {
            return false;
          }
        }
        if (p.getChildCount() != otherP.getChildCount()) {
          return false;
        }
        for (int i = p.getChildCount()-1; i >= 0; i--) {
          Tree t = p.getChild(i);
          stack.push(t);
          Tree otherT = otherP.getChild(i);
          otherStack.push(otherT);
        }
      }

      return stack.empty() && otherStack.empty();
    }


    private static void handleSetColRefs(ASTNode tree) {
      CalcitePlanner.ASTSearcher astSearcher = new CalcitePlanner.ASTSearcher();
      while (true) {
        astSearcher.reset();
        ASTNode setCols = astSearcher.depthFirstSearch(tree, HiveParser.TOK_SETCOLREF);
        if (setCols == null) break;
        processSetColsNode(setCols, astSearcher);
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
    private static void processSetColsNode(ASTNode setCols, ASTSearcher searcher) {
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
        if (queryOfSelect.childIndex == 0) break; // We are the left-most child.
        Tree moreToTheLeft = queryOfSelect.parent.getChild(0);
        Preconditions.checkState(moreToTheLeft != queryOfSelect);
        ASTNode newSelect = searcher.simpleBreadthFirstSearchAny((ASTNode)moreToTheLeft,
          HiveParser.TOK_SELECT, HiveParser.TOK_SELECTDI);
        Preconditions.checkState(newSelect != select);
        select = newSelect;
        // Repeat the procedure for the new select.
      }

      // Found the proper columns.
      List<ASTNode> newChildren = new ArrayList<>(select.getChildCount());
      HashSet<String> aliases = new HashSet<>();
      for (int i = 0; i < select.getChildCount(); ++i) {
        Tree selExpr = select.getChild(i);
        if (selExpr.getType() == HiveParser.QUERY_HINT) continue;
        assert selExpr.getType() == HiveParser.TOK_SELEXPR;
        assert selExpr.getChildCount() > 0;
        // Examine the last child. It could be an alias.
        Tree child = selExpr.getChild(selExpr.getChildCount() - 1);
        switch (child.getType()) {
        case HiveParser.TOK_SETCOLREF:
          // We have a nested setcolref. Process that and start from scratch TODO: use stack?
          processSetColsNode((ASTNode)child, searcher);
          processSetColsNode(setCols, searcher);
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
          if (!createChildColumnRef(idChild, alias, newChildren, aliases)) {
            setCols.token.setType(HiveParser.TOK_ALLCOLREF);
            return;
          }
          break;
        case HiveParser.Identifier:
          if (!createChildColumnRef(child, alias, newChildren, aliases)) {
            setCols.token.setType(HiveParser.TOK_ALLCOLREF);
            return;
          }
          break;
        case HiveParser.DOT: {
          Tree colChild = child.getChild(child.getChildCount() - 1);
          assert colChild.getType() == HiveParser.Identifier : colChild;
          if (!createChildColumnRef(colChild, alias, newChildren, aliases)) {
            setCols.token.setType(HiveParser.TOK_ALLCOLREF);
            return;
          }
          break;
        }
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
        List<ASTNode> newChildren, HashSet<String> aliases) {
      String colAlias = child.getText();
      if (!aliases.add(colAlias)) {
        // TODO: if a side of the union has 2 columns with the same name, noone on the higher
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
        if (!Modifier.isStatic(f.getModifiers())) continue;
        String name = f.getName();
        if (!name.startsWith("KW_")) continue;
        name = name.substring(3);
        if (excludes != null && excludes.contains(name)) continue;
        if (sb.length() > 0) {
          sb.append(",");
        }
        sb.append(name);
      }
      return sb.toString();
    }

  public static RelNode parseQuery(HiveConf conf, String viewQuery)
      throws SemanticException, IOException, ParseException {
    final Context ctx = new Context(conf);
    ctx.setIsLoadingMaterializedView(true);
    final ASTNode ast = parse(viewQuery, ctx);
    final CalcitePlanner analyzer = getAnalyzer(conf, ctx);
    return analyzer.genLogicalPlan(ast);
  }

  public static List<FieldSchema> parseQueryAndGetSchema(HiveConf conf, String viewQuery)
      throws SemanticException, IOException, ParseException {
    final Context ctx = new Context(conf);
    ctx.setIsLoadingMaterializedView(true);
    final ASTNode ast = parse(viewQuery, ctx);
    final CalcitePlanner analyzer = getAnalyzer(conf, ctx);
    analyzer.genLogicalPlan(ast);
    return analyzer.getResultSchema();
  }

  private static CalcitePlanner getAnalyzer(HiveConf conf, Context ctx) throws SemanticException {
    final QueryState qs = new QueryState.Builder().withHiveConf(conf).build();
    final CalcitePlanner analyzer = new CalcitePlanner(qs);
    analyzer.initCtx(ctx);
    analyzer.init(false);
    return analyzer;
  }
}
