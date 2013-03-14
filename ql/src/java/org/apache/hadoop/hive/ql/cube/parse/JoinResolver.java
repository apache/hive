package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.JoinCond;
import org.apache.hadoop.hive.ql.parse.JoinType;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class JoinResolver implements ContextRewriter {

  private static final Log LOG = LogFactory.getLog(JoinResolver.class);

  public JoinResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    resolveJoins((CubeQueryContext)cubeql);
  }

  public void resolveJoins(CubeQueryContext cubeql) throws SemanticException {
    QB cubeQB = cubeql.getQB();
    if (cubeQB.getParseInfo().getJoinExpr() != null) {
      cubeQB.setQbJoinTree(genJoinTree(cubeQB,
          cubeQB.getParseInfo().getJoinExpr()));
    } else {
      LOG.info("No join expr available");
    }
  }

  private QBJoinTree genJoinTree(QB qb, ASTNode joinParseTree)
      throws SemanticException {
    QBJoinTree joinTree = new QBJoinTree();
    JoinCond[] condn = new JoinCond[1];

    switch (joinParseTree.getToken().getType()) {
    case HiveParser.TOK_LEFTOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTOUTER);
      break;
    case HiveParser.TOK_RIGHTOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.RIGHTOUTER);
      break;
    case HiveParser.TOK_FULLOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.FULLOUTER);
      break;
    case HiveParser.TOK_LEFTSEMIJOIN:
      joinTree.setNoSemiJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTSEMI);
      break;
    default:
      condn[0] = new JoinCond(0, 1, JoinType.INNER);
      joinTree.setNoOuterJoin(true);
      break;
    }

    joinTree.setJoinCond(condn);

    ASTNode left = (ASTNode) joinParseTree.getChild(0);
    ASTNode right = (ASTNode) joinParseTree.getChild(1);

    if ((left.getToken().getType() == HiveParser.TOK_TABREF)
        || (left.getToken().getType() == HiveParser.TOK_SUBQUERY)) {
      String tableName = SemanticAnalyzer.getUnescapedUnqualifiedTableName(
          (ASTNode) left.getChild(0))
          .toLowerCase();
      String alias = left.getChildCount() == 1 ? tableName
          : SemanticAnalyzer.unescapeIdentifier(
              left.getChild(left.getChildCount() - 1)
              .getText().toLowerCase());
      joinTree.setLeftAlias(alias);
      String[] leftAliases = new String[1];
      leftAliases[0] = alias;
      joinTree.setLeftAliases(leftAliases);
      String[] children = new String[2];
      children[0] = alias;
      joinTree.setBaseSrc(children);
    } else if (isJoinToken(left)) {
      QBJoinTree leftTree = genJoinTree(qb, left);
      joinTree.setJoinSrc(leftTree);
      String[] leftChildAliases = leftTree.getLeftAliases();
      String leftAliases[] = new String[leftChildAliases.length + 1];
      for (int i = 0; i < leftChildAliases.length; i++) {
        leftAliases[i] = leftChildAliases[i];
      }
      leftAliases[leftChildAliases.length] = leftTree.getRightAliases()[0];
      joinTree.setLeftAliases(leftAliases);
    } else {
      assert (false);
    }

    if ((right.getToken().getType() == HiveParser.TOK_TABREF)
        || (right.getToken().getType() == HiveParser.TOK_SUBQUERY)) {
      String tableName = SemanticAnalyzer.getUnescapedUnqualifiedTableName(
          (ASTNode) right.getChild(0)).toLowerCase();
      String alias = right.getChildCount() == 1 ? tableName
          : SemanticAnalyzer.unescapeIdentifier(right.getChild(
              right.getChildCount() - 1).getText().toLowerCase());
      String[] rightAliases = new String[1];
      rightAliases[0] = alias;
      joinTree.setRightAliases(rightAliases);
      String[] children = joinTree.getBaseSrc();
      if (children == null) {
        children = new String[2];
      }
      children[1] = alias;
      joinTree.setBaseSrc(children);
      // remember rhs table for semijoin
      if (joinTree.getNoSemiJoin() == false) {
        joinTree.addRHSSemijoin(alias);
      }
    } else {
      assert false;
    }

    ArrayList<ArrayList<ASTNode>> expressions = new ArrayList<
        ArrayList<ASTNode>>();
    expressions.add(new ArrayList<ASTNode>());
    expressions.add(new ArrayList<ASTNode>());
    joinTree.setExpressions(expressions);

    ArrayList<Boolean> nullsafes = new ArrayList<Boolean>();
    joinTree.setNullSafes(nullsafes);

    ArrayList<ArrayList<ASTNode>> filters = new ArrayList<ArrayList<ASTNode>>();
    filters.add(new ArrayList<ASTNode>());
    filters.add(new ArrayList<ASTNode>());
    joinTree.setFilters(filters);

    ArrayList<ArrayList<ASTNode>> filtersForPushing =
        new ArrayList<ArrayList<ASTNode>>();
    filtersForPushing.add(new ArrayList<ASTNode>());
    filtersForPushing.add(new ArrayList<ASTNode>());
    joinTree.setFiltersForPushing(filtersForPushing);

    ASTNode joinCond = (ASTNode) joinParseTree.getChild(2);
    ArrayList<String> leftSrc = new ArrayList<String>();
    parseJoinCondition(joinTree, joinCond, leftSrc);
    if (leftSrc.size() == 1) {
      joinTree.setLeftAlias(leftSrc.get(0));
    }

    // check the hints to see if the user has specified a map-side join. This
    // will be removed later on, once the cost-based
    // infrastructure is in place
    if (qb.getParseInfo().getHints() != null) {
      List<String> mapSideTables = getMapSideJoinTables(qb);
      List<String> mapAliases = joinTree.getMapAliases();

      for (String mapTbl : mapSideTables) {
        boolean mapTable = false;
        for (String leftAlias : joinTree.getLeftAliases()) {
          if (mapTbl.equalsIgnoreCase(leftAlias)) {
            mapTable = true;
          }
        }
        for (String rightAlias : joinTree.getRightAliases()) {
          if (mapTbl.equalsIgnoreCase(rightAlias)) {
            mapTable = true;
          }
        }

        if (mapTable) {
          if (mapAliases == null) {
            mapAliases = new ArrayList<String>();
          }
          mapAliases.add(mapTbl);
          joinTree.setMapSideJoin(true);
        }
      }

      joinTree.setMapAliases(mapAliases);

      parseStreamTables(joinTree, qb);
    }

    return joinTree;
  }

  private List<String> getMapSideJoinTables(QB qb) {
    List<String> cols = new ArrayList<String>();
    ASTNode hints = qb.getParseInfo().getHints();
    for (int pos = 0; pos < hints.getChildCount(); pos++) {
      ASTNode hint = (ASTNode) hints.getChild(pos);
      if (((ASTNode) hint.getChild(0)).getToken().getType()
          == HiveParser.TOK_MAPJOIN) {
        ASTNode hintTblNames = (ASTNode) hint.getChild(1);
        int numCh = hintTblNames.getChildCount();
        for (int tblPos = 0; tblPos < numCh; tblPos++) {
          String tblName = ((ASTNode) hintTblNames.getChild(tblPos)).getText()
              .toLowerCase();
          if (!cols.contains(tblName)) {
            cols.add(tblName);
          }
        }
      }
    }
    return cols;
  }

  private boolean isJoinToken(ASTNode node) {
    if ((node.getToken().getType() == HiveParser.TOK_JOIN)
        || (node.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN)
        || (node.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN)
        || (node.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN)
        || (node.getToken().getType() == HiveParser.TOK_LEFTSEMIJOIN)
        || (node.getToken().getType() == HiveParser.TOK_UNIQUEJOIN)) {
      return true;
    }
    return false;
  }

  private void parseJoinCondition(QBJoinTree joinTree, ASTNode joinCond,
      ArrayList<String> leftSrc) throws SemanticException {
    if (joinCond == null) {
      return;
    }

    JoinType type = joinTree.getJoinCond()[0].getJoinType();
    switch (joinCond.getToken().getType()) {
    case HiveParser.KW_OR:
      throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_3
          .getMsg(joinCond));

    case HiveParser.KW_AND:
      parseJoinCondition(joinTree, (ASTNode) joinCond.getChild(0), leftSrc);
      parseJoinCondition(joinTree, (ASTNode) joinCond.getChild(1), leftSrc);
      break;

    case HiveParser.EQUAL_NS:
    case HiveParser.EQUAL:
      ASTNode leftCondn = (ASTNode) joinCond.getChild(0);
      ArrayList<String> leftCondAl1 = new ArrayList<String>();
      ArrayList<String> leftCondAl2 = new ArrayList<String>();
      parseJoinCondPopulateAlias(joinTree, leftCondn, leftCondAl1, leftCondAl2,
          null);

      ASTNode rightCondn = (ASTNode) joinCond.getChild(1);
      ArrayList<String> rightCondAl1 = new ArrayList<String>();
      ArrayList<String> rightCondAl2 = new ArrayList<String>();
      parseJoinCondPopulateAlias(joinTree, rightCondn, rightCondAl1,
          rightCondAl2, null);

      // is it a filter or a join condition
      // if it is filter see if it can be pushed above the join
      // filter cannot be pushed if
      // * join is full outer or
      // * join is left outer and filter is on left alias or
      // * join is right outer and filter is on right alias
      if (((leftCondAl1.size() != 0) && (leftCondAl2.size() != 0))
          || ((rightCondAl1.size() != 0) && (rightCondAl2.size() != 0))) {
        throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1
            .getMsg(joinCond));
      }

      if (leftCondAl1.size() != 0) {
        if ((rightCondAl1.size() != 0)
            || ((rightCondAl1.size() == 0) && (rightCondAl2.size() == 0))) {
          if (type.equals(JoinType.LEFTOUTER) ||
              type.equals(JoinType.FULLOUTER)) {
            joinTree.getFilters().get(0).add(joinCond);
          } else {
            joinTree.getFiltersForPushing().get(0).add(joinCond);
          }
        } else if (rightCondAl2.size() != 0) {
          populateAliases(leftCondAl1, leftCondAl2, leftCondn, joinTree,
              leftSrc);
          populateAliases(rightCondAl1, rightCondAl2, rightCondn, joinTree,
              leftSrc);
          boolean nullsafe = joinCond.getToken().getType() == HiveParser.EQUAL_NS;
          joinTree.getNullSafes().add(nullsafe);
        }
      } else if (leftCondAl2.size() != 0) {
        if ((rightCondAl2.size() != 0)
            || ((rightCondAl1.size() == 0) && (rightCondAl2.size() == 0))) {
          if (type.equals(JoinType.RIGHTOUTER)
              || type.equals(JoinType.FULLOUTER)) {
            joinTree.getFilters().get(1).add(joinCond);
          } else {
            joinTree.getFiltersForPushing().get(1).add(joinCond);
          }
        } else if (rightCondAl1.size() != 0) {
          populateAliases(leftCondAl1, leftCondAl2, leftCondn, joinTree,
              leftSrc);
          populateAliases(rightCondAl1, rightCondAl2, rightCondn, joinTree,
              leftSrc);
          boolean nullsafe = joinCond.getToken().getType() ==
              HiveParser.EQUAL_NS;
          joinTree.getNullSafes().add(nullsafe);
        }
      } else if (rightCondAl1.size() != 0) {
        if (type.equals(JoinType.LEFTOUTER)
            || type.equals(JoinType.FULLOUTER)) {
          joinTree.getFilters().get(0).add(joinCond);
        } else {
          joinTree.getFiltersForPushing().get(0).add(joinCond);
        }
      } else {
        if (type.equals(JoinType.RIGHTOUTER)
            || type.equals(JoinType.FULLOUTER)) {
          joinTree.getFilters().get(1).add(joinCond);
        } else {
          joinTree.getFiltersForPushing().get(1).add(joinCond);
        }
      }

      break;

    default:
      boolean isFunction = (joinCond.getType() == HiveParser.TOK_FUNCTION);

      // Create all children
      int childrenBegin = (isFunction ? 1 : 0);
      ArrayList<ArrayList<String>> leftAlias = new ArrayList<ArrayList<String>>(
          joinCond.getChildCount() - childrenBegin);
      ArrayList<ArrayList<String>> rightAlias = new ArrayList<ArrayList<String>>(
          joinCond.getChildCount() - childrenBegin);
      for (int ci = 0; ci < joinCond.getChildCount() - childrenBegin; ci++) {
        ArrayList<String> left = new ArrayList<String>();
        ArrayList<String> right = new ArrayList<String>();
        leftAlias.add(left);
        rightAlias.add(right);
      }

      for (int ci = childrenBegin; ci < joinCond.getChildCount(); ci++) {
        parseJoinCondPopulateAlias(joinTree, (ASTNode) joinCond.getChild(ci),
            leftAlias.get(ci - childrenBegin), rightAlias.get(ci
                - childrenBegin), null);
      }

      boolean leftAliasNull = true;
      for (ArrayList<String> left : leftAlias) {
        if (left.size() != 0) {
          leftAliasNull = false;
          break;
        }
      }

      boolean rightAliasNull = true;
      for (ArrayList<String> right : rightAlias) {
        if (right.size() != 0) {
          rightAliasNull = false;
          break;
        }
      }

      if (!leftAliasNull && !rightAliasNull) {
        throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1
            .getMsg(joinCond));
      }

      if (!leftAliasNull) {
        if (type.equals(JoinType.LEFTOUTER)
            || type.equals(JoinType.FULLOUTER)) {
           joinTree.getFilters().get(0).add(joinCond);
        } else {
          joinTree.getFiltersForPushing().get(0).add(joinCond);
        }
      } else {
        if (type.equals(JoinType.RIGHTOUTER)
            || type.equals(JoinType.FULLOUTER)) {
          joinTree.getFilters().get(1).add(joinCond);
        } else {
          joinTree.getFiltersForPushing().get(1).add(joinCond);
        }
      }

      break;
    }
  }

  private boolean isPresent(String[] list, String elem) {
    for (String s : list) {
      if (s.toLowerCase().equals(elem)) {
        return true;
      }
    }
    return false;
  }

  private void parseJoinCondPopulateAlias(QBJoinTree joinTree, ASTNode condn,
      ArrayList<String> leftAliases, ArrayList<String> rightAliases,
      ArrayList<String> fields) throws SemanticException {
    // String[] allAliases = joinTree.getAllAliases();
    switch (condn.getToken().getType()) {
    case HiveParser.TOK_TABLE_OR_COL:
      String tableOrCol = SemanticAnalyzer.unescapeIdentifier(
          condn.getChild(0).getText().toLowerCase());
      if (isPresent(joinTree.getLeftAliases(), tableOrCol)) {
        if (!leftAliases.contains(tableOrCol)) {
          leftAliases.add(tableOrCol);
        }
      } else if (isPresent(joinTree.getRightAliases(), tableOrCol)) {
        if (!rightAliases.contains(tableOrCol)) {
          rightAliases.add(tableOrCol);
        }
      } else {
        // We don't support columns without table prefix in JOIN condition right
        // now.
        // We need to pass Metadata here to know which table the column belongs
        // to.
        throw new SemanticException(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(condn
            .getChild(0)));
      }
      break;

    case HiveParser.Identifier:
      // it may be a field name, return the identifier and let the caller decide
      // whether it is or not
      if (fields != null) {
        fields.add(SemanticAnalyzer.unescapeIdentifier(
            condn.getToken().getText().toLowerCase()));
      }
      break;
    case HiveParser.Number:
    case HiveParser.StringLiteral:
    case HiveParser.TOK_STRINGLITERALSEQUENCE:
    case HiveParser.TOK_CHARSETLITERAL:
    case HiveParser.KW_TRUE:
    case HiveParser.KW_FALSE:
      break;

    case HiveParser.TOK_FUNCTION:
      // check all the arguments
      for (int i = 1; i < condn.getChildCount(); i++) {
        parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(i),
            leftAliases, rightAliases, null);
      }
      break;

    default:
      // This is an operator - so check whether it is unary or binary operator
      if (condn.getChildCount() == 1) {
        parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(0),
            leftAliases, rightAliases, null);
      } else if (condn.getChildCount() == 2) {

        ArrayList<String> fields1 = null;
        // if it is a dot operator, remember the field name of the rhs of the
        // left semijoin
        if (joinTree.getNoSemiJoin() == false
            && condn.getToken().getType() == HiveParser.DOT) {
          // get the semijoin rhs table name and field name
          fields1 = new ArrayList<String>();
          int rhssize = rightAliases.size();
          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(0),
              leftAliases, rightAliases, null);
          String rhsAlias = null;

          if (rightAliases.size() > rhssize) { // the new table is rhs table
            rhsAlias = rightAliases.get(rightAliases.size() - 1);
          }
          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(1),
              leftAliases, rightAliases, fields1);
          if (rhsAlias != null && fields1.size() > 0) {
            joinTree.addRHSSemijoinColumns(rhsAlias, condn);
          }
        } else {
          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(0),
              leftAliases, rightAliases, null);
          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(1),
              leftAliases, rightAliases, fields1);
        }
      } else {
        throw new SemanticException(condn.toStringTree() + " encountered with "
            + condn.getChildCount() + " children");
      }
      break;
    }
  }

  private void populateAliases(ArrayList<String> leftAliases,
      ArrayList<String> rightAliases, ASTNode condn, QBJoinTree joinTree,
      ArrayList<String> leftSrc) throws SemanticException {
    if ((leftAliases.size() != 0) && (rightAliases.size() != 0)) {
      throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1
          .getMsg(condn));
    }
    if (rightAliases.size() != 0) {
      assert rightAliases.size() == 1;
      joinTree.getExpressions().get(1).add(condn);
    } else if (leftAliases.size() != 0) {
      joinTree.getExpressions().get(0).add(condn);
      for (String s : leftAliases) {
        if (!leftSrc.contains(s)) {
          leftSrc.add(s);
        }
      }
    } else {
      throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_2
          .getMsg(condn));
    }
  }

  private void parseStreamTables(QBJoinTree joinTree, QB qb) {
    List<String> streamAliases = joinTree.getStreamAliases();
    for (Node hintNode : qb.getParseInfo().getHints().getChildren()) {
      ASTNode hint = (ASTNode) hintNode;
      if (hint.getChild(0).getType() == HiveParser.TOK_STREAMTABLE) {
        for (int i = 0; i < hint.getChild(1).getChildCount(); i++) {
          if (streamAliases == null) {
            streamAliases = new ArrayList<String>();
          }
          streamAliases.add(hint.getChild(1).getChild(i).getText());
        }
      }
    }
    joinTree.setStreamAliases(streamAliases);
  }
}
