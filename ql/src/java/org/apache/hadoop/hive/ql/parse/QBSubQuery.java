package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.parse.SubQueryUtils.ISubQueryJoinInfo;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory.DefaultExprProcessor;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class QBSubQuery implements ISubQueryJoinInfo {
  
  public static enum SubQueryType {
    EXISTS,
    NOT_EXISTS,
    IN,
    NOT_IN;

    public static SubQueryType get(ASTNode opNode) throws SemanticException {
      switch(opNode.getType()) {
      case HiveParser.KW_EXISTS:
        return EXISTS;
      case HiveParser.TOK_SUBQUERY_OP_NOTEXISTS:
        return NOT_EXISTS;
      case HiveParser.KW_IN:
        return IN;
      case HiveParser.TOK_SUBQUERY_OP_NOTIN:
        return NOT_IN;
      default:
        throw new SemanticException(SemanticAnalyzer.generateErrorMessage(opNode,
            "Operator not supported in SubQuery use."));
      }
    }
  }

  public static class SubQueryTypeDef {
    private final ASTNode ast;
    private final SubQueryType type;

    public SubQueryTypeDef(ASTNode ast, SubQueryType type) {
      super();
      this.ast = ast;
      this.type = type;
    }

    public ASTNode getAst() {
      return ast;
    }

    public SubQueryType getType() {
      return type;
    }

  }

  /*
   * An expression is either the left/right side of an Equality predicate in the SubQuery where
   * clause; or it is the entire conjunct. For e.g. if the Where Clause for a SubQuery is:
   * where R1.X = R2.Y and R2.Z > 7
   * Then the expressions analyzed are R1.X, R2.X ( the left and right sides of the Equality
   * predicate); and R2.Z > 7.
   *
   * The ExprType tracks whether the expr:
   * - has a reference to a SubQuery table source
   * - has a reference to Outer(parent) Query table source
   */
  static enum ExprType {
    REFERS_NONE(false, false) {
      @Override
      public ExprType combine(ExprType other) {
        return other;
      }
    },
    REFERS_PARENT(true, false) {
      @Override
      public ExprType combine(ExprType other) {
        switch(other) {
        case REFERS_SUBQUERY:
        case REFERS_BOTH:
          return REFERS_BOTH;
        default:
          return this;
        }
      }
    },
    REFERS_SUBQUERY(false, true) {
      @Override
      public ExprType combine(ExprType other) {
        switch(other) {
        case REFERS_PARENT:
        case REFERS_BOTH:
          return REFERS_BOTH;
        default:
          return this;
        }
      }
    },
    REFERS_BOTH(true,true) {
      @Override
      public ExprType combine(ExprType other) {
        return this;
      }
    };

    final boolean refersParent;
    final boolean refersSubQuery;

    ExprType(boolean refersParent, boolean refersSubQuery) {
      this.refersParent = refersParent;
      this.refersSubQuery = refersSubQuery;
    }

    public boolean refersParent() {
      return refersParent;
    }
    public boolean refersSubQuery() {
      return refersSubQuery;
    }
    public abstract ExprType combine(ExprType other);
  }

  /*
   * This class captures the information about a 
   * conjunct in the where clause of the SubQuery.
   * For a equality predicate it capture for each side:
   * - the AST
   * - the type of Expression (basically what columns are referenced)
   * - for Expressions that refer the parent it captures the 
   *   parent's ColumnInfo. In case of outer Aggregation expressions
   *   we need this to introduce a new mapping in the OuterQuery
   *   RowResolver. A join condition must use qualified column references,
   *   so we generate a new name for the aggr expression and use it in the 
   *   joining condition.
   *   For e.g.
   *   having exists ( select x from R2 where y = min(R1.z) )
   *   where the expression 'min(R1.z)' is from the outer Query.
   *   We give this expression a new name like 'R1._gby_sq_col_1'
   *   and use the join condition: R1._gby_sq_col_1 = R2.y
   */
  static class Conjunct {
    private final ASTNode leftExpr;
    private final ASTNode rightExpr;
    private final ExprType leftExprType;
    private final ExprType rightExprType;
    private final ColumnInfo leftOuterColInfo;
    private final ColumnInfo rightOuterColInfo;

   Conjunct(ASTNode leftExpr, 
        ASTNode rightExpr, 
        ExprType leftExprType,
        ExprType rightExprType,
        ColumnInfo leftOuterColInfo,
        ColumnInfo rightOuterColInfo) {
      super();
      this.leftExpr = leftExpr;
      this.rightExpr = rightExpr;
      this.leftExprType = leftExprType;
      this.rightExprType = rightExprType;
      this.leftOuterColInfo = leftOuterColInfo;
      this.rightOuterColInfo = rightOuterColInfo;
    }
    ASTNode getLeftExpr() {
      return leftExpr;
    }
    ASTNode getRightExpr() {
      return rightExpr;
    }
    ExprType getLeftExprType() {
      return leftExprType;
    }
    ExprType getRightExprType() {
      return rightExprType;
    }

    boolean eitherSideRefersBoth() {
      if ( leftExprType == ExprType.REFERS_BOTH ) {
        return true;
      } else if ( rightExpr != null ) {
        return rightExprType == ExprType.REFERS_BOTH;
      }
      return false;
    }

    boolean isCorrelated() {
      if ( rightExpr != null ) {
        return leftExprType.combine(rightExprType) == ExprType.REFERS_BOTH;
      }
      return false;
    }

    boolean refersOuterOnly() {
      if ( rightExpr == null ) {
        return leftExprType == ExprType.REFERS_PARENT;
      }
      return leftExprType.combine(rightExprType) == ExprType.REFERS_PARENT;
    }
    ColumnInfo getLeftOuterColInfo() {
      return leftOuterColInfo;
    }
    ColumnInfo getRightOuterColInfo() {
      return rightOuterColInfo;
    }
  }

  class ConjunctAnalyzer {
    RowResolver parentQueryRR;
    boolean forHavingClause;
    String parentQueryNewAlias;
    NodeProcessor defaultExprProcessor;
    Stack<Node> stack;

    ConjunctAnalyzer(RowResolver parentQueryRR,
    		boolean forHavingClause,
    		String parentQueryNewAlias) {
      this.parentQueryRR = parentQueryRR;
      defaultExprProcessor = new DefaultExprProcessor();
      this.forHavingClause = forHavingClause;
      this.parentQueryNewAlias = parentQueryNewAlias;
      stack = new Stack<Node>();
    }

    /*
     * 1. On encountering a DOT, we attempt to resolve the leftmost name
     *    to the Parent Query.
     * 2. An unqualified name is assumed to be a SubQuery reference.
     *    We don't attempt to resolve this to the Parent; because
     *    we require all Parent column references to be qualified.
     * 3. All other expressions have a Type based on their children.
     *    An Expr w/o children is assumed to refer to neither.
     */
    private ObjectPair<ExprType,ColumnInfo> analyzeExpr(ASTNode expr) {
      ColumnInfo cInfo = null;
      if ( forHavingClause ) {
      	try {
      	  cInfo = parentQueryRR.getExpression(expr);
      		if ( cInfo != null) {
      		    return ObjectPair.create(ExprType.REFERS_PARENT, cInfo);
      	    }
      	} catch(SemanticException se) {
      	}
      }
      if ( expr.getType() == HiveParser.DOT) {
        ASTNode dot = firstDot(expr);
        cInfo = resolveDot(dot);
        if ( cInfo != null ) {
          return ObjectPair.create(ExprType.REFERS_PARENT, cInfo);
        }
        return ObjectPair.create(ExprType.REFERS_SUBQUERY, null);
      } else if ( expr.getType() == HiveParser.TOK_TABLE_OR_COL ) {
        return ObjectPair.create(ExprType.REFERS_SUBQUERY, null);
      } else {
        ExprType exprType = ExprType.REFERS_NONE;
        int cnt = expr.getChildCount();
        for(int i=0; i < cnt; i++) {
          ASTNode child = (ASTNode) expr.getChild(i);
          exprType = exprType.combine(analyzeExpr(child).getFirst());
        }
        return ObjectPair.create(exprType, null);
      }
    }

    /*
     * 1. The only correlation operator we check for is EQUAL; because that is
     *    the one for which we can do a Algebraic transformation.
     * 2. For expressions that are not an EQUAL predicate, we treat them as conjuncts
     *    having only 1 side. These should only contain references to the SubQuery
     *    table sources.
     * 3. For expressions that are an EQUAL predicate; we analyze each side and let the
     *    left and right exprs in the Conjunct object.
     *
     * @return Conjunct  contains details on the left and right side of the conjunct expression.
     */
    Conjunct analyzeConjunct(ASTNode conjunct) throws SemanticException {
      int type = conjunct.getType();

      if ( type == HiveParser.EQUAL ) {
        ASTNode left = (ASTNode) conjunct.getChild(0);
        ASTNode right = (ASTNode) conjunct.getChild(1);
        ObjectPair<ExprType,ColumnInfo> leftInfo = analyzeExpr(left);
        ObjectPair<ExprType,ColumnInfo> rightInfo = analyzeExpr(right);

        return new Conjunct(left, right, 
            leftInfo.getFirst(), rightInfo.getFirst(),
            leftInfo.getSecond(), rightInfo.getSecond());
      } else {
        ObjectPair<ExprType,ColumnInfo> sqExprInfo = analyzeExpr(conjunct);
        return new Conjunct(conjunct, null, 
            sqExprInfo.getFirst(), null,
            sqExprInfo.getSecond(), sqExprInfo.getSecond());
      }
    }

    /*
     * Try to resolve a qualified name as a column reference on the Parent Query's RowResolver.
     * Apply this logic on the leftmost(first) dot in an AST tree.
     */
    protected ColumnInfo resolveDot(ASTNode node) {
      try {
        TypeCheckCtx tcCtx = new TypeCheckCtx(parentQueryRR);
        String str = BaseSemanticAnalyzer.unescapeIdentifier(node.getChild(1).getText());
        ExprNodeDesc idDesc = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, str);
         ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc)
             defaultExprProcessor.process(node, stack, tcCtx, (Object) null, idDesc);
         if ( colDesc != null ) {
           String[] qualName = parentQueryRR.reverseLookup(colDesc.getColumn());
           return parentQueryRR.get(qualName[0], qualName[1]);
         }
      } catch(SemanticException se) {
      }
      return null;
    }

    /*
     * We want to resolve the leftmost name to the Parent Query's RR.
     * Hence we do a left walk down the AST, until we reach the bottom most DOT.
     */
    protected ASTNode firstDot(ASTNode dot) {
      ASTNode firstChild = (ASTNode) dot.getChild(0);
      if ( firstChild != null && firstChild.getType() == HiveParser.DOT) {
        return firstDot(firstChild);
      }
      return dot;
    }

  }

  /*
   * When transforming a Not In SubQuery we need to check for nulls in the 
   * Joining expressions of the SubQuery. If there are nulls then the SubQuery always
   * return false. For more details see 
   * https://issues.apache.org/jira/secure/attachment/12614003/SubQuerySpec.pdf
   * 
   * Basically, SQL semantics say that:
   * - R1.A not in (null, 1, 2, ...)
   *   is always false. 
   *   A 'not in' operator is equivalent to a '<> all'. Since a not equal check with null 
   *   returns false, a not in predicate against aset with a 'null' value always returns false.
   *   
   * So for not in SubQuery predicates:
   * - we join in a null count predicate.
   * - And the joining condition is that the 'Null Count' query has a count of 0.
   *   
   */
  class NotInCheck implements ISubQueryJoinInfo {
    
    private static final String CNT_ALIAS = "c1";
    
    /*
     * expressions in SubQ that are joined to the Outer Query.
     */
    List<ASTNode> subQryCorrExprs;
    
    /*
     * row resolver of the SubQuery.
     * Set by the SemanticAnalyzer after the Plan for the SubQuery is genned.
     * This is neede in case the SubQuery select list contains a TOK_ALLCOLREF
     */
    RowResolver sqRR;
    
    NotInCheck() {
      subQryCorrExprs = new ArrayList<ASTNode>();
    }
    
    void addCorrExpr(ASTNode corrExpr) {
      subQryCorrExprs.add(corrExpr);
    }
    
    public ASTNode getSubQueryAST() {
      ASTNode ast = SubQueryUtils.buildNotInNullCheckQuery(
          QBSubQuery.this.getSubQueryAST(), 
          QBSubQuery.this.getAlias(), 
          CNT_ALIAS, 
          subQryCorrExprs,
          sqRR);
      SubQueryUtils.setOriginDeep(ast, QBSubQuery.this.originalSQASTOrigin);
      return ast;
    }
    
    public String getAlias() {
      return QBSubQuery.this.getAlias() + "_notin_nullcheck";
    }
    
    public JoinType getJoinType() {
      return JoinType.LEFTSEMI;
    }
    
    public ASTNode getJoinConditionAST() {
      ASTNode ast = 
          SubQueryUtils.buildNotInNullJoinCond(getAlias(), CNT_ALIAS);
      SubQueryUtils.setOriginDeep(ast, QBSubQuery.this.originalSQASTOrigin);
      return ast;
    }
    
    public QBSubQuery getSubQuery() {
      return QBSubQuery.this;
    }
    
    public String getOuterQueryId() {
      return QBSubQuery.this.getOuterQueryId();
    }
    
    void setSQRR(RowResolver sqRR) {
      this.sqRR = sqRR;
    }
        
  }
  
  private final String outerQueryId;
  private final int sqIdx;
  private final String alias;
  private final ASTNode subQueryAST;
  private final ASTNode parentQueryExpression;
  private final SubQueryTypeDef operator;
  private boolean containsAggregationExprs;
  private boolean hasCorrelation;
  private ASTNode joinConditionAST;
  private JoinType joinType;
  private ASTNode postJoinConditionAST;
  private int numCorrExprsinSQ;
  private List<ASTNode> subQueryJoinAliasExprs;
  private transient final ASTNodeOrigin originalSQASTOrigin;

  /*
   * tracks number of exprs from correlated predicates added to SQ select list.
   */
  private int numOfCorrelationExprsAddedToSQSelect;

  private boolean groupbyAddedToSQ;
  
  private int numOuterCorrExprsForHaving;
  
  private NotInCheck notInCheck;

  public QBSubQuery(String outerQueryId,
      int sqIdx,
      ASTNode subQueryAST,
      ASTNode parentQueryExpression,
      SubQueryTypeDef operator,
      ASTNode originalSQAST,
      Context ctx) {
    super();
    this.subQueryAST = subQueryAST;
    this.parentQueryExpression = parentQueryExpression;
    this.operator = operator;
    this.outerQueryId = outerQueryId;
    this.sqIdx = sqIdx;
    this.alias = "sq_" + this.sqIdx;
    this.numCorrExprsinSQ = 0;
    this.numOuterCorrExprsForHaving = 0;
    String s = ctx.getTokenRewriteStream().toString(
        originalSQAST.getTokenStartIndex(), originalSQAST.getTokenStopIndex());
    originalSQASTOrigin = new ASTNodeOrigin("SubQuery", alias, s, alias, originalSQAST);
    numOfCorrelationExprsAddedToSQSelect = 0;
    groupbyAddedToSQ = false;
    
    if ( operator.getType() == SubQueryType.NOT_IN ) {
      notInCheck = new NotInCheck();
    }
  }

  public ASTNode getSubQueryAST() {
    return subQueryAST;
  }
  public ASTNode getOuterQueryExpression() {
    return parentQueryExpression;
  }
  public SubQueryTypeDef getOperator() {
    return operator;
  }

  void validateAndRewriteAST(RowResolver outerQueryRR,
		  boolean forHavingClause,
		  String outerQueryAlias,
		  Set<String> outerQryAliases) throws SemanticException {

    ASTNode selectClause = (ASTNode) subQueryAST.getChild(1).getChild(1);

    int selectExprStart = 0;
    if ( selectClause.getChild(0).getType() == HiveParser.TOK_HINTLIST ) {
      selectExprStart = 1;
    }
    
    /*
     * Restriction.16.s :: Correlated Expression in Outer Query must not contain
     * unqualified column references.
     */
    if ( parentQueryExpression != null && !forHavingClause ) { 
        ASTNode u = SubQueryUtils.hasUnQualifiedColumnReferences(parentQueryExpression);
        if ( u != null ) {
          subQueryAST.setOrigin(originalSQASTOrigin);
          throw new SemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
              u, "Correlating expression cannot contain unqualified column references."));
        }
    }
    
    /*
     * Restriction 17.s :: SubQuery cannot use the same table alias as one used in
     * the Outer Query.
     */
    List<String> sqAliases = SubQueryUtils.getTableAliasesInSubQuery(this);
    String sharedAlias = null;
    for(String s : sqAliases ) {
      if ( outerQryAliases.contains(s) ) {
        sharedAlias = s;
      }
    }
    if ( sharedAlias != null) {
      ASTNode whereClause = SubQueryUtils.subQueryWhere(subQueryAST);
      
      if ( whereClause != null ) {
        ASTNode u = SubQueryUtils.hasUnQualifiedColumnReferences(whereClause);
        if ( u != null ) {
          subQueryAST.setOrigin(originalSQASTOrigin);
          throw new SemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
              u, "SubQuery cannot use the table alias: " + sharedAlias + "; " +
              		"this is also an alias in the Outer Query and SubQuery contains a unqualified column reference"));
        }
      }
    }

    /*
     * Check.5.h :: For In and Not In the SubQuery must implicitly or
     * explicitly only contain one select item.
     */
    if ( operator.getType() != SubQueryType.EXISTS &&
        operator.getType() != SubQueryType.NOT_EXISTS &&
        selectClause.getChildCount() - selectExprStart > 1 ) {
      subQueryAST.setOrigin(originalSQASTOrigin);
      throw new SemanticException(ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
          subQueryAST, "SubQuery can contain only 1 item in Select List."));
    }

    containsAggregationExprs = false;
    boolean containsWindowing = false;
    for(int i= selectExprStart; i < selectClause.getChildCount(); i++ ) {

      ASTNode selectItem = (ASTNode) selectClause.getChild(i);
      int r = SubQueryUtils.checkAggOrWindowing(selectItem);

      containsWindowing = containsWindowing | ( r == 2);
      containsAggregationExprs = containsAggregationExprs | ( r == 1 );
    }

    rewrite(outerQueryRR, forHavingClause, outerQueryAlias);

    SubQueryUtils.setOriginDeep(subQueryAST, originalSQASTOrigin);

    /*
     * Restriction.13.m :: In the case of an implied Group By on a
     * correlated SubQuery, the SubQuery always returns 1 row.
     * An exists on a SubQuery with an implied GBy will always return true.
     * Whereas Algebraically transforming to a Join may not return true. See
     * Specification doc for details.
     * Similarly a not exists on a SubQuery with a implied GBY will always return false.
     */
    if ( operator.getType() == SubQueryType.EXISTS  &&
        containsAggregationExprs &&
        groupbyAddedToSQ ) {
      throw new SemanticException(ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
          subQueryAST,
          "An Exists predicate on SubQuery with implicit Aggregation(no Group By clause) " +
          "cannot be rewritten. (predicate will always return true)."));
    }
    if ( operator.getType() == SubQueryType.NOT_EXISTS  &&
        containsAggregationExprs &&
        groupbyAddedToSQ ) {
      throw new SemanticException(ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
          subQueryAST,
          "A Not Exists predicate on SubQuery with implicit Aggregation(no Group By clause) " +
          "cannot be rewritten. (predicate will always return false)."));
    }

    /*
     * Restriction.14.h :: Correlated Sub Queries cannot contain Windowing clauses.
     */
    if ( containsWindowing && hasCorrelation ) {
      throw new SemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
          subQueryAST, "Correlated Sub Queries cannot contain Windowing clauses."));
    }

    /*
     * Check.4.h :: For Exists and Not Exists, the Sub Query must
     * have 1 or more correlated predicates.
     */
    if ( ( operator.getType() == SubQueryType.EXISTS ||
        operator.getType() == SubQueryType.NOT_EXISTS ) &&
        !hasCorrelation ) {
      throw new SemanticException(ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
          subQueryAST, "For Exists/Not Exists operator SubQuery must be Correlated."));
    }

  }

  private void setJoinType() {
    if ( operator.getType() == SubQueryType.NOT_IN ||
        operator.getType() == SubQueryType.NOT_EXISTS ) {
      joinType = JoinType.LEFTOUTER;
    } else {
      joinType = JoinType.LEFTSEMI;
    }
  }

  void buildJoinCondition(RowResolver outerQueryRR, RowResolver sqRR,
		  boolean forHavingClause,
		  String outerQueryAlias) throws SemanticException {
    ASTNode parentQueryJoinCond = null;

    if ( parentQueryExpression != null ) {
      
      ColumnInfo outerQueryCol = null;
      try {
        outerQueryCol = outerQueryRR.getExpression(parentQueryExpression);
      } catch(SemanticException se) {
      }
      
      parentQueryJoinCond = SubQueryUtils.buildOuterQryToSQJoinCond(
        getOuterQueryExpression(),
        alias,
        sqRR);
      
      if ( outerQueryCol != null ) {
        rewriteCorrConjunctForHaving(parentQueryJoinCond, true, 
            outerQueryAlias, outerQueryRR, outerQueryCol);
      }
    }
    joinConditionAST = SubQueryUtils.andAST(parentQueryJoinCond, joinConditionAST);
    setJoinType();

    if ( joinType == JoinType.LEFTOUTER ) {
      if ( operator.getType() == SubQueryType.NOT_EXISTS && hasCorrelation ) {
        postJoinConditionAST = SubQueryUtils.buildPostJoinNullCheck(subQueryJoinAliasExprs);
      } else if ( operator.getType() == SubQueryType.NOT_IN ) {
        postJoinConditionAST = SubQueryUtils.buildOuterJoinPostCond(alias, sqRR);
      }
    }

    SubQueryUtils.setOriginDeep(joinConditionAST, originalSQASTOrigin);
    SubQueryUtils.setOriginDeep(postJoinConditionAST, originalSQASTOrigin);
  }

  ASTNode updateOuterQueryFilter(ASTNode outerQryFilter) {
    if (postJoinConditionAST == null ) {
      return outerQryFilter;
    } else if ( outerQryFilter == null ) {
      return postJoinConditionAST;
    }
    ASTNode node = SubQueryUtils.andAST(outerQryFilter, postJoinConditionAST);
    node.setOrigin(originalSQASTOrigin);
    return node;
  }

  String getNextCorrExprAlias() {
    return "sq_corr_" + numCorrExprsinSQ++;
  }

  /*
   * - If the SubQuery has no where clause, there is nothing to rewrite.
   * - Decompose SubQuery where clause into list of Top level conjuncts.
   * - For each conjunct
   *   - Break down the conjunct into (LeftExpr, LeftExprType, RightExpr,
   *     RightExprType)
   *   - If the top level operator is an Equality Operator we will break
   *     it down into left and right; in all other case there is only a
   *     lhs.
   *   - The ExprType is based on whether the Expr. refers to the Parent
   *     Query table sources, refers to the SubQuery sources or both.
   *   - We assume an unqualified Column refers to a SubQuery table source.
   *     This is because we require Parent Column references to be qualified
   *     within the SubQuery.
   *   - If the lhs or rhs expr refers to both Parent and SubQuery sources,
   *     we flag this as Unsupported.
   *   - If the conjunct as a whole, only refers to the Parent Query sources,
   *     we flag this as an Error.
   *   - A conjunct is Correlated if the lhs refers to SubQuery sources and rhs
   *     refers to Parent Query sources or the reverse.
   *   - Say the lhs refers to SubQuery and rhs refers to Parent Query sources; the
   *     other case is handled analogously.
   *     - remove this conjunct from the SubQuery where clause.
   *     - for the SubQuery expression(lhs) construct a new alias
   *     - in the correlated predicate, replace the SubQuery
   *       expression(lhs) with the alias AST.
   *     - add this altered predicate to the Join predicate tracked by the
   *       QBSubQuery object.
   *     - add the alias AST to a list of subQueryJoinAliasExprs. This
   *       list is used in the case of Outer Joins to add null check
   *       predicates to the Outer Query's where clause.
   *     - Add the SubQuery expression with the alias as a SelectItem to
   *       the SubQuery's SelectList.
   *     - In case this SubQuery contains aggregation expressions add this SubQuery
   *       expression to its GroupBy; add it to the front of the GroupBy.
   *   - If predicate is not correlated, let it remain in the SubQuery
   *     where clause.
   * Additional things for Having clause:
   * - A correlation predicate may refer to an aggregation expression.
   * - This introduces 2 twists to the rewrite:
   *   a. When analyzing equality predicates we need to analyze each side 
   *      to see if it is an aggregation expression from the Outer Query.
   *      So for e.g. this is a valid correlation predicate:
   *         R2.x = min(R1.y)
   *      Where R1 is an outer table reference, and R2 is a SubQuery table reference.
   *   b. When hoisting the correlation predicate to a join predicate, we need to
   *      rewrite it to be in the form the Join code allows: so the predicte needs
   *      to contain a qualified column references.
   *      We handle this by generating a new name for the aggregation expression,
   *      like R1._gby_sq_col_1 and adding this mapping to the Outer Query's
   *      Row Resolver. Then we construct a joining predicate using this new 
   *      name; so in our e.g. the condition would be: R2.x = R1._gby_sq_col_1
   */
  private void rewrite(RowResolver parentQueryRR,
		  boolean forHavingClause,
		  String outerQueryAlias) throws SemanticException {
    ASTNode selectClause = (ASTNode) subQueryAST.getChild(1).getChild(1);
    ASTNode whereClause = SubQueryUtils.subQueryWhere(subQueryAST);

    if ( whereClause == null ) {
      return;
    }

    ASTNode searchCond = (ASTNode) whereClause.getChild(0);
    List<ASTNode> conjuncts = new ArrayList<ASTNode>();
    SubQueryUtils.extractConjuncts(searchCond, conjuncts);

    ConjunctAnalyzer conjunctAnalyzer = new ConjunctAnalyzer(parentQueryRR,
    		forHavingClause, outerQueryAlias);
    ASTNode sqNewSearchCond = null;

    for(ASTNode conjunctAST : conjuncts) {
      Conjunct conjunct = conjunctAnalyzer.analyzeConjunct(conjunctAST);

      /*
       *  Restriction.11.m :: A SubQuery predicate that refers to an Outer
       *  Query column must be a valid Join predicate.
       */
      if ( conjunct.eitherSideRefersBoth() ) {
        throw new SemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
            conjunctAST,
            "SubQuery expression refers to both Parent and SubQuery expressions and " +
            "is not a valid join condition."));
      }

      /*
       * Check.12.h :: SubQuery predicates cannot only refer to Outer Query columns.
       */
      if ( conjunct.refersOuterOnly() ) {
        throw new SemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
            conjunctAST,
            "SubQuery expression refers to Outer query expressions only."));
      }

      if ( conjunct.isCorrelated() ) {
        hasCorrelation = true;
        subQueryJoinAliasExprs = new ArrayList<ASTNode>();
        String exprAlias = getNextCorrExprAlias();
        ASTNode sqExprAlias = SubQueryUtils.createAliasAST(exprAlias);
        ASTNode sqExprForCorr = SubQueryUtils.createColRefAST(alias, exprAlias);

        if ( conjunct.getLeftExprType().refersSubQuery() ) {
          if ( forHavingClause && conjunct.getRightOuterColInfo() != null ) {
            rewriteCorrConjunctForHaving(conjunctAST, false, outerQueryAlias, 
                parentQueryRR, conjunct.getRightOuterColInfo());
          }
          ASTNode joinPredciate = SubQueryUtils.alterCorrelatedPredicate(
              conjunctAST, sqExprForCorr, true);
          joinConditionAST = SubQueryUtils.andAST(joinConditionAST, joinPredciate);
          subQueryJoinAliasExprs.add(sqExprForCorr);
          ASTNode selExpr = SubQueryUtils.createSelectItem(conjunct.getLeftExpr(), sqExprAlias);
          selectClause.addChild(selExpr);
          numOfCorrelationExprsAddedToSQSelect++;
          if ( containsAggregationExprs ) {
            ASTNode gBy = getSubQueryGroupByAST();
            SubQueryUtils.addGroupExpressionToFront(gBy, conjunct.getLeftExpr());
          }
          if ( notInCheck != null ) {
            notInCheck.addCorrExpr((ASTNode)conjunctAST.getChild(0));
          }
        } else {
          if ( forHavingClause && conjunct.getLeftOuterColInfo() != null ) {
            rewriteCorrConjunctForHaving(conjunctAST, true, outerQueryAlias, 
                parentQueryRR, conjunct.getLeftOuterColInfo());
          }
          ASTNode joinPredciate = SubQueryUtils.alterCorrelatedPredicate(
              conjunctAST, sqExprForCorr, false);
          joinConditionAST = SubQueryUtils.andAST(joinConditionAST, joinPredciate);
          subQueryJoinAliasExprs.add(sqExprForCorr);
          ASTNode selExpr = SubQueryUtils.createSelectItem(conjunct.getRightExpr(), sqExprAlias);
          selectClause.addChild(selExpr);
          numOfCorrelationExprsAddedToSQSelect++;
          if ( containsAggregationExprs ) {
            ASTNode gBy = getSubQueryGroupByAST();
            SubQueryUtils.addGroupExpressionToFront(gBy, conjunct.getRightExpr());
          }
          if ( notInCheck != null ) {
            notInCheck.addCorrExpr((ASTNode)conjunctAST.getChild(1));
          }
        }
      } else {
        sqNewSearchCond = SubQueryUtils.andAST(sqNewSearchCond, conjunctAST);
      }
    }

    if ( sqNewSearchCond != searchCond ) {
      if ( sqNewSearchCond == null ) {
        /*
         * for now just adding a true condition(1=1) to where clause.
         * Can remove the where clause from the AST; requires moving all subsequent children
         * left.
         */
        sqNewSearchCond = SubQueryUtils.constructTrueCond();
      }
      whereClause.setChild(0, sqNewSearchCond);
    }

  }

  /*
   * called if the SubQuery is Agg and Correlated.
   * if SQ doesn't have a GroupBy, it is added to the SQ AST.
   */
  private ASTNode getSubQueryGroupByAST() {
    ASTNode groupBy = null;
    if ( subQueryAST.getChild(1).getChildCount() > 3 &&
        subQueryAST.getChild(1).getChild(3).getType() == HiveParser.TOK_GROUPBY ) {
      groupBy = (ASTNode) subQueryAST.getChild(1).getChild(3);
    }

    if ( groupBy != null ) {
      return groupBy;
    }

    groupBy = SubQueryUtils.buildGroupBy();
    groupbyAddedToSQ = true;

    List<ASTNode> newChildren = new ArrayList<ASTNode>();
    newChildren.add(groupBy);
    if ( subQueryAST.getChildCount() > 3) {
      for( int i = subQueryAST.getChildCount() - 1; i >= 3; i-- ) {
        ASTNode child = (ASTNode) subQueryAST.getChild(i);
        newChildren.add(child);
      }
    }

    for(ASTNode child : newChildren ) {
      subQueryAST.addChild(child);
    }

    return groupBy;
  }


  public String getOuterQueryId() {
    return outerQueryId;
  }

  public JoinType getJoinType() {
    return joinType;
  }

  public String getAlias() {
    return alias;
  }

  public ASTNode getJoinConditionAST() {
    return joinConditionAST;
  }

  public int getNumOfCorrelationExprsAddedToSQSelect() {
    return numOfCorrelationExprsAddedToSQSelect;
  }
  
  public QBSubQuery getSubQuery() {
    return this;
  }
  
  NotInCheck getNotInCheck() {
    return notInCheck;
  }
  
  private void rewriteCorrConjunctForHaving(ASTNode conjunctASTNode,
      boolean refersLeft,
      String outerQueryAlias,
      RowResolver outerQueryRR,
      ColumnInfo outerQueryCol) {
    
    String newColAlias = "_gby_sq_col_" + numOuterCorrExprsForHaving++;
    ASTNode outerExprForCorr = SubQueryUtils.createColRefAST(outerQueryAlias, newColAlias);
    if ( refersLeft ) {
      conjunctASTNode.setChild(0, outerExprForCorr);
    } else {
      conjunctASTNode.setChild(1, outerExprForCorr);
    }
    outerQueryRR.put(outerQueryAlias, newColAlias, outerQueryCol);
  }
      
}
