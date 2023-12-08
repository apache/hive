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
package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveGroupingID;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortExchange;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveValues;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.JdbcHiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter.HiveToken;
import org.apache.hadoop.hive.ql.optimizer.signature.RelTreeSignature;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.apache.hadoop.hive.ql.util.DirectionUtils;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

import static org.apache.calcite.rel.core.Values.isEmpty;

public class ASTConverter {
  private static final Logger LOG = LoggerFactory.getLogger(ASTConverter.class);
  public static final String NON_FK_FILTERED = "NON_FK_FILTERED";
  public static final String NON_FK_NOT_FILTERED = "NON_FK_NOT_FILTERED";

  private final RelNode          root;
  private final HiveAST          hiveAST;
  private RelNode          from;
  private Filter           where;
  private Aggregate        groupBy;
  private Filter           having;
  private RelNode          select;
  private RelNode          orderLimit;

  private Schema           schema;

  private long             derivedTableCount;

  private PlanMapper planMapper;

  ASTConverter(RelNode root, long dtCounterInitVal, PlanMapper planMapper) {
    this.root = root;
    hiveAST = new HiveAST();
    this.derivedTableCount = dtCounterInitVal;
    this.planMapper = planMapper;
  }

  public static ASTNode convert(final RelNode relNode, List<FieldSchema> resultSchema, boolean alignColumns, PlanMapper planMapper)
      throws CalciteSemanticException {
    RelNode root = PlanModifierForASTConv.convertOpTree(relNode, resultSchema, alignColumns);
    ASTConverter c = new ASTConverter(root, 0, planMapper);
    return c.convert();
  }

  /**
   * This method generates the abstract syntax tree of a query does not return any rows.
   * All projected columns are null and the data types are come from the passed {@link RelDataType}.
   * <pre>
   * SELECT NULL alias0 ... NULL aliasn LIMIT 0;
   * </pre>
   * Due to a subsequent optimization when converting the plan to TEZ tasks
   * adding a limit 0 enables Hive not submitting the query to TEZ application manager
   * but returns empty result set immediately.
   * <pre>
   * TOK_QUERY
   *   TOK_INSERT
   *      TOK_DESTINATION
   *         TOK_DIR
   *            TOK_TMP_FILE
   *      TOK_SELECT
   *         TOK_SELEXPR
   *            TOK_FUNCTION
   *               TOK_&lt;type&gt;
   *               TOK_NULL
   *            alias0
   *         ...
   *         TOK_SELEXPR
   *            TOK_FUNCTION
   *               TOK_&lt;type&gt;
   *               TOK_NULL
   *            aliasn
   *      TOK_LIMIT
   *         0
   *         0
   * </pre>
   * @param dataType - Schema
   * @return Root {@link ASTNode} of the result plan.
   *
   * @see QueryProperties#getOuterQueryLimit()
   * @see org.apache.hadoop.hive.ql.parse.TaskCompiler#compile(ParseContext, List, Set, Set)
   */
  public static ASTNode emptyPlan(RelDataType dataType) {
    if (dataType.getFieldCount() == 0) {
      throw new IllegalArgumentException("Schema is empty.");
    }

    ASTBuilder select = ASTBuilder.construct(HiveParser.TOK_SELECT, "TOK_SELECT");
    for (int i = 0; i < dataType.getFieldCount(); ++i) {
      RelDataTypeField fieldType = dataType.getFieldList().get(i);
      select.add(ASTBuilder.selectExpr(createNullField(fieldType.getType()), fieldType.getName()));
    }

    ASTNode insert = ASTBuilder.
            construct(HiveParser.TOK_INSERT, "TOK_INSERT").
            add(ASTBuilder.destNode()).
            add(select).
            add(ASTBuilder.limit(0, 0)).
            node();

    return ASTBuilder.
            construct(HiveParser.TOK_QUERY, "TOK_QUERY").
            add(insert).
            node();
  }

  private static ASTNode createNullField(RelDataType fieldType) {
    if (fieldType.getSqlTypeName() == SqlTypeName.NULL) {
      return ASTBuilder.construct(HiveParser.TOK_NULL, "TOK_NULL").node();
    }

    ASTNode astNode = convertType(fieldType);
    return ASTBuilder.construct(HiveParser.TOK_FUNCTION, "TOK_FUNCTION")
        .add(astNode)
        .add(HiveParser.TOK_NULL, "TOK_NULL")
        .node();
  }

  static ASTNode convertType(RelDataType fieldType) {
    if (fieldType.getSqlTypeName() == SqlTypeName.NULL) {
      return ASTBuilder.construct(HiveParser.TOK_NULL, "TOK_NULL").node();
    }

    if (fieldType.getSqlTypeName() == SqlTypeName.ROW) {
      ASTBuilder columnListNode = ASTBuilder.construct(HiveParser.TOK_TABCOLLIST, "TOK_TABCOLLIST");
      for (RelDataTypeField structFieldType : fieldType.getFieldList()) {
        ASTNode colNode = ASTBuilder.construct(HiveParser.TOK_TABCOL, "TOK_TABCOL")
            .add(HiveParser.Identifier, structFieldType.getName())
            .add(convertType(structFieldType.getType()))
            .node();
        columnListNode.add(colNode);
      }
      return ASTBuilder.construct(HiveParser.TOK_STRUCT, "TOK_STRUCT").add(columnListNode).node();
    }

    if (fieldType.getSqlTypeName() == SqlTypeName.MAP) {
      ASTBuilder mapCallNode = ASTBuilder.construct(HiveParser.TOK_MAP, "TOK_MAP");
      mapCallNode.add(convertType(fieldType.getKeyType()));
      mapCallNode.add(convertType(fieldType.getValueType()));
      return mapCallNode.node();
    }

    if (fieldType.getSqlTypeName() == SqlTypeName.ARRAY) {
      ASTBuilder arrayCallNode = ASTBuilder.construct(HiveParser.TOK_LIST, "TOK_LIST");
      arrayCallNode.add(convertType(fieldType.getComponentType()));
      return arrayCallNode.node();
    }

    HiveToken ht = TypeConverter.hiveToken(fieldType);
    ASTBuilder astBldr = ASTBuilder.construct(ht.type, ht.text);
    if (ht.args != null) {
      for (String castArg : ht.args) {
        astBldr.add(HiveParser.Identifier, castArg);
      }
    }

    return astBldr.node();
  }

  private ASTNode convert() throws CalciteSemanticException {
    if (root instanceof HiveValues) {
      HiveValues values = (HiveValues) root;
      if (isEmpty(values)) {
        select = values;
        return emptyPlan(values.getRowType());
      } else {
        throw new UnsupportedOperationException("Values with non-empty tuples are not supported.");
      }
    }
    /*
     * 1. Walk RelNode Graph; note from, where, gBy.. nodes.
     */
    new QBVisitor().go(root);

    /*
     * 2. convert from node.
     */
    QueryBlockInfo qb = convertSource(from);
    schema = qb.schema;
    hiveAST.from = ASTBuilder.construct(HiveParser.TOK_FROM, "TOK_FROM").add(qb.ast).node();

    /*
     * 3. convert filterNode
     */
    if (where != null) {
      ASTNode cond = where.getCondition().accept(new RexVisitor(schema, false, root.getCluster().getRexBuilder()));
      hiveAST.where = ASTBuilder.where(cond);
      planMapper.link(cond, where);
      planMapper.link(cond, RelTreeSignature.of(where));
    }

    /*
     * 4. GBy
     */
    if (groupBy != null) {
      ASTBuilder b;
      boolean groupingSetsExpression = false;
      Group aggregateType = groupBy.getGroupType();
      switch (aggregateType) {
        case SIMPLE:
          b = ASTBuilder.construct(HiveParser.TOK_GROUPBY, "TOK_GROUPBY");
          break;
        case ROLLUP:
        case CUBE:
        case OTHER:
          b = ASTBuilder.construct(HiveParser.TOK_GROUPING_SETS, "TOK_GROUPING_SETS");
          groupingSetsExpression = true;
          break;
        default:
          throw new CalciteSemanticException("Group type not recognized");
      }

      HiveAggregate hiveAgg = (HiveAggregate) groupBy;
      if (hiveAgg.getAggregateColumnsOrder() != null) {
        // Aggregation columns may have been sorted in specific order
        for (int pos : hiveAgg.getAggregateColumnsOrder()) {
          addRefToBuilder(b, groupBy.getGroupSet().nth(pos));
        }
        for (int pos = 0; pos < groupBy.getGroupCount(); pos++) {
          if (!hiveAgg.getAggregateColumnsOrder().contains(pos)) {
            addRefToBuilder(b, groupBy.getGroupSet().nth(pos));
          }
        }
      } else {
        // Aggregation columns have not been reordered
        for (int i : groupBy.getGroupSet()) {
          addRefToBuilder(b, i);
        }
      }

      //Grouping sets expressions
      if(groupingSetsExpression) {
        for(ImmutableBitSet groupSet: groupBy.getGroupSets()) {
          ASTBuilder expression = ASTBuilder.construct(
                  HiveParser.TOK_GROUPING_SETS_EXPRESSION, "TOK_GROUPING_SETS_EXPRESSION");
          for (int i : groupSet) {
            addRefToBuilder(expression, i);
          }
          b.add(expression);
        }
      }

      if (!groupBy.getGroupSet().isEmpty()) {
        hiveAST.groupBy = b.node();
      }

      schema = new Schema(schema, groupBy);
    }

    /*
     * 5. Having
     */
    if (having != null) {
      ASTNode cond = having.getCondition().accept(new RexVisitor(schema, false, root.getCluster().getRexBuilder()));
      hiveAST.having = ASTBuilder.having(cond);
    }

    /*
     * 6. Project
     */
    ASTBuilder b = ASTBuilder.construct(HiveParser.TOK_SELECT, "TOK_SELECT");

    if (select instanceof Project) {
      List<RexNode> childExps = ((Project) select).getProjects();
      if (childExps.isEmpty()) {
        RexLiteral r = select.getCluster().getRexBuilder().makeExactLiteral(new BigDecimal(1));
        ASTNode selectExpr = ASTBuilder.selectExpr(ASTBuilder.literal(r), "1");
        b.add(selectExpr);
      } else {
        int i = 0;

        for (RexNode r : childExps) {
          ASTNode expr = r.accept(new RexVisitor(schema, r instanceof RexLiteral,
              select.getCluster().getRexBuilder()));
          String alias = select.getRowType().getFieldNames().get(i++);
          ASTNode selectExpr = ASTBuilder.selectExpr(expr, alias);
          b.add(selectExpr);
        }
      }
      hiveAST.select = b.node();
    } else {
      // select is UDTF
      HiveTableFunctionScan udtf = (HiveTableFunctionScan) select;
      List<ASTNode> children = new ArrayList<>();
      RexCall call = (RexCall) udtf.getCall();
      for (RexNode r : call.getOperands()) {
        ASTNode expr = r.accept(new RexVisitor(schema, r instanceof RexLiteral,
            select.getCluster().getRexBuilder()));
        children.add(expr);
      }
      ASTBuilder sel = ASTBuilder.construct(HiveParser.TOK_SELEXPR, "TOK_SELEXPR");
      ASTNode function = buildUDTFAST(call.getOperator().getName(), children);
      sel.add(function);

      List<String> fields = udtf.getRowType().getFieldNames();
      for (int i = 0; i < udtf.getRowType().getFieldCount(); ++i) {
        sel.add(HiveParser.Identifier, fields.get(i));
      }
      b.add(sel);
      hiveAST.select = b.node();
    }

    /*
     * 7. Order Use in Order By from the block above. RelNode has no pointer to
     * parent hence we need to go top down; but OB at each block really belong
     * to its src/from. Hence the need to pass in sort for each block from
     * its parent.
     * 8. Limit
     */
    convertOrderToASTNode(orderLimit);

    return hiveAST.getAST();
  }

  private void addRefToBuilder(ASTBuilder b, int i) {
    RexInputRef iRef = new RexInputRef(i,
        root.getCluster().getTypeFactory().createSqlType(SqlTypeName.ANY));
    b.add(iRef.accept(new RexVisitor(schema, false, root.getCluster().getRexBuilder())));
  }

  private static ASTNode buildUDTFAST(String functionName, List<ASTNode> children) {
    ASTNode node = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
    node.addChild((ASTNode) ParseDriver.adaptor.create(HiveParser.Identifier, functionName));
    for (ASTNode c : children) {
      ParseDriver.adaptor.addChild(node, c);
    }
    return node;
  }

  private void convertOrderToASTNode(RelNode node) {
    if (node == null) {
      return;
    }

    if (node instanceof HiveSortLimit) {
      convertOrderLimitToASTNode((HiveSortLimit) node);
    } else if (node instanceof HiveSortExchange) {
      convertSortToASTNode((HiveSortExchange) node);
    }
  }

  private void convertOrderLimitToASTNode(HiveSortLimit hiveSortLimit) {
    List<RelFieldCollation> fieldCollations = hiveSortLimit.getCollation().getFieldCollations();
    convertFieldCollationsToASTNode(hiveSortLimit, new Schema(hiveSortLimit), fieldCollations,
            hiveSortLimit.getInputRefToCallMap(), HiveParser.TOK_ORDERBY, "TOK_ORDERBY");

    RexNode offsetExpr = hiveSortLimit.getOffsetExpr();
    RexNode fetchExpr = hiveSortLimit.getFetchExpr();
    if (fetchExpr != null) {
      Object offset = (offsetExpr == null) ? Integer.valueOf(0) : ((RexLiteral) offsetExpr).getValue2();
      Object fetch = ((RexLiteral) fetchExpr).getValue2();
      hiveAST.limit = ASTBuilder.limit(offset, fetch);
    }
  }

  private void convertSortToASTNode(HiveSortExchange hiveSortExchange) {
    List<RelFieldCollation> fieldCollations = hiveSortExchange.getCollation().getFieldCollations();
    convertFieldCollationsToASTNode(hiveSortExchange, new Schema(hiveSortExchange), fieldCollations,
            null, HiveParser.TOK_SORTBY, "TOK_SORTBY");
  }

  private void convertFieldCollationsToASTNode(
          RelNode node, Schema schema, List<RelFieldCollation> fieldCollations, Map<Integer, RexNode> obRefToCallMap,
          int astToken, String astText) {
    if (fieldCollations.isEmpty()) {
      return;
    }

    // 1 Add order/sort by token
    ASTNode orderAst = ASTBuilder.createAST(astToken, astText);

    RexNode obExpr;
    ASTNode astCol;
    for (RelFieldCollation c : fieldCollations) {

      // 2 Add Direction token
      ASTNode directionAST = c.getDirection() == RelFieldCollation.Direction.ASCENDING ? ASTBuilder
              .createAST(HiveParser.TOK_TABSORTCOLNAMEASC, "TOK_TABSORTCOLNAMEASC") : ASTBuilder
              .createAST(HiveParser.TOK_TABSORTCOLNAMEDESC, "TOK_TABSORTCOLNAMEDESC");
      ASTNode nullDirectionAST;
      // Null direction
      if (c.nullDirection == RelFieldCollation.NullDirection.FIRST) {
        nullDirectionAST = ASTBuilder.createAST(HiveParser.TOK_NULLS_FIRST, "TOK_NULLS_FIRST");
        directionAST.addChild(nullDirectionAST);
      } else if (c.nullDirection == RelFieldCollation.NullDirection.LAST) {
        nullDirectionAST = ASTBuilder.createAST(HiveParser.TOK_NULLS_LAST, "TOK_NULLS_LAST");
        directionAST.addChild(nullDirectionAST);
      } else {
        // Default
        if (c.getDirection() == RelFieldCollation.Direction.ASCENDING) {
          nullDirectionAST = ASTBuilder.createAST(HiveParser.TOK_NULLS_FIRST, "TOK_NULLS_FIRST");
          directionAST.addChild(nullDirectionAST);
        } else {
          nullDirectionAST = ASTBuilder.createAST(HiveParser.TOK_NULLS_LAST, "TOK_NULLS_LAST");
          directionAST.addChild(nullDirectionAST);
        }
      }

      // 3 Convert OB expr (OB Expr is usually an input ref except for top
      // level OB; top level OB will have RexCall kept in a map.)
      obExpr = null;
      if (obRefToCallMap != null) {
        obExpr = obRefToCallMap.get(c.getFieldIndex());
      }

      if (obExpr != null) {
        astCol = obExpr.accept(new RexVisitor(schema, false, node.getCluster().getRexBuilder()));
      } else {
        ColumnInfo cI = schema.get(c.getFieldIndex());
        /*
         * The RowResolver setup for Select drops Table associations. So
         * setup ASTNode on unqualified name.
         */
        astCol = ASTBuilder.unqualifiedName(cI.column);
      }

      // 4 buildup the ob expr AST
      nullDirectionAST.addChild(astCol);
      orderAst.addChild(directionAST);
    }
    hiveAST.order = orderAst;
  }

  private Schema getRowSchema(String tblAlias) {
    if (select instanceof Project) {
      return new Schema((Project) select, tblAlias);
    } else if (select instanceof TableFunctionScan) {
      return new Schema((TableFunctionScan) select, tblAlias);
    } else {
      return new Schema(tblAlias, select.getRowType().getFieldList());
    }
  }

  private QueryBlockInfo convertSource(RelNode r) throws CalciteSemanticException {
    Schema s = null;
    ASTNode ast = null;

    if (r instanceof TableScan) {
      TableScan f = (TableScan) r;
      s = new Schema(f);
      ast = ASTBuilder.table(f);
      planMapper.link(ast, f);
    } else if (r instanceof HiveJdbcConverter) {
      HiveJdbcConverter f = (HiveJdbcConverter) r;
      s = new Schema(f);
      ast = ASTBuilder.table(f);
    } else if (r instanceof DruidQuery) {
      DruidQuery f = (DruidQuery) r;
      s = new Schema(f);
      ast = ASTBuilder.table(f);
    } else if (r instanceof Join) {
      Join join = (Join) r;
      QueryBlockInfo left = convertSource(join.getLeft());
      QueryBlockInfo right = convertSource(join.getRight());
      s = new Schema(left.schema, right.schema);
      ASTNode cond = join.getCondition().accept(new RexVisitor(s, false, r.getCluster().getRexBuilder()));
      boolean semiJoin = join.isSemiJoin() || join.getJoinType() == JoinRelType.ANTI;
      if (join.getRight() instanceof Join && !semiJoin) {
          // should not be done for semijoin since it will change the semantics
        // Invert join inputs; this is done because otherwise the SemanticAnalyzer
        // methods to merge joins will not kick in
        JoinRelType type;
        if (join.getJoinType() == JoinRelType.LEFT) {
          type = JoinRelType.RIGHT;
        } else if (join.getJoinType() == JoinRelType.RIGHT) {
          type = JoinRelType.LEFT;
        } else {
          type = join.getJoinType();
        }
        ast = ASTBuilder.join(right.ast, left.ast, type, cond);
        addPkFkInfoToAST(ast, join, true);
      } else {
        ast = ASTBuilder.join(left.ast, right.ast, join.getJoinType(), cond);
        addPkFkInfoToAST(ast, join, false);
      }

      if (semiJoin) {
        s = left.schema;
      }
    } else if (r instanceof Union) {
      Union u = ((Union) r);
      ASTNode left = new ASTConverter(((Union) r).getInput(0), this.derivedTableCount, planMapper).convert();
      for (int ind = 1; ind < u.getInputs().size(); ind++) {
        left = getUnionAllAST(left, new ASTConverter(((Union) r).getInput(ind),
            this.derivedTableCount, planMapper).convert());
        String sqAlias = nextAlias();
        ast = ASTBuilder.subQuery(left, sqAlias);
        s = new Schema((Union) r, sqAlias);
      }
    } else if (isLateralView(r)) {
      TableFunctionScan tfs = ((TableFunctionScan) r);

      // retrieve the base table source.
      QueryBlockInfo tableFunctionSource = convertSource(tfs.getInput(0));
      String sqAlias = tableFunctionSource.schema.get(0).table;
      // the schema will contain the base table source fields
      s = new Schema(tfs, sqAlias);

      ast = createASTLateralView(tfs, s, tableFunctionSource, sqAlias);

    } else {
      ASTConverter src = new ASTConverter(r, this.derivedTableCount, planMapper);
      ASTNode srcAST = src.convert();
      String sqAlias = nextAlias();
      s = src.getRowSchema(sqAlias);
      ast = ASTBuilder.subQuery(srcAST, sqAlias);
    }
    return new QueryBlockInfo(s, ast);
  }

  /**
   * Add PK-FK join information to the AST as a query hint
   * @param ast
   * @param join
   * @param swapSides whether the left and right input of the join is swapped
   */
  private void addPkFkInfoToAST(ASTNode ast, Join join, boolean swapSides) {
    List<RexNode> joinFilters = new ArrayList<>(RelOptUtil.conjunctions(join.getCondition()));
    RelMetadataQuery mq = join.getCluster().getMetadataQuery();
    HiveRelOptUtil.PKFKJoinInfo rightInputResult =
            HiveRelOptUtil.extractPKFKJoin(join, joinFilters, false, mq);
    HiveRelOptUtil.PKFKJoinInfo leftInputResult =
            HiveRelOptUtil.extractPKFKJoin(join, joinFilters, true, mq);
    // Add the fkJoinIndex (0=left, 1=right, if swapSides is false) to the AST
    // check if the nonFK side is filtered
    if (leftInputResult.isPkFkJoin && leftInputResult.additionalPredicates.isEmpty()) {
      RelNode nonFkInput = join.getRight();
      ast.addChild(pkFkHint(swapSides ? 1 : 0, HiveRelOptUtil.isRowFilteringPlan(mq, nonFkInput)));
    } else if (rightInputResult.isPkFkJoin && rightInputResult.additionalPredicates.isEmpty()) {
      RelNode nonFkInput = join.getLeft();
      ast.addChild(pkFkHint(swapSides ? 0 : 1, HiveRelOptUtil.isRowFilteringPlan(mq, nonFkInput)));
    }
  }

  private ASTNode pkFkHint(int fkTableIndex, boolean nonFkSideIsFiltered) {
    ParseDriver parseDriver = new ParseDriver();
    try {
      return parseDriver.parseHint(String.format("PKFK_JOIN(%d, %s)",
              fkTableIndex, nonFkSideIsFiltered ? NON_FK_FILTERED : NON_FK_NOT_FILTERED));
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  private static ASTNode createASTLateralView(TableFunctionScan tfs, Schema s,
      QueryBlockInfo tableFunctionSource, String sqAlias) {
    // The structure of the AST LATERAL VIEW will be:
    //
    //   TOK_LATERAL_VIEW
    //     TOK_SELECT
    //       TOK_SELEXPR
    //         TOK_FUNCTION
    //           <udtf func>
    //           ...
    //         <col alias for function>
    //         TOK_TABALIAS
    //           <table alias for lateral view>

    // set up the select for the parameters of the UDTF
    List<ASTNode> children = new ArrayList<>();
    // The UDTF function call within the table function scan will be of the form:
    // lateral(my_udtf_func(...), $0, $1, ...).  For recreating the AST, we need
    // the inner "my_udtf_func".
    RexCall lateralCall = (RexCall) tfs.getCall();
    RexCall call = (RexCall) lateralCall.getOperands().get(0);
    for (RexNode rn : call.getOperands()) {
      ASTNode expr = rn.accept(new RexVisitor(s, rn instanceof RexLiteral,
          tfs.getCluster().getRexBuilder()));
      children.add(expr);
    }
    ASTNode function = buildUDTFAST(call.getOperator().getName(), children);

    // Add the function to the SELEXPR
    ASTBuilder selexpr = ASTBuilder.construct(HiveParser.TOK_SELEXPR, "TOK_SELEXPR");
    selexpr.add(function);

    // Add only the table generated size columns to the select expr for the function,
    // skipping over the base table columns from the input side of the join.
    int i = 0;
    for (ColumnInfo c : s) {
      if (i++ < tableFunctionSource.schema.size()) {
        continue;
      }
      selexpr.add(HiveParser.Identifier, c.column);
    }
    // add the table alias for the lateral view.
    ASTBuilder tabAlias = ASTBuilder.construct(HiveParser.TOK_TABALIAS, "TOK_TABALIAS");
    tabAlias.add(HiveParser.Identifier, sqAlias);

    // add the table alias to the SEL_EXPR
    selexpr.add(tabAlias.node());

    // create the SELECT clause
    ASTBuilder sel = ASTBuilder.construct(HiveParser.TOK_SELEXPR, "TOK_SELECT");
    sel.add(selexpr.node());

    // place the SELECT clause under the LATERAL VIEW clause
    ASTBuilder lateralview = ASTBuilder.construct(HiveParser.TOK_LATERAL_VIEW, "TOK_LATERAL_VIEW");
    lateralview.add(sel.node());

    // finally, add the LATERAL VIEW clause under the left side source which is the base table.
    lateralview.add(tableFunctionSource.ast);

    return lateralview.node();
  }

  private boolean isLateralView(RelNode relNode) {
    if (!(relNode instanceof TableFunctionScan)) {
      return false;
    }
    TableFunctionScan htfs = (TableFunctionScan) relNode;
    RexCall call = (RexCall) htfs.getCall();
    return ((RexCall) htfs.getCall()).getOperator() == SqlStdOperatorTable.LATERAL;
  }

  class QBVisitor extends RelVisitor {

    public void handle(Filter filter) {
      RelNode child = filter.getInput();
      if (child instanceof Aggregate && !((Aggregate) child).getGroupSet().isEmpty()) {
        ASTConverter.this.having = filter;
      } else {
        ASTConverter.this.where = filter;
      }
    }

    public void handle(Project project) {
      if (ASTConverter.this.select == null) {
        ASTConverter.this.select = project;
      } else {
        ASTConverter.this.from = project;
      }
    }

    public void handle(TableFunctionScan tableFunctionScan) {
      if (ASTConverter.this.select == null) {
        ASTConverter.this.select = tableFunctionScan;
      } else {
        ASTConverter.this.from = tableFunctionScan;
      }
    }

    public void handle(Values values) {
      if (ASTConverter.this.select == null) {
        ASTConverter.this.select = values;
      } else {
        ASTConverter.this.from = values;
      }
    }

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {

      if (node instanceof TableScan ||
          node instanceof DruidQuery ||
          node instanceof HiveJdbcConverter) {
        ASTConverter.this.from = node;
      } else if (node instanceof Filter) {
        handle((Filter) node);
      } else if (node instanceof Project) {
        handle((Project) node);
      } else if (node instanceof TableFunctionScan) {
        handle((TableFunctionScan) node);
      } else if (node instanceof Join) {
        ASTConverter.this.from = node;
      } else if (node instanceof Union) {
        ASTConverter.this.from = node;
      } else if (node instanceof Aggregate) {
        ASTConverter.this.groupBy = (Aggregate) node;
      } else if (node instanceof Sort || node instanceof Exchange) {
        if (ASTConverter.this.select != null) {
          ASTConverter.this.from = node;
        } else {
          ASTConverter.this.orderLimit = node;
        }
      } else if (node instanceof Values) {
        handle((HiveValues) node);
      }
      /*
       * once the source node is reached; stop traversal for this QB
       */
      if (ASTConverter.this.from == null) {
        node.childrenAccept(this);
      }
    }

  }


  static class RexVisitor extends RexVisitorImpl<ASTNode> {

    private final Schema schema;
    private final RexBuilder rexBuilder;
    // this is to keep track of null literal which already has been visited
    private Map<RexLiteral, Boolean> nullLiteralMap ;


    protected RexVisitor(Schema schema, boolean useTypeQualInLiteral) {
      this(schema, useTypeQualInLiteral, null);

    }
    protected RexVisitor(Schema schema) {
      this(schema, false);
    }

    protected RexVisitor(Schema schema, boolean useTypeQualInLiteral, RexBuilder rexBuilder) {
      super(true);
      this.schema = schema;
      this.rexBuilder = rexBuilder;

      this.nullLiteralMap =
          new TreeMap<>(new Comparator<RexLiteral>(){
            // RexLiteral's equal only consider value and type which isn't sufficient
            // so providing custom comparator which distinguishes b/w objects irrespective
            // of value/type
            @Override
            public int compare(RexLiteral o1, RexLiteral o2) {
              if(o1 == o2) {
                return 0;
              } else {
                return 1;
              }
            }
          });
    }

    @Override
    public ASTNode visitFieldAccess(RexFieldAccess fieldAccess) {
      return ASTBuilder.construct(HiveParser.DOT, ".").add(super.visitFieldAccess(fieldAccess))
          .add(HiveParser.Identifier, fieldAccess.getField().getName()).node();
    }

    @Override
    public ASTNode visitInputRef(RexInputRef inputRef) {
      ColumnInfo cI = schema.get(inputRef.getIndex());
      if (cI.agg != null) {
        return (ASTNode) ParseDriver.adaptor.dupTree(cI.agg);
      }

      if (cI.table == null || cI.table.isEmpty()) {
        return ASTBuilder.unqualifiedName(cI.column);
      } else {
        return ASTBuilder.qualifiedName(cI.table, cI.column);
      }

    }

    @Override
    public ASTNode visitLiteral(RexLiteral literal) {

      if (RexUtil.isNull(literal) && literal.getType().getSqlTypeName() != SqlTypeName.NULL
          && rexBuilder != null) {
        // It is NULL value with different type, we need to introduce a CAST
        // to keep it
        if(nullLiteralMap.containsKey(literal)) {
          return ASTBuilder.literal(literal);
        }
        nullLiteralMap.put(literal, true);
        RexNode r = rexBuilder.makeAbstractCast(literal.getType(), literal);

        return r.accept(this);
      }
      return ASTBuilder.literal(literal);
    }

    private ASTNode getPSpecAST(RexWindow window) {
      ASTNode pSpecAst = null;

      ASTNode dByAst = null;
      if (window.partitionKeys != null && !window.partitionKeys.isEmpty()) {
        dByAst = ASTBuilder.createAST(HiveParser.TOK_DISTRIBUTEBY, "TOK_DISTRIBUTEBY");
        for (RexNode pk : window.partitionKeys) {
          ASTNode astCol = pk.accept(this);
          dByAst.addChild(astCol);
        }
      }

      ASTNode oByAst = null;
      if (window.orderKeys != null && !window.orderKeys.isEmpty()) {
        oByAst = ASTBuilder.createAST(HiveParser.TOK_ORDERBY, "TOK_ORDERBY");
        for (RexFieldCollation ok : window.orderKeys) {
          ASTNode directionAST = ok.getDirection() == RelFieldCollation.Direction.ASCENDING ? ASTBuilder
              .createAST(HiveParser.TOK_TABSORTCOLNAMEASC, "TOK_TABSORTCOLNAMEASC") : ASTBuilder
              .createAST(HiveParser.TOK_TABSORTCOLNAMEDESC, "TOK_TABSORTCOLNAMEDESC");
          ASTNode nullDirectionAST;
          // Null direction
          if (ok.right.contains(SqlKind.NULLS_FIRST)) {
            nullDirectionAST = ASTBuilder.createAST(HiveParser.TOK_NULLS_FIRST, "TOK_NULLS_FIRST");
            directionAST.addChild(nullDirectionAST);
          } else if (ok.right.contains(SqlKind.NULLS_LAST)) {
            nullDirectionAST = ASTBuilder.createAST(HiveParser.TOK_NULLS_LAST, "TOK_NULLS_LAST");
            directionAST.addChild(nullDirectionAST);
          } else {
            // Default
            if (ok.getDirection() == RelFieldCollation.Direction.ASCENDING) {
              nullDirectionAST = ASTBuilder.createAST(HiveParser.TOK_NULLS_FIRST, "TOK_NULLS_FIRST");
              directionAST.addChild(nullDirectionAST);
            } else {
              nullDirectionAST = ASTBuilder.createAST(HiveParser.TOK_NULLS_LAST, "TOK_NULLS_LAST");
              directionAST.addChild(nullDirectionAST);
            }
          }
          ASTNode astCol = ok.left.accept(this);

          nullDirectionAST.addChild(astCol);
          oByAst.addChild(directionAST);
        }
      }

      if (dByAst != null || oByAst != null) {
        pSpecAst = ASTBuilder.createAST(HiveParser.TOK_PARTITIONINGSPEC, "TOK_PARTITIONINGSPEC");
        if (dByAst != null) {
          pSpecAst.addChild(dByAst);
        }
        if (oByAst != null) {
          pSpecAst.addChild(oByAst);
        }
      }

      return pSpecAst;
    }

    private ASTNode getWindowBound(RexWindowBound wb) {
      ASTNode wbAST = null;

      if (wb.isCurrentRow()) {
        wbAST = ASTBuilder.createAST(HiveParser.KW_CURRENT, "CURRENT");
      } else {
        if (wb.isPreceding()) {
          wbAST = ASTBuilder.createAST(HiveParser.KW_PRECEDING, "PRECEDING");
        } else {
          wbAST = ASTBuilder.createAST(HiveParser.KW_FOLLOWING, "FOLLOWING");
        }
        if (wb.isUnbounded()) {
          wbAST.addChild(ASTBuilder.createAST(HiveParser.KW_UNBOUNDED, "UNBOUNDED"));
        } else {
          ASTNode offset = wb.getOffset().accept(this);
          wbAST.addChild(offset);
        }
      }

      return wbAST;
    }

    private ASTNode getWindowRangeAST(RexWindow window) {
      ASTNode wRangeAst = null;

      ASTNode startAST = null;
      boolean lbUnbounded = false;
      RexWindowBound lb = window.getLowerBound();
      if (lb != null) {
        startAST = getWindowBound(lb);
        lbUnbounded = lb.isUnbounded();
      }

      ASTNode endAST = null;
      boolean ubUnbounded = false;
      RexWindowBound ub = window.getUpperBound();
      if (ub != null) {
        endAST = getWindowBound(ub);
        ubUnbounded = ub.isUnbounded();
      }

      if (startAST != null || endAST != null) {
        // NOTE: in Hive AST Rows->Range(Physical) & Range -> Values (logical)
        // In Calcite, "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
        // is represented as "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
        // since they are equivalent. However, in Hive, it is most commonly represented
        // as "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING".
        if (window.isRows() || (lbUnbounded && ubUnbounded)) {
          wRangeAst = ASTBuilder.createAST(HiveParser.TOK_WINDOWRANGE, "TOK_WINDOWRANGE");
        } else {
          wRangeAst = ASTBuilder.createAST(HiveParser.TOK_WINDOWVALUES, "TOK_WINDOWVALUES");
        }
        if (startAST != null) {
          wRangeAst.addChild(startAST);
        }
        if (endAST != null) {
          wRangeAst.addChild(endAST);
        }
      }

      return wRangeAst;
    }

    @Override
    public ASTNode visitOver(RexOver over) {
      if (!deep) {
        return null;
      }

      // 1. Translate the UDAF
      final ASTNode wUDAFAst = visitCall(over);

      // 2. Add TOK_WINDOW as child of UDAF
      ASTNode wSpec = ASTBuilder.createAST(HiveParser.TOK_WINDOWSPEC, "TOK_WINDOWSPEC");
      wUDAFAst.addChild(wSpec);

      // 3. Add Part Spec & Range Spec as child of TOK_WINDOW
      final RexWindow window = over.getWindow();
      final ASTNode wPSpecAst = getPSpecAST(window);
      final ASTNode wRangeAst = getWindowRangeAST(window);
      if (wPSpecAst != null) {
        wSpec.addChild(wPSpecAst);
      }
      if (wRangeAst != null) {
        wSpec.addChild(wRangeAst);
      }
      if (over.ignoreNulls()) {
        ASTNode ignoreNulls = ASTBuilder.createAST(HiveParser.TOK_IGNORE_NULLS, "TOK_IGNORE_NULLS");
        wSpec.addChild(ignoreNulls);
      }

      return wUDAFAst;
    }

    @Override
    public ASTNode visitCall(RexCall call) {
      if (!deep) {
        return null;
      }

      SqlOperator op = call.getOperator();
      List<ASTNode> astNodeLst = new LinkedList<ASTNode>();
      switch (op.kind) {
      case EQUALS:
      case NOT_EQUALS:
      case LESS_THAN:
      case GREATER_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN_OR_EQUAL:
        if (rexBuilder != null && RexUtil.isReferenceOrAccess(call.operands.get(1), true) &&
            RexUtil.isLiteral(call.operands.get(0), true)) {
          // Swap to get reference on the left side
          return visitCall((RexCall) RexUtil.invert(rexBuilder, call));
        } else {
          for (RexNode operand : call.operands) {
            astNodeLst.add(operand.accept(this));
          }
        }
        break;
      case IS_DISTINCT_FROM:
        for (RexNode operand : call.operands) {
          astNodeLst.add(operand.accept(this));
        }
        return SqlFunctionConverter.buildAST(SqlStdOperatorTable.NOT,
          Collections.singletonList(SqlFunctionConverter.buildAST(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, astNodeLst, call.getType())), call.getType());
      case CAST:
        assert(call.getOperands().size() == 1);
        astNodeLst.add(convertType(call.getType()));
        astNodeLst.add(call.getOperands().get(0).accept(this));
        break;
      case EXTRACT:
        // Extract on date: special handling since function in Hive does
        // include <time_unit>. Observe that <time_unit> information
        // is implicit in the function name, thus translation will
        // proceed correctly if we just ignore the <time_unit>
        astNodeLst.add(call.operands.get(1).accept(this));
        break;
      case FLOOR:
        if (call.operands.size() == 2) {
          // Floor on date: special handling since function in Hive does
          // include <time_unit>. Observe that <time_unit> information
          // is implicit in the function name, thus translation will
          // proceed correctly if we just ignore the <time_unit>
          astNodeLst.add(call.operands.get(0).accept(this));
          break;
        }
        // fall-through
      default:
        for (RexNode operand : call.operands) {
          astNodeLst.add(operand.accept(this));
        }
      }

      if (isFlat(call)) {
        return SqlFunctionConverter.buildAST(op, astNodeLst, 0);
      } else {
        return SqlFunctionConverter.buildAST(op, astNodeLst, call.getType());
      }
    }

    @Override
    public ASTNode visitDynamicParam(RexDynamicParam dynamicParam) {
      return ASTBuilder.dynamicParam(dynamicParam);
    }
  }

  static class QueryBlockInfo {
    Schema  schema;
    ASTNode ast;

    public QueryBlockInfo(Schema schema, ASTNode ast) {
      super();
      this.schema = schema;
      this.ast = ast;
    }
  }

  /*
   * represents the schema exposed by a QueryBlock.
   */
  static class Schema extends ArrayList<ColumnInfo> {

    private static final long serialVersionUID = 1L;

    Schema(TableScan scan) {
      HiveTableScan hts = (HiveTableScan) scan;
      String tabName = hts.getTableAlias();
      for (RelDataTypeField field : scan.getRowType().getFieldList()) {
        add(new ColumnInfo(tabName, field.getName()));
      }
    }

    Schema(DruidQuery dq) {
      HiveTableScan hts = (HiveTableScan) dq.getTableScan();
      String tabName = hts.getTableAlias();
      for (RelDataTypeField field : dq.getRowType().getFieldList()) {
        add(new ColumnInfo(tabName, field.getName()));
      }
    }

    Schema(HiveJdbcConverter scan) {
      HiveJdbcConverter jdbcHiveCoverter = scan;
      final JdbcHiveTableScan jdbcTableScan = jdbcHiveCoverter.getTableScan();
      String tabName = jdbcTableScan.getHiveTableScan().getTableAlias();
      for (RelDataTypeField field : jdbcHiveCoverter.getRowType().getFieldList()) {
        add(new ColumnInfo(tabName, field.getName()));
      }
    }

    Schema(Project select, String alias) {
      for (RelDataTypeField field : select.getRowType().getFieldList()) {
        add(new ColumnInfo(alias, field.getName()));
      }
    }

    Schema(TableFunctionScan select, String alias) {
      for (RelDataTypeField field : select.getRowType().getFieldList()) {
        add(new ColumnInfo(alias, field.getName()));
      }
    }

    Schema(Union unionRel, String alias) {
      for (RelDataTypeField field : unionRel.getRowType().getFieldList()) {
        add(new ColumnInfo(alias, field.getName()));
      }
    }

    Schema(Schema left, Schema right) {
      for (ColumnInfo cI : Iterables.concat(left, right)) {
        add(cI);
      }
    }

    Schema(Schema src, Aggregate gBy) {
      for (int i : gBy.getGroupSet()) {
        ColumnInfo cI = src.get(i);
        add(cI);
      }
      List<AggregateCall> aggs = gBy.getAggCallList();
      for (AggregateCall agg : aggs) {
        if (agg.getAggregation() == HiveGroupingID.INSTANCE) {
          add(new ColumnInfo(null,VirtualColumn.GROUPINGID.getName()));
          continue;
        }
        int argCount = agg.getArgList().size();
        ASTBuilder b = agg.isDistinct() ? ASTBuilder.construct(HiveParser.TOK_FUNCTIONDI,
            "TOK_FUNCTIONDI") : argCount == 0 ? ASTBuilder.construct(HiveParser.TOK_FUNCTIONSTAR,
            "TOK_FUNCTIONSTAR") : ASTBuilder.construct(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
        b.add(HiveParser.Identifier, agg.getAggregation().getName());
        Iterator<RelFieldCollation> collationIterator = agg.collation.getFieldCollations().listIterator();
        RexBuilder rexBuilder = gBy.getCluster().getRexBuilder();
        for (int i : agg.getArgList()) {
          RexInputRef iRef = new RexInputRef(i, gBy.getCluster().getTypeFactory()
              .createSqlType(SqlTypeName.ANY));
          b.add(iRef.accept(new RexVisitor(src, false, gBy.getCluster().getRexBuilder())));
          if (collationIterator.hasNext()) {
            RelFieldCollation fieldCollation = collationIterator.next();
            RexInputRef inputRef = new RexInputRef(fieldCollation.getFieldIndex(),
                    gBy.getCluster().getTypeFactory().createSqlType(SqlTypeName.ANY));
            b.add(inputRef.accept(new RexVisitor(src, false, rexBuilder)));
            b.add(ASTBuilder.createAST(HiveParser.NumberLiteral,
                    Integer.toString(DirectionUtils.directionToCode(fieldCollation.getDirection()))));
            b.add(ASTBuilder.createAST(HiveParser.NumberLiteral,
                    Integer.toString(NullOrdering.fromDirection(fieldCollation.nullDirection).getCode())));
          }
        }
        add(new ColumnInfo(null, b.node()));
      }
    }

    /**
     * Assumption:<br>
     * 1. Project will always be child of Sort.<br>
     * 2. In Calcite every projection in Project is uniquely named
     * (unambiguous) without using table qualifier (table name).<br>
     *
     * @param order
     *          Hive Sort Node
     * @return Schema
     */
    Schema(HiveSortLimit order) {
      this((Project) order.getInput(), null);
    }

    Schema(HiveSortExchange sort) {
      this((Project) sort.getInput(), null);
    }

    public Schema(String tabAlias, List<RelDataTypeField> fieldList) {
      for (RelDataTypeField field : fieldList) {
        add(new ColumnInfo(tabAlias, field.getName()));
      }
    }
  }

  /*
   * represents Column information exposed by a QueryBlock.
   */
  static class ColumnInfo {
    String  table;
    String  column;
    ASTNode agg;

    ColumnInfo(String table, String column) {
      super();
      this.table = table;
      this.column = column;
    }

    ColumnInfo(String table, ASTNode agg) {
      super();
      this.table = table;
      this.agg = agg;
    }

    ColumnInfo(String alias, ColumnInfo srcCol) {
      this.table = alias;
      this.column = srcCol.column;
      this.agg = srcCol.agg;
    }
  }

  private String nextAlias() {
    String tabAlias = String.format("$hdt$_%d", derivedTableCount);
    derivedTableCount++;
    return tabAlias;
  }

  static class HiveAST {

    ASTNode from;
    ASTNode where;
    ASTNode groupBy;
    ASTNode having;
    ASTNode select;
    ASTNode order;
    ASTNode limit;

    public ASTNode getAST() {
      ASTBuilder b = ASTBuilder
          .construct(HiveParser.TOK_QUERY, "TOK_QUERY")
          .add(from)
          .add(
              ASTBuilder.construct(HiveParser.TOK_INSERT, "TOK_INSERT").add(ASTBuilder.destNode())
                  .add(select).add(where).add(groupBy).add(having).add(order).add(limit));
      return b.node();
    }
  }

  public ASTNode getUnionAllAST(ASTNode leftAST, ASTNode rightAST) {

    ASTNode unionTokAST = ASTBuilder.construct(HiveParser.TOK_UNIONALL, "TOK_UNIONALL").add(leftAST)
        .add(rightAST).node();

    return unionTokAST;
  }

  public static boolean isFlat(RexCall call) {
    boolean flat = false;
    if (call.operands != null && call.operands.size() > 2) {
      SqlOperator op = call.getOperator();
      if (op.getKind() == SqlKind.AND || op.getKind() == SqlKind.OR) {
        flat = true;
      }
    }

    return flat;
  }

}
