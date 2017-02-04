/**
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExtractDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFloorDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveGroupingID;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter.HiveToken;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

public class ASTConverter {
  private static final Logger LOG = LoggerFactory.getLogger(ASTConverter.class);

  private final RelNode          root;
  private final HiveAST          hiveAST;
  private RelNode          from;
  private Filter           where;
  private Aggregate        groupBy;
  private Filter           having;
  private RelNode          select;
  private Sort             orderLimit;

  private Schema           schema;

  private long             derivedTableCount;

  ASTConverter(RelNode root, long dtCounterInitVal) {
    this.root = root;
    hiveAST = new HiveAST();
    this.derivedTableCount = dtCounterInitVal;
  }

  public static ASTNode convert(final RelNode relNode, List<FieldSchema> resultSchema, boolean alignColumns)
      throws CalciteSemanticException {
    RelNode root = PlanModifierForASTConv.convertOpTree(relNode, resultSchema, alignColumns);
    ASTConverter c = new ASTConverter(root, 0);
    return c.convert();
  }

  private ASTNode convert() throws CalciteSemanticException {
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
      ASTNode cond = where.getCondition().accept(new RexVisitor(schema));
      hiveAST.where = ASTBuilder.where(cond);
    }

    /*
     * 4. GBy
     */
    if (groupBy != null) {
      ASTBuilder b;
      boolean groupingSetsExpression = false;
      if (groupBy.indicator) {
        Group aggregateType = Aggregate.Group.induce(groupBy.getGroupSet(),
                groupBy.getGroupSets());
        if (aggregateType == Group.ROLLUP) {
          b = ASTBuilder.construct(HiveParser.TOK_ROLLUP_GROUPBY, "TOK_ROLLUP_GROUPBY");
        }
        else if (aggregateType == Group.CUBE) {
          b = ASTBuilder.construct(HiveParser.TOK_CUBE_GROUPBY, "TOK_CUBE_GROUPBY");
        }
        else {
          b = ASTBuilder.construct(HiveParser.TOK_GROUPING_SETS, "TOK_GROUPING_SETS");
          groupingSetsExpression = true;
        }
      }
      else {
        b = ASTBuilder.construct(HiveParser.TOK_GROUPBY, "TOK_GROUPBY");
      }

      HiveAggregate hiveAgg = (HiveAggregate) groupBy;
      for (int pos : hiveAgg.getAggregateColumnsOrder()) {
        RexInputRef iRef = new RexInputRef(groupBy.getGroupSet().nth(pos),
            groupBy.getCluster().getTypeFactory().createSqlType(SqlTypeName.ANY));
        b.add(iRef.accept(new RexVisitor(schema)));
      }
      for (int pos = 0; pos < groupBy.getGroupCount(); pos++) {
        if (!hiveAgg.getAggregateColumnsOrder().contains(pos)) {
          RexInputRef iRef = new RexInputRef(groupBy.getGroupSet().nth(pos),
              groupBy.getCluster().getTypeFactory().createSqlType(SqlTypeName.ANY));
          b.add(iRef.accept(new RexVisitor(schema)));
        }
      }

      //Grouping sets expressions
      if(groupingSetsExpression) {
        for(ImmutableBitSet groupSet: groupBy.getGroupSets()) {
          ASTBuilder expression = ASTBuilder.construct(
                  HiveParser.TOK_GROUPING_SETS_EXPRESSION, "TOK_GROUPING_SETS_EXPRESSION");
          for (int i : groupSet) {
            RexInputRef iRef = new RexInputRef(i, groupBy.getCluster().getTypeFactory()
                .createSqlType(SqlTypeName.ANY));
            expression.add(iRef.accept(new RexVisitor(schema)));
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
      ASTNode cond = having.getCondition().accept(new RexVisitor(schema));
      hiveAST.having = ASTBuilder.having(cond);
    }

    /*
     * 6. Project
     */
    ASTBuilder b = ASTBuilder.construct(HiveParser.TOK_SELECT, "TOK_SELECT");

    if (select instanceof Project) {
      if (select.getChildExps().isEmpty()) {
        RexLiteral r = select.getCluster().getRexBuilder().makeExactLiteral(new BigDecimal(1));
        ASTNode selectExpr = ASTBuilder.selectExpr(ASTBuilder.literal(r), "1");
        b.add(selectExpr);
      } else {
        int i = 0;

        for (RexNode r : select.getChildExps()) {
          if (RexUtil.isNull(r) && r.getType().getSqlTypeName() != SqlTypeName.NULL) {
            // It is NULL value with different type, we need to introduce a CAST
            // to keep it
            r = select.getCluster().getRexBuilder().makeAbstractCast(r.getType(), r);
          }
          ASTNode expr = r.accept(new RexVisitor(schema, r instanceof RexLiteral));
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
        if (RexUtil.isNull(r) && r.getType().getSqlTypeName() != SqlTypeName.NULL) {
          // It is NULL value with different type, we need to introduce a CAST
          // to keep it
          r = select.getCluster().getRexBuilder().makeAbstractCast(r.getType(), r);
        }
        ASTNode expr = r.accept(new RexVisitor(schema, r instanceof RexLiteral));
        children.add(expr);
      }
      ASTBuilder sel = ASTBuilder.construct(HiveParser.TOK_SELEXPR, "TOK_SELEXPR");
      ASTNode function = buildUDTFAST(call.getOperator().getName(), children);
      sel.add(function);
      for (String alias : udtf.getRowType().getFieldNames()) {
        sel.add(HiveParser.Identifier, alias);
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
    convertOrderLimitToASTNode((HiveSortLimit) orderLimit);

    return hiveAST.getAST();
  }

  private ASTNode buildUDTFAST(String functionName, List<ASTNode> children) {
    ASTNode node = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
    node.addChild((ASTNode) ParseDriver.adaptor.create(HiveParser.Identifier, functionName));
    for (ASTNode c : children) {
      ParseDriver.adaptor.addChild(node, c);
    }
    return node;
  }
  private void convertOrderLimitToASTNode(HiveSortLimit order) {
    if (order != null) {
      HiveSortLimit hiveSortLimit = order;
      if (!hiveSortLimit.getCollation().getFieldCollations().isEmpty()) {
        // 1 Add order by token
        ASTNode orderAst = ASTBuilder.createAST(HiveParser.TOK_ORDERBY, "TOK_ORDERBY");

        schema = new Schema(hiveSortLimit);
        Map<Integer, RexNode> obRefToCallMap = hiveSortLimit.getInputRefToCallMap();
        RexNode obExpr;
        ASTNode astCol;
        for (RelFieldCollation c : hiveSortLimit.getCollation().getFieldCollations()) {

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
          if (obRefToCallMap != null)
            obExpr = obRefToCallMap.get(c.getFieldIndex());

          if (obExpr != null) {
            astCol = obExpr.accept(new RexVisitor(schema));
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

      RexNode offsetExpr = hiveSortLimit.getOffsetExpr();
      RexNode fetchExpr = hiveSortLimit.getFetchExpr();
      if (fetchExpr != null) {
        Object offset = (offsetExpr == null) ?
            new Integer(0) : ((RexLiteral) offsetExpr).getValue2();
        Object fetch = ((RexLiteral) fetchExpr).getValue2();
        hiveAST.limit = ASTBuilder.limit(offset, fetch);
      }
    }
  }

  private Schema getRowSchema(String tblAlias) {
    if (select instanceof Project) {
      return new Schema((Project) select, tblAlias);
    } else {
      return new Schema((TableFunctionScan) select, tblAlias);
    }
  }

  private QueryBlockInfo convertSource(RelNode r) throws CalciteSemanticException {
    Schema s = null;
    ASTNode ast = null;

    if (r instanceof TableScan) {
      TableScan f = (TableScan) r;
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
      ASTNode cond = join.getCondition().accept(new RexVisitor(s));
      boolean semiJoin = join instanceof SemiJoin;
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
        ast = ASTBuilder.join(right.ast, left.ast, type, cond, semiJoin);
      } else {
        ast = ASTBuilder.join(left.ast, right.ast, join.getJoinType(), cond, semiJoin);
      }
      if (semiJoin) {
        s = left.schema;
      }
    } else if (r instanceof Union) {
      Union u = ((Union) r);
      ASTNode left = new ASTConverter(((Union) r).getInput(0), this.derivedTableCount).convert();
      for (int ind = 1; ind < u.getInputs().size(); ind++) {
        left = getUnionAllAST(left, new ASTConverter(((Union) r).getInput(ind),
            this.derivedTableCount).convert());
        String sqAlias = nextAlias();
        ast = ASTBuilder.subQuery(left, sqAlias);
        s = new Schema((Union) r, sqAlias);
      }
    } else {
      ASTConverter src = new ASTConverter(r, this.derivedTableCount);
      ASTNode srcAST = src.convert();
      String sqAlias = nextAlias();
      s = src.getRowSchema(sqAlias);
      ast = ASTBuilder.subQuery(srcAST, sqAlias);
    }
    return new QueryBlockInfo(s, ast);
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

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {

      if (node instanceof TableScan ||
          node instanceof DruidQuery) {
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
      } else if (node instanceof Sort) {
        if (ASTConverter.this.select != null) {
          ASTConverter.this.from = node;
        } else {
          ASTConverter.this.orderLimit = (Sort) node;
        }
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
    private final boolean useTypeQualInLiteral;

    protected RexVisitor(Schema schema) {
      this(schema, false);
    }

    protected RexVisitor(Schema schema, boolean useTypeQualInLiteral) {
      super(true);
      this.schema = schema;
      this.useTypeQualInLiteral = useTypeQualInLiteral;
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

      if (cI.table == null || cI.table.isEmpty())
        return ASTBuilder.unqualifiedName(cI.column);
      else
        return ASTBuilder.qualifiedName(cI.table, cI.column);

    }

    @Override
    public ASTNode visitLiteral(RexLiteral literal) {
      return ASTBuilder.literal(literal, useTypeQualInLiteral);
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
        if (dByAst != null)
          pSpecAst.addChild(dByAst);
        if (oByAst != null)
          pSpecAst.addChild(oByAst);
      }

      return pSpecAst;
    }

    private ASTNode getWindowBound(RexWindowBound wb) {
      ASTNode wbAST = null;

      if (wb.isCurrentRow()) {
        wbAST = ASTBuilder.createAST(HiveParser.KW_CURRENT, "CURRENT");
      } else {
        if (wb.isPreceding())
          wbAST = ASTBuilder.createAST(HiveParser.KW_PRECEDING, "PRECEDING");
        else
          wbAST = ASTBuilder.createAST(HiveParser.KW_FOLLOWING, "FOLLOWING");
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
      RexWindowBound ub = window.getUpperBound();
      if (ub != null) {
        startAST = getWindowBound(ub);
      }

      ASTNode endAST = null;
      RexWindowBound lb = window.getLowerBound();
      if (lb != null) {
        endAST = getWindowBound(lb);
      }

      if (startAST != null || endAST != null) {
        // NOTE: in Hive AST Rows->Range(Physical) & Range -> Values (logical)
        if (window.isRows())
          wRangeAst = ASTBuilder.createAST(HiveParser.TOK_WINDOWRANGE, "TOK_WINDOWRANGE");
        else
          wRangeAst = ASTBuilder.createAST(HiveParser.TOK_WINDOWVALUES, "TOK_WINDOWVALUES");
        if (startAST != null)
          wRangeAst.addChild(startAST);
        if (endAST != null)
          wRangeAst.addChild(endAST);
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
      if (wPSpecAst != null)
        wSpec.addChild(wPSpecAst);
      if (wRangeAst != null)
        wSpec.addChild(wRangeAst);

      return wUDAFAst;
    }

    @Override
    public ASTNode visitCall(RexCall call) {
      if (!deep) {
        return null;
      }

      SqlOperator op = call.getOperator();
      List<ASTNode> astNodeLst = new LinkedList<ASTNode>();
      if (op.kind == SqlKind.CAST) {
        HiveToken ht = TypeConverter.hiveToken(call.getType());
        ASTBuilder astBldr = ASTBuilder.construct(ht.type, ht.text);
        if (ht.args != null) {
          for (String castArg : ht.args)
            astBldr.add(HiveParser.Identifier, castArg);
        }
        astNodeLst.add(astBldr.node());
      }

      if (op.kind == SqlKind.EXTRACT) {
        // Extract on date: special handling since function in Hive does
        // include <time_unit>. Observe that <time_unit> information
        // is implicit in the function name, thus translation will
        // proceed correctly if we just ignore the <time_unit>
        astNodeLst.add(call.operands.get(1).accept(this));
      } else if (op.kind == SqlKind.FLOOR &&
              call.operands.size() == 2) {
        // Floor on date: special handling since function in Hive does
        // include <time_unit>. Observe that <time_unit> information
        // is implicit in the function name, thus translation will
        // proceed correctly if we just ignore the <time_unit>
        astNodeLst.add(call.operands.get(0).accept(this));
      } else {
        for (RexNode operand : call.operands) {
          astNodeLst.add(operand.accept(this));
        }
      }

      if (isFlat(call)) {
        return SqlFunctionConverter.buildAST(op, astNodeLst, 0);
      } else {
        return SqlFunctionConverter.buildAST(op, astNodeLst);
      }
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
      HiveTableScan hts = (HiveTableScan) ((DruidQuery)dq).getTableScan();
      String tabName = hts.getTableAlias();
      for (RelDataTypeField field : dq.getRowType().getFieldList()) {
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
      // If we are using grouping sets, we add the
      // fields again, these correspond to the boolean
      // grouping in Calcite. They are not used by Hive.
      if(gBy.indicator) {
        for (int i : gBy.getGroupSet()) {
          ColumnInfo cI = src.get(i);
          add(cI);
        }
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
        for (int i : agg.getArgList()) {
          RexInputRef iRef = new RexInputRef(i, gBy.getCluster().getTypeFactory()
              .createSqlType(SqlTypeName.ANY));
          b.add(iRef.accept(new RexVisitor(src)));
        }
        add(new ColumnInfo(null, b.node()));
      }
    }

    /**
     * Assumption:<br>
     * 1. Project will always be child of Sort.<br>
     * 2. In Calcite every projection in Project is uniquely named
     * (unambigous) without using table qualifier (table name).<br>
     *
     * @param order
     *          Hive Sort Node
     * @return Schema
     */
    public Schema(HiveSortLimit order) {
      Project select = (Project) order.getInput();
      for (String projName : select.getRowType().getFieldNames()) {
        add(new ColumnInfo(null, projName));
      }
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
