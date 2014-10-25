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
package org.apache.hadoop.hive.ql.optimizer.optiq.translator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.hydromatic.optiq.util.BitSets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqSemanticException;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveSortRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.translator.SqlFunctionConverter.HiveToken;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelVisitor;
import org.eigenbase.rel.SortRel;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.rel.UnionRelBase;
import org.eigenbase.rel.rules.SemiJoinRel;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexFieldAccess;
import org.eigenbase.rex.RexFieldCollation;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexOver;
import org.eigenbase.rex.RexVisitorImpl;
import org.eigenbase.rex.RexWindow;
import org.eigenbase.rex.RexWindowBound;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.type.SqlTypeName;

import com.google.common.collect.Iterables;

public class ASTConverter {
  private static final Log LOG = LogFactory.getLog(ASTConverter.class);

  private RelNode          root;
  private HiveAST          hiveAST;
  private RelNode          from;
  private FilterRelBase    where;
  private AggregateRelBase groupBy;
  private FilterRelBase    having;
  private ProjectRelBase   select;
  private SortRel          order;
  private SortRel          limit;

  private Schema           schema;

  private long             derivedTableCount;

  ASTConverter(RelNode root, long dtCounterInitVal) {
    this.root = root;
    hiveAST = new HiveAST();
    this.derivedTableCount = dtCounterInitVal;
  }

  public static ASTNode convert(final RelNode relNode, List<FieldSchema> resultSchema)
      throws OptiqSemanticException {
    RelNode root = PlanModifierForASTConv.convertOpTree(relNode, resultSchema);
    ASTConverter c = new ASTConverter(root, 0);
    return c.convert();
  }

  private ASTNode convert() {
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
      ASTBuilder b = ASTBuilder.construct(HiveParser.TOK_GROUPBY, "TOK_GROUPBY");
      for (int i : BitSets.toIter(groupBy.getGroupSet())) {
        RexInputRef iRef = new RexInputRef(i, groupBy.getCluster().getTypeFactory()
            .createSqlType(SqlTypeName.ANY));
        b.add(iRef.accept(new RexVisitor(schema)));
      }

      if (!groupBy.getGroupSet().isEmpty())
        hiveAST.groupBy = b.node();
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

    if (select.getChildExps().isEmpty()) {
      RexLiteral r = select.getCluster().getRexBuilder().makeExactLiteral(new BigDecimal(1));
      ASTNode selectExpr = ASTBuilder.selectExpr(ASTBuilder.literal(r), "1");
      b.add(selectExpr);
    } else {
      int i = 0;

      for (RexNode r : select.getChildExps()) {
        ASTNode selectExpr = ASTBuilder.selectExpr(r.accept(
             new RexVisitor(schema, r instanceof RexLiteral)),
                  select.getRowType().getFieldNames().get(i++));
        b.add(selectExpr);
      }
    }
    hiveAST.select = b.node();

    /*
     * 7. Order Use in Order By from the block above. RelNode has no pointer to
     * parent hence we need to go top down; but OB at each block really belong
     * to its src/from. Hence the need to pass in sortRel for each block from
     * its parent.
     */
    convertOBToASTNode((HiveSortRel) order);

    // 8. Limit
    convertLimitToASTNode((HiveSortRel) limit);

    return hiveAST.getAST();
  }

  private void convertLimitToASTNode(HiveSortRel limit) {
    if (limit != null) {
      HiveSortRel hiveLimit = (HiveSortRel) limit;
      RexNode limitExpr = hiveLimit.getFetchExpr();
      if (limitExpr != null) {
        Object val = ((RexLiteral) limitExpr).getValue2();
        hiveAST.limit = ASTBuilder.limit(val);
      }
    }
  }

  private void convertOBToASTNode(HiveSortRel order) {
    if (order != null) {
      HiveSortRel hiveSort = (HiveSortRel) order;
      if (!hiveSort.getCollation().getFieldCollations().isEmpty()) {
        // 1 Add order by token
        ASTNode orderAst = ASTBuilder.createAST(HiveParser.TOK_ORDERBY, "TOK_ORDERBY");

        schema = new Schema((HiveSortRel) hiveSort);
        Map<Integer, RexNode> obRefToCallMap = hiveSort.getInputRefToCallMap();
        RexNode obExpr;
        ASTNode astCol;
        for (RelFieldCollation c : hiveSort.getCollation().getFieldCollations()) {

          // 2 Add Direction token
          ASTNode directionAST = c.getDirection() == RelFieldCollation.Direction.ASCENDING ? ASTBuilder
              .createAST(HiveParser.TOK_TABSORTCOLNAMEASC, "TOK_TABSORTCOLNAMEASC") : ASTBuilder
              .createAST(HiveParser.TOK_TABSORTCOLNAMEDESC, "TOK_TABSORTCOLNAMEDESC");

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
          directionAST.addChild(astCol);
          orderAst.addChild(directionAST);
        }
        hiveAST.order = orderAst;
      }
    }
  }

  private Schema getRowSchema(String tblAlias) {
    return new Schema(select, tblAlias);
  }

  private QueryBlockInfo convertSource(RelNode r) {
    Schema s;
    ASTNode ast;

    if (r instanceof TableAccessRelBase) {
      TableAccessRelBase f = (TableAccessRelBase) r;
      s = new Schema(f);
      ast = ASTBuilder.table(f);
    } else if (r instanceof JoinRelBase) {
      JoinRelBase join = (JoinRelBase) r;
      QueryBlockInfo left = convertSource(join.getLeft());
      QueryBlockInfo right = convertSource(join.getRight());
      s = new Schema(left.schema, right.schema);
      ASTNode cond = join.getCondition().accept(new RexVisitor(s));
      boolean semiJoin = join instanceof SemiJoinRel;
      ast = ASTBuilder.join(left.ast, right.ast, join.getJoinType(), cond, semiJoin);
      if (semiJoin)
        s = left.schema;
    } else if (r instanceof UnionRelBase) {
      RelNode leftInput = ((UnionRelBase) r).getInput(0);
      RelNode rightInput = ((UnionRelBase) r).getInput(1);

      ASTConverter leftConv = new ASTConverter(leftInput, this.derivedTableCount);
      ASTConverter rightConv = new ASTConverter(rightInput, this.derivedTableCount);
      ASTNode leftAST = leftConv.convert();
      ASTNode rightAST = rightConv.convert();

      ASTNode unionAST = getUnionAllAST(leftAST, rightAST);

      String sqAlias = nextAlias();
      ast = ASTBuilder.subQuery(unionAST, sqAlias);
      s = new Schema((UnionRelBase) r, sqAlias);
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

    public void handle(FilterRelBase filter) {
      RelNode child = filter.getChild();
      if (child instanceof AggregateRelBase && !((AggregateRelBase) child).getGroupSet().isEmpty()) {
        ASTConverter.this.having = filter;
      } else {
        ASTConverter.this.where = filter;
      }
    }

    public void handle(ProjectRelBase project) {
      if (ASTConverter.this.select == null) {
        ASTConverter.this.select = project;
      } else {
        ASTConverter.this.from = project;
      }
    }

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {

      if (node instanceof TableAccessRelBase) {
        ASTConverter.this.from = node;
      } else if (node instanceof FilterRelBase) {
        handle((FilterRelBase) node);
      } else if (node instanceof ProjectRelBase) {
        handle((ProjectRelBase) node);
      } else if (node instanceof JoinRelBase) {
        ASTConverter.this.from = node;
      } else if (node instanceof UnionRelBase) {
        ASTConverter.this.from = node;
      } else if (node instanceof AggregateRelBase) {
        ASTConverter.this.groupBy = (AggregateRelBase) node;
      } else if (node instanceof SortRel) {
        if (ASTConverter.this.select != null) {
          ASTConverter.this.from = node;
        } else {
          SortRel hiveSortRel = (SortRel) node;
          if (hiveSortRel.getCollation().getFieldCollations().isEmpty())
            ASTConverter.this.limit = hiveSortRel;
          else
            ASTConverter.this.order = hiveSortRel;
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
    private boolean useTypeQualInLiteral;

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
          ASTNode astNode = ok.getDirection() == RelFieldCollation.Direction.ASCENDING ? ASTBuilder
              .createAST(HiveParser.TOK_TABSORTCOLNAMEASC, "TOK_TABSORTCOLNAMEASC") : ASTBuilder
              .createAST(HiveParser.TOK_TABSORTCOLNAMEDESC, "TOK_TABSORTCOLNAMEDESC");
          ASTNode astCol = ok.left.accept(this);
          astNode.addChild(astCol);
          oByAst.addChild(astNode);
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

      for (RexNode operand : call.operands) {
        astNodeLst.add(operand.accept(this));
      }

      if (isFlat(call))
        return SqlFunctionConverter.buildAST(op, astNodeLst, 0);
      else
        return SqlFunctionConverter.buildAST(op, astNodeLst);
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

    Schema(TableAccessRelBase scan) {
      String tabName = ((RelOptHiveTable) scan.getTable()).getTableAlias();
      for (RelDataTypeField field : scan.getRowType().getFieldList()) {
        add(new ColumnInfo(tabName, field.getName()));
      }
    }

    Schema(ProjectRelBase select, String alias) {
      for (RelDataTypeField field : select.getRowType().getFieldList()) {
        add(new ColumnInfo(alias, field.getName()));
      }
    }

    Schema(UnionRelBase unionRel, String alias) {
      for (RelDataTypeField field : unionRel.getRowType().getFieldList()) {
        add(new ColumnInfo(alias, field.getName()));
      }
    }

    @SuppressWarnings("unchecked")
    Schema(Schema left, Schema right) {
      for (ColumnInfo cI : Iterables.concat(left, right)) {
        add(cI);
      }
    }

    Schema(Schema src, AggregateRelBase gBy) {
      for (int i : BitSets.toIter(gBy.getGroupSet())) {
        ColumnInfo cI = src.get(i);
        add(cI);
      }
      List<AggregateCall> aggs = gBy.getAggCallList();
      for (AggregateCall agg : aggs) {
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
     * 1. ProjectRel will always be child of SortRel.<br>
     * 2. In Optiq every projection in ProjectRelBase is uniquely named
     * (unambigous) without using table qualifier (table name).<br>
     * 
     * @param order
     *          Hive Sort Rel Node
     * @return Schema
     */
    public Schema(HiveSortRel order) {
      ProjectRelBase select = (ProjectRelBase) order.getChild();
      for (String projName : select.getRowType().getFieldNames()) {
        add(new ColumnInfo(null, projName));
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

    ASTNode unionTokAST = ASTBuilder.construct(HiveParser.TOK_UNION, "TOK_UNION").add(leftAST)
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
