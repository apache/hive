package org.apache.hadoop.hive.ql.optimizer.optiq.translator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import net.hydromatic.optiq.util.BitSets;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
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
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexVisitorImpl;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.type.BasicSqlType;
import org.eigenbase.sql.type.SqlTypeName;

import com.google.common.collect.Iterables;

public class ASTConverter {

  RelNode          root;
  HiveAST          hiveAST;
  RelNode          from;
  FilterRelBase    where;
  AggregateRelBase groupBy;
  FilterRelBase    having;
  ProjectRelBase   select;
  SortRel          order;

  Schema           schema;
  
  ASTConverter(RelNode root) {
    this.root = root;
    hiveAST = new HiveAST();
  }

  public static ASTNode convert(final RelNode relNode, List<FieldSchema> resultSchema) {
    SortRel sortrel = null;
    RelNode root = DerivedTableInjector.convertOpTree(relNode, resultSchema);

    if (root instanceof SortRel) {
      sortrel = (SortRel) root;
      root = sortrel.getChild();
      if (!(root instanceof ProjectRelBase))
        throw new RuntimeException("Child of root sort node is not a project");
    }

    ASTConverter c = new ASTConverter(root);
    return c.convert(sortrel);
  }

  public ASTNode convert(SortRel sortrel) {
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
        RexInputRef iRef = new RexInputRef(i, new BasicSqlType(SqlTypeName.ANY));
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
    int i = 0;
    ASTBuilder b = ASTBuilder.construct(HiveParser.TOK_SELECT, "TOK_SELECT");

    for (RexNode r : select.getChildExps()) {
      ASTNode selectExpr = ASTBuilder.selectExpr(r.accept(new RexVisitor(schema)), select
          .getRowType().getFieldNames().get(i++));
      b.add(selectExpr);
    }
    hiveAST.select = b.node();

    /*
     * 7. Order
     * Use in Order By from the block above. RelNode has no pointer to parent
     * hence we need to go top down; but OB at each block really belong to its
     * src/from. Hence the need to pass in sortRel for each block from its parent.
     */
    if (sortrel != null) {
      HiveSortRel hiveSort = (HiveSortRel) sortrel;
      if (!hiveSort.getCollation().getFieldCollations().isEmpty()) {
        ASTNode orderAst = ASTBuilder.createAST(HiveParser.TOK_ORDERBY, "TOK_ORDERBY");
        schema = new Schema((HiveSortRel) sortrel);
        for (RelFieldCollation c : hiveSort.getCollation().getFieldCollations()) {
          ColumnInfo cI = schema.get(c.getFieldIndex());
          /*
           * The RowResolver setup for Select drops Table associations. So setup
           * ASTNode on unqualified name.
           */
          ASTNode astCol = ASTBuilder.unqualifiedName(cI.column);
          ASTNode astNode = c.getDirection() == RelFieldCollation.Direction.ASCENDING
              ? ASTBuilder.createAST(HiveParser.TOK_TABSORTCOLNAMEASC, "TOK_TABSORTCOLNAMEASC")
              : ASTBuilder.createAST(HiveParser.TOK_TABSORTCOLNAMEDESC, "TOK_TABSORTCOLNAMEDESC");
          astNode.addChild(astCol);
          orderAst.addChild(astNode);
        }
        hiveAST.order = orderAst;
      }
      RexNode limitExpr = hiveSort.getFetchExpr();
      if (limitExpr != null) {
        Object val = ((RexLiteral) limitExpr).getValue2();
        hiveAST.limit = ASTBuilder.limit(val);
      }

    }

    return hiveAST.getAST();
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
      boolean semiJoin = ((join instanceof HiveJoinRel) && ((HiveJoinRel)join).isLeftSemiJoin()) ? true : false;
      ast = ASTBuilder.join(left.ast, right.ast, join.getJoinType(), cond, semiJoin);
      if (semiJoin)
        s = left.schema;
    } else {
      ASTConverter src = new ASTConverter(r);
      ASTNode srcAST = src.convert(order);
      String sqAlias = ASTConverter.nextAlias();
      s = src.getRowSchema(sqAlias);
      ast = ASTBuilder.subQuery(srcAST, sqAlias);
    }
    return new QueryBlockInfo(s, ast);
  }

  class QBVisitor extends RelVisitor {

    public void handle(FilterRelBase filter) {
      RelNode child = filter.getChild();
      if (child instanceof AggregateRelBase) {
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
      } else if (node instanceof AggregateRelBase) {
        ASTConverter.this.groupBy = (AggregateRelBase) node;
      } else if (node instanceof SortRel) {
        ASTConverter.this.order = (SortRel) node;
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

    protected RexVisitor(Schema schema) {
      super(true);
      this.schema = schema;
    }

    @Override
    public ASTNode visitInputRef(RexInputRef inputRef) {
      ColumnInfo cI = schema.get(inputRef.getIndex());
      if (cI.agg != null) {
        return (ASTNode) ParseDriver.adaptor.dupTree(cI.agg);
      }
      return ASTBuilder.qualifiedName(cI.table, cI.column);
    }

    @Override
    public ASTNode visitLiteral(RexLiteral literal) {
      return ASTBuilder.literal(literal);
    }

    @Override
    public ASTNode visitCall(RexCall call) {
      if (!deep) {
        return null;
      }

      SqlOperator op = call.getOperator();
      List<ASTNode> astNodeLst = new LinkedList<ASTNode>();
      if (op.kind == SqlKind.CAST) {
        HiveToken ht = TypeConverter.convert(call.getType());
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
      String tabName = scan.getTable().getQualifiedName().get(0);
      for (RelDataTypeField field : scan.getRowType().getFieldList()) {
        add(new ColumnInfo(tabName, field.getName()));
      }
    }

    Schema(ProjectRelBase select, String alias) {
      for (RelDataTypeField field : select.getRowType().getFieldList()) {
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
          RexInputRef iRef = new RexInputRef(i, new BasicSqlType(SqlTypeName.ANY));
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

  static String nextAlias() {
    return String.format("$hdt$_%d", derivedTableCounter.getAndIncrement());
  }

  private static AtomicLong derivedTableCounter = new AtomicLong(0);

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

  private static boolean isFlat(RexCall call) {
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
