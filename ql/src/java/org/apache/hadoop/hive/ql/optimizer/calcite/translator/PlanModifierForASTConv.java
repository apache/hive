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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Window.RexWinAggCall;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRelColumnsAlignment;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class PlanModifierForASTConv {

  private static final Logger LOG = LoggerFactory.getLogger(PlanModifierForASTConv.class);


  public static RelNode convertOpTree(RelNode rel, List<FieldSchema> resultSchema, boolean alignColumns)
      throws CalciteSemanticException {
    RelNode newTopNode = rel;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Original plan for PlanModifier\n " + RelOptUtil.toString(newTopNode));
    }

    if (!(newTopNode instanceof Project) && !(newTopNode instanceof Sort)) {
      newTopNode = introduceDerivedTable(newTopNode);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Plan after top-level introduceDerivedTable\n "
            + RelOptUtil.toString(newTopNode));
      }
    }

    convertOpTree(newTopNode, (RelNode) null);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Plan after nested convertOpTree\n " + RelOptUtil.toString(newTopNode));
    }

    if (alignColumns) {
      HiveRelColumnsAlignment propagator = new HiveRelColumnsAlignment(
          HiveRelFactories.HIVE_BUILDER.create(newTopNode.getCluster(), null));
      newTopNode = propagator.align(newTopNode);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Plan after propagating order\n " + RelOptUtil.toString(newTopNode));
      }
    }

    Pair<RelNode, RelNode> topSelparentPair = HiveCalciteUtil.getTopLevelSelect(newTopNode);
    PlanModifierUtil.fixTopOBSchema(newTopNode, topSelparentPair, resultSchema, true);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Plan after fixTopOBSchema\n " + RelOptUtil.toString(newTopNode));
    }

    topSelparentPair = HiveCalciteUtil.getTopLevelSelect(newTopNode);
    newTopNode = renameTopLevelSelectInResultSchema(newTopNode, topSelparentPair, resultSchema);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Final plan after modifier\n " + RelOptUtil.toString(newTopNode));
    }
    return newTopNode;
  }

  private static String getTblAlias(RelNode rel) {

    if (null == rel) {
      return null;
    }
    if (rel instanceof HiveTableScan) {
      return ((HiveTableScan)rel).getTableAlias();
    }
    if (rel instanceof DruidQuery) {
      DruidQuery dq = (DruidQuery) rel;
      return ((HiveTableScan) dq.getTableScan()).getTableAlias();
    }
    if (rel instanceof HiveJdbcConverter) {
      HiveJdbcConverter conv = (HiveJdbcConverter) rel;
      return conv.getTableScan().getHiveTableScan().getTableAlias();
    }
    if (rel instanceof Project) {
      return null;
    }
    if (rel.getInputs().size() == 1) {
      return getTblAlias(rel.getInput(0));
    }
    return null;
  }

  private static void convertOpTree(RelNode rel, RelNode parent) {

    if (rel instanceof HepRelVertex) {
      throw new RuntimeException("Found HepRelVertex");
    } else if (rel instanceof Join) {
      if (!validJoinParent(rel, parent)) {
        introduceDerivedTable(rel, parent);
      }
      String leftChild = getTblAlias(((Join)rel).getLeft());
      if (null != leftChild && leftChild.equalsIgnoreCase(getTblAlias(((Join)rel).getRight()))) {
        // introduce derived table above one child, if this is self-join
        // since user provided aliases are lost at this point.
        introduceDerivedTable(((Join)rel).getLeft(), rel);
      }
    } else if (rel instanceof MultiJoin) {
      throw new RuntimeException("Found MultiJoin");
    } else if (rel instanceof RelSubset) {
      throw new RuntimeException("Found RelSubset");
    } else if (rel instanceof SetOp) {
      // TODO: Handle more than 2 inputs for setop
      if (!validSetopParent(rel, parent))
        introduceDerivedTable(rel, parent);

      SetOp setop = (SetOp) rel;
      for (RelNode inputRel : setop.getInputs()) {
        if (!validSetopChild(inputRel)) {
          introduceDerivedTable(inputRel, setop);
        }
      }
    } else if (rel instanceof SingleRel) {
      if (rel instanceof HiveJdbcConverter) {
        introduceDerivedTable(rel, parent);
      } else if (rel instanceof Filter) {
        if (!validFilterParent(rel, parent)) {
          introduceDerivedTable(rel, parent);
        }
      } else if (rel instanceof HiveSortLimit) {
        if (!validSortParent(rel, parent)) {
          introduceDerivedTable(rel, parent);
        }
        if (!validSortChild((HiveSortLimit) rel)) {
          introduceDerivedTable(((HiveSortLimit) rel).getInput(), rel);
        }
      } else if (rel instanceof HiveAggregate) {
        RelNode newParent = parent;
        if (!validGBParent(rel, parent)) {
          newParent = introduceDerivedTable(rel, parent);
        }
        // check if groupby is empty and there is no other cols in aggr
        // this should only happen when newParent is constant.
        if (isEmptyGrpAggr(rel)) {
          replaceEmptyGroupAggr(rel, newParent);
        }
      }
    }

    List<RelNode> childNodes = rel.getInputs();
    if (childNodes != null) {
      for (RelNode r : childNodes) {
        convertOpTree(r, rel);
      }
    }
  }

  public static RelNode renameTopLevelSelectInResultSchema(final RelNode rootRel,
      Pair<RelNode, RelNode> topSelparentPair, List<FieldSchema> resultSchema)
      throws CalciteSemanticException {
    RelNode parentOforiginalProjRel = topSelparentPair.getKey();
    HiveProject originalProjRel = (HiveProject) topSelparentPair.getValue();

    // Assumption: top portion of tree could only be
    // (limit)?(OB)?(Project)....
    List<RexNode> rootChildExps = originalProjRel.getChildExps();
    if (resultSchema.size() != rootChildExps.size()) {
      // Safeguard against potential issues in CBO RowResolver construction. Disable CBO for now.
      LOG.error(PlanModifierUtil.generateInvalidSchemaMessage(originalProjRel, resultSchema, 0));
      throw new CalciteSemanticException("Result Schema didn't match Optimized Op Tree Schema");
    }

    List<String> newSelAliases = new ArrayList<String>();
    String colAlias;
    for (int i = 0; i < rootChildExps.size(); i++) {
      colAlias = resultSchema.get(i).getName();
      colAlias = getNewColAlias(newSelAliases, colAlias);
      newSelAliases.add(colAlias);
    }

    HiveProject replacementProjectRel = HiveProject.create(originalProjRel.getInput(),
        originalProjRel.getChildExps(), newSelAliases);

    if (rootRel == originalProjRel) {
      return replacementProjectRel;
    } else {
      parentOforiginalProjRel.replaceInput(0, replacementProjectRel);
      return rootRel;
    }
  }

  private static String getNewColAlias(List<String> newSelAliases, String colAlias) {
    int index = 1;
    String newColAlias = colAlias;
    while (newSelAliases.contains(newColAlias)) {
      //This means that the derived colAlias collides with existing ones.
      newColAlias = colAlias + "_" + (index++);
    }
    return newColAlias;
  }

  private static RelNode introduceDerivedTable(final RelNode rel) {
    List<RexNode> projectList = HiveCalciteUtil.getProjsFromBelowAsInputRef(rel);

    HiveProject select = HiveProject.create(rel.getCluster(), rel, projectList,
        rel.getRowType(), rel.getCollationList());

    return select;
  }

  private static RelNode introduceDerivedTable(final RelNode rel, RelNode parent) {
    int i = 0;
    int pos = -1;
    List<RelNode> childList = parent.getInputs();

    for (RelNode child : childList) {
      if (child == rel) {
        pos = i;
        break;
      }
      i++;
    }

    if (pos == -1) {
      throw new RuntimeException("Couldn't find child node in parent's inputs");
    }

    RelNode select = introduceDerivedTable(rel);

    parent.replaceInput(pos, select);

    return select;
  }

  private static boolean validJoinParent(RelNode joinNode, RelNode parent) {
    boolean validParent = true;

    if (parent instanceof Join) {
      // In Hive AST, right child of join cannot be another join,
      // thus we need to introduce a project on top of it.
      // But we only need the additional project if the left child
      // is another join too; if it is not, ASTConverter will swap
      // the join inputs, leaving the join operator on the left.
      // we also do it if parent is HiveSemiJoin since ASTConverter won't
      // swap inputs then
      // This will help triggering multijoin recognition methods that
      // are embedded in SemanticAnalyzer.
      if (((Join) parent).getRight() == joinNode &&
            (((Join) parent).getLeft() instanceof Join || parent instanceof HiveSemiJoin) ) {
        validParent = false;
      }
    } else if (parent instanceof SetOp) {
      validParent = false;
    }

    return validParent;
  }

  private static boolean validFilterParent(RelNode filterNode, RelNode parent) {
    boolean validParent = true;

    // TODO: Verify GB having is not a separate filter (if so we shouldn't
    // introduce derived table)
    if (parent instanceof Filter || parent instanceof Join || parent instanceof SetOp ||
       (parent instanceof Aggregate && filterNode.getInputs().get(0) instanceof Aggregate)) {
      validParent = false;
    }

    return validParent;
  }

  private static boolean validGBParent(RelNode gbNode, RelNode parent) {
    boolean validParent = true;

    // TOODO: Verify GB having is not a seperate filter (if so we shouldn't
    // introduce derived table)
    if (parent instanceof Join || parent instanceof SetOp
        || parent instanceof Aggregate
        || (parent instanceof Filter && ((Aggregate) gbNode).getGroupSet().isEmpty())) {
      validParent = false;
    }

    if (parent instanceof Project) {
      for (RexNode child : parent.getChildExps()) {
        if (child instanceof RexOver || child instanceof RexWinAggCall) {
          // Hive can't handle select rank() over(order by sum(c1)/sum(c2)) from t1 group by c3
          // but can handle    select rank() over (order by c4) from
          // (select sum(c1)/sum(c2)  as c4 from t1 group by c3) t2;
          // so introduce a project on top of this gby.
          return false;
        }
      }
    }

    return validParent;
  }

  private static boolean validSortParent(RelNode sortNode, RelNode parent) {
    boolean validParent = true;

    if (parent != null && !(parent instanceof Project) &&
        !(HiveCalciteUtil.pureLimitRelNode(parent) && HiveCalciteUtil.pureOrderRelNode(sortNode))) {
      validParent = false;
    }

    return validParent;
  }

  private static boolean validSortChild(HiveSortLimit sortNode) {
    boolean validChild = true;
    RelNode child = sortNode.getInput();

    if (!(child instanceof Project) &&
        !(HiveCalciteUtil.pureLimitRelNode(sortNode) && HiveCalciteUtil.pureOrderRelNode(child))) {
      validChild = false;
    }

    return validChild;
  }

  private static boolean validSetopParent(RelNode setop, RelNode parent) {
    boolean validChild = true;

    if (parent != null && !(parent instanceof Project)) {
      validChild = false;
    }

    return validChild;
  }

  private static boolean validSetopChild(RelNode setopChild) {
    boolean validChild = true;

    if (!(setopChild instanceof Project)) {
      validChild = false;
    }

    return validChild;
  }

  private static boolean isEmptyGrpAggr(RelNode gbNode) {
    // Verify if both groupset and aggrfunction are empty)
    Aggregate aggrnode = (Aggregate) gbNode;
    if (aggrnode.getGroupSet().isEmpty() && aggrnode.getAggCallList().isEmpty()) {
      return true;
    }
    return false;
  }

  private static void replaceEmptyGroupAggr(final RelNode rel, RelNode parent) {
    // If this function is called, the parent should only include constant
    List<RexNode> exps = parent.getChildExps();
    for (RexNode rexNode : exps) {
      if (!rexNode.accept(new HiveCalciteUtil.ConstantFinder())) {
        throw new RuntimeException("We expect " + parent.toString()
            + " to contain only constants. However, " + rexNode.toString() + " is "
            + rexNode.getKind());
      }
    }
    HiveAggregate oldAggRel = (HiveAggregate) rel;
    RelDataTypeFactory typeFactory = oldAggRel.getCluster().getTypeFactory();
    RelDataType longType = TypeConverter.convert(TypeInfoFactory.longTypeInfo, typeFactory);
    RelDataType intType = TypeConverter.convert(TypeInfoFactory.intTypeInfo, typeFactory);
    // Create the dummy aggregation.
    SqlAggFunction countFn = SqlFunctionConverter.getCalciteAggFn("count", false,
        ImmutableList.of(intType), longType);
    // TODO: Using 0 might be wrong; might need to walk down to find the
    // proper index of a dummy.
    List<Integer> argList = ImmutableList.of(0);
    AggregateCall dummyCall = new AggregateCall(countFn, false, argList, longType, null);
    Aggregate newAggRel = oldAggRel.copy(oldAggRel.getTraitSet(), oldAggRel.getInput(),
        oldAggRel.indicator, oldAggRel.getGroupSet(), oldAggRel.getGroupSets(),
        ImmutableList.of(dummyCall));
    RelNode select = introduceDerivedTable(newAggRel);
    parent.replaceInput(0, select);
  }
}
