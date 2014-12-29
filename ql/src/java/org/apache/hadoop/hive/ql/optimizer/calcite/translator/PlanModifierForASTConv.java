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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSort;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class PlanModifierForASTConv {
  private static final Log LOG = LogFactory.getLog(PlanModifierForASTConv.class);

  public static RelNode convertOpTree(RelNode rel, List<FieldSchema> resultSchema)
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

    Pair<RelNode, RelNode> topSelparentPair = HiveCalciteUtil.getTopLevelSelect(newTopNode);
    fixTopOBSchema(newTopNode, topSelparentPair, resultSchema);
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

  private static void convertOpTree(RelNode rel, RelNode parent) {

    if (rel instanceof HepRelVertex) {
      throw new RuntimeException("Found HepRelVertex");
    } else if (rel instanceof Join) {
      if (!validJoinParent(rel, parent)) {
        introduceDerivedTable(rel, parent);
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
      if (rel instanceof Filter) {
        if (!validFilterParent(rel, parent)) {
          introduceDerivedTable(rel, parent);
        }
      } else if (rel instanceof HiveSort) {
        if (!validSortParent(rel, parent)) {
          introduceDerivedTable(rel, parent);
        }
        if (!validSortChild((HiveSort) rel)) {
          introduceDerivedTable(((HiveSort) rel).getInput(), rel);
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

  private static void fixTopOBSchema(final RelNode rootRel,
      Pair<RelNode, RelNode> topSelparentPair, List<FieldSchema> resultSchema)
      throws CalciteSemanticException {
    if (!(topSelparentPair.getKey() instanceof Sort)
        || !HiveCalciteUtil.orderRelNode(topSelparentPair.getKey())) {
      return;
    }
    HiveSort obRel = (HiveSort) topSelparentPair.getKey();
    Project obChild = (Project) topSelparentPair.getValue();
    if (obChild.getRowType().getFieldCount() <= resultSchema.size()) {
      return;
    }

    RelDataType rt = obChild.getRowType();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    Set<Integer> collationInputRefs = new HashSet(
        RelCollationImpl.ordinals(obRel.getCollation()));
    ImmutableMap.Builder<Integer, RexNode> inputRefToCallMapBldr = ImmutableMap.builder();
    for (int i = resultSchema.size(); i < rt.getFieldCount(); i++) {
      if (collationInputRefs.contains(i)) {
        inputRefToCallMapBldr.put(i, obChild.getChildExps().get(i));
      }
    }
    ImmutableMap<Integer, RexNode> inputRefToCallMap = inputRefToCallMapBldr.build();

    if ((obChild.getRowType().getFieldCount() - inputRefToCallMap.size()) != resultSchema.size()) {
      LOG.error(generateInvalidSchemaMessage(obChild, resultSchema, inputRefToCallMap.size()));
      throw new CalciteSemanticException("Result Schema didn't match Optimized Op Tree Schema");
    }
    // This removes order-by only expressions from the projections.
    HiveProject replacementProjectRel = HiveProject.create(obChild.getInput(), obChild
        .getChildExps().subList(0, resultSchema.size()), obChild.getRowType().getFieldNames()
        .subList(0, resultSchema.size()));
    obRel.replaceInput(0, replacementProjectRel);
    obRel.setInputRefToCallMap(inputRefToCallMap);
  }

  private static String generateInvalidSchemaMessage(Project topLevelProj,
      List<FieldSchema> resultSchema, int fieldsForOB) {
    String errorDesc = "Result Schema didn't match Calcite Optimized Op Tree; schema: ";
    for (FieldSchema fs : resultSchema) {
      errorDesc += "[" + fs.getName() + ":" + fs.getType() + "], ";
    }
    errorDesc += " projection fields: ";
    for (RexNode exp : topLevelProj.getChildExps()) {
      errorDesc += "[" + exp.toString() + ":" + exp.getType() + "], ";
    }
    if (fieldsForOB != 0) {
      errorDesc += fieldsForOB + " fields removed due to ORDER BY  ";
    }
    return errorDesc.substring(0, errorDesc.length() - 2);
  }

  private static RelNode renameTopLevelSelectInResultSchema(final RelNode rootRel,
      Pair<RelNode, RelNode> topSelparentPair, List<FieldSchema> resultSchema)
      throws CalciteSemanticException {
    RelNode parentOforiginalProjRel = topSelparentPair.getKey();
    HiveProject originalProjRel = (HiveProject) topSelparentPair.getValue();

    // Assumption: top portion of tree could only be
    // (limit)?(OB)?(Project)....
    List<RexNode> rootChildExps = originalProjRel.getChildExps();
    if (resultSchema.size() != rootChildExps.size()) {
      // Safeguard against potential issues in CBO RowResolver construction. Disable CBO for now.
      LOG.error(generateInvalidSchemaMessage(originalProjRel, resultSchema, 0));
      throw new CalciteSemanticException("Result Schema didn't match Optimized Op Tree Schema");
    }

    List<String> newSelAliases = new ArrayList<String>();
    String colAlias;
    for (int i = 0; i < rootChildExps.size(); i++) {
      colAlias = resultSchema.get(i).getName();
      if (colAlias.startsWith("_")) {
        colAlias = colAlias.substring(1);
      }
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
      if (((Join) parent).getRight() == joinNode) {
        validParent = false;
      }
    } else if (parent instanceof SetOp) {
      validParent = false;
    }

    return validParent;
  }

  private static boolean validFilterParent(RelNode filterNode, RelNode parent) {
    boolean validParent = true;

    // TOODO: Verify GB having is not a seperate filter (if so we shouldn't
    // introduce derived table)
    if (parent instanceof Filter || parent instanceof Join
        || parent instanceof SetOp) {
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

    return validParent;
  }

  private static boolean validSortParent(RelNode sortNode, RelNode parent) {
    boolean validParent = true;

    if (parent != null && !(parent instanceof Project)
        && !((parent instanceof Sort) || HiveCalciteUtil.orderRelNode(parent)))
      validParent = false;

    return validParent;
  }

  private static boolean validSortChild(HiveSort sortNode) {
    boolean validChild = true;
    RelNode child = sortNode.getInput();

    if (!(HiveCalciteUtil.limitRelNode(sortNode) && HiveCalciteUtil.orderRelNode(child))
        && !(child instanceof Project)) {
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
      if (rexNode.getKind() != SqlKind.LITERAL) {
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
    SqlAggFunction countFn = (SqlAggFunction) SqlFunctionConverter.getCalciteAggFn("count",
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
