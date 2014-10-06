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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.optimizer.optiq.HiveOptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqSemanticException;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveAggregateRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveSortRel;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.EmptyRel;
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.OneRowRelBase;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SetOpRel;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.rel.SortRel;
import org.eigenbase.rel.rules.MultiJoinRel;
import org.eigenbase.relopt.hep.HepRelVertex;
import org.eigenbase.relopt.volcano.RelSubset;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.Pair;

import com.google.common.collect.ImmutableMap;

public class PlanModifierForASTConv {

  public static RelNode convertOpTree(RelNode rel, List<FieldSchema> resultSchema)
      throws OptiqSemanticException {
    RelNode newTopNode = rel;

    if (!(newTopNode instanceof ProjectRelBase) && !(newTopNode instanceof SortRel)) {
      newTopNode = introduceDerivedTable(newTopNode);
    }

    convertOpTree(newTopNode, (RelNode) null);

    Pair<RelNode, RelNode> topSelparentPair = HiveOptiqUtil.getTopLevelSelect(newTopNode);
    fixTopOBSchema(newTopNode, topSelparentPair, resultSchema);
    topSelparentPair = HiveOptiqUtil.getTopLevelSelect(newTopNode);
    newTopNode = renameTopLevelSelectInResultSchema(newTopNode, topSelparentPair, resultSchema);

    return newTopNode;
  }

  private static void convertOpTree(RelNode rel, RelNode parent) {

    if (rel instanceof EmptyRel) {
      throw new RuntimeException("Found Empty Rel");
    } else if (rel instanceof HepRelVertex) {
      throw new RuntimeException("Found HepRelVertex");
    } else if (rel instanceof JoinRelBase) {
      if (!validJoinParent(rel, parent)) {
        introduceDerivedTable(rel, parent);
      }
    } else if (rel instanceof MultiJoinRel) {
      throw new RuntimeException("Found MultiJoinRel");
    } else if (rel instanceof OneRowRelBase) {
      throw new RuntimeException("Found OneRowRelBase");
    } else if (rel instanceof RelSubset) {
      throw new RuntimeException("Found RelSubset");
    } else if (rel instanceof SetOpRel) {
      // TODO: Handle more than 2 inputs for setop
      if (!validSetopParent(rel, parent))
        introduceDerivedTable(rel, parent);

      SetOpRel setopRel = (SetOpRel) rel;
      for (RelNode inputRel : setopRel.getInputs()) {
        if (!validSetopChild(inputRel)) {
          introduceDerivedTable(inputRel, setopRel);
        }
      }
    } else if (rel instanceof SingleRel) {
      if (rel instanceof FilterRelBase) {
        if (!validFilterParent(rel, parent)) {
          introduceDerivedTable(rel, parent);
        }
      } else if (rel instanceof HiveSortRel) {
        if (!validSortParent(rel, parent)) {
          introduceDerivedTable(rel, parent);
        }
        if (!validSortChild((HiveSortRel) rel)) {
          introduceDerivedTable(((HiveSortRel) rel).getChild(), rel);
        }
      } else if (rel instanceof HiveAggregateRel) {
        if (!validGBParent(rel, parent)) {
          introduceDerivedTable(rel, parent);
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
      throws OptiqSemanticException {
    if (topSelparentPair.getKey() instanceof SortRel
        && HiveOptiqUtil.orderRelNode(topSelparentPair.getKey())) {
      HiveSortRel obRel = (HiveSortRel) topSelparentPair.getKey();
      ProjectRelBase obChild = (ProjectRelBase) topSelparentPair.getValue();

      if (obChild.getRowType().getFieldCount() > resultSchema.size()) {
        RelDataType rt = obChild.getRowType();
        Set<Integer> collationInputRefs = new HashSet(RelCollationImpl.ordinals(obRel
            .getCollation()));
        ImmutableMap.Builder<Integer, RexNode> inputRefToCallMapBldr = ImmutableMap.builder();
        for (int i = resultSchema.size(); i < rt.getFieldCount(); i++) {
          if (collationInputRefs.contains(i)) {
            inputRefToCallMapBldr.put(i, obChild.getChildExps().get(i));
          }
        }

        ImmutableMap<Integer, RexNode> inputRefToCallMap = inputRefToCallMapBldr.build();
        if ((obChild.getRowType().getFieldCount() - inputRefToCallMap.size()) == resultSchema
            .size()) {
          HiveProjectRel replacementProjectRel = HiveProjectRel.create(obChild.getChild(), obChild
              .getChildExps().subList(0, resultSchema.size()), obChild.getRowType().getFieldNames()
              .subList(0, resultSchema.size()));
          obRel.replaceInput(0, replacementProjectRel);
          obRel.setInputRefToCallMap(inputRefToCallMap);
        } else {
          throw new OptiqSemanticException(
              "Result Schema didn't match Optiq Optimized Op Tree Schema");
        }
      }
    }
  }

  private static RelNode renameTopLevelSelectInResultSchema(final RelNode rootRel,
      Pair<RelNode, RelNode> topSelparentPair, List<FieldSchema> resultSchema)
      throws OptiqSemanticException {
    RelNode parentOforiginalProjRel = topSelparentPair.getKey();
    HiveProjectRel originalProjRel = (HiveProjectRel) topSelparentPair.getValue();

    // Assumption: top portion of tree could only be
    // (limit)?(OB)?(ProjectRelBase)....
    List<RexNode> rootChildExps = originalProjRel.getChildExps();
    if (resultSchema.size() != rootChildExps.size()) {
      // this is a bug in Hive where for queries like select key,value,value
      // convertRowSchemaToResultSetSchema() only returns schema containing
      // key,value
      // Underlying issue is much deeper because it seems like RowResolver
      // itself doesnt have
      // those mappings. see limit_pushdown.q & limit_pushdown_negative.q
      // Till Hive issue is fixed, disable CBO for such queries.
      throw new OptiqSemanticException("Result Schema didn't match Optiq Optimized Op Tree Schema");
    }

    List<String> newSelAliases = new ArrayList<String>();
    String colAlias;
    for (int i = 0; i < rootChildExps.size(); i++) {
      colAlias = resultSchema.get(i).getName();
      if (colAlias.startsWith("_"))
        colAlias = colAlias.substring(1);
      newSelAliases.add(colAlias);
    }

    HiveProjectRel replacementProjectRel = HiveProjectRel.create(originalProjRel.getChild(),
        originalProjRel.getChildExps(), newSelAliases);

    if (rootRel == originalProjRel) {
      return replacementProjectRel;
    } else {
      parentOforiginalProjRel.replaceInput(0, replacementProjectRel);
      return rootRel;
    }
  }

  private static RelNode introduceDerivedTable(final RelNode rel) {
    List<RexNode> projectList = HiveOptiqUtil.getProjsFromBelowAsInputRef(rel);

    HiveProjectRel select = HiveProjectRel.create(rel.getCluster(), rel, projectList,
        rel.getRowType(), rel.getCollationList());

    return select;
  }

  private static void introduceDerivedTable(final RelNode rel, RelNode parent) {
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
  }

  private static boolean validJoinParent(RelNode joinNode, RelNode parent) {
    boolean validParent = true;

    if (parent instanceof JoinRelBase) {
      if (((JoinRelBase) parent).getRight() == joinNode) {
        validParent = false;
      }
    } else if (parent instanceof SetOpRel) {
      validParent = false;
    }

    return validParent;
  }

  private static boolean validFilterParent(RelNode filterNode, RelNode parent) {
    boolean validParent = true;

    // TOODO: Verify GB having is not a seperate filter (if so we shouldn't
    // introduce derived table)
    if (parent instanceof FilterRelBase || parent instanceof JoinRelBase
        || parent instanceof SetOpRel) {
      validParent = false;
    }

    return validParent;
  }

  private static boolean validGBParent(RelNode gbNode, RelNode parent) {
    boolean validParent = true;

    // TOODO: Verify GB having is not a seperate filter (if so we shouldn't
    // introduce derived table)
    if (parent instanceof JoinRelBase || parent instanceof SetOpRel
        || parent instanceof AggregateRelBase
        || (parent instanceof FilterRelBase && ((AggregateRelBase) gbNode).getGroupSet().isEmpty())) {
      validParent = false;
    }

    return validParent;
  }

  private static boolean validSortParent(RelNode sortNode, RelNode parent) {
    boolean validParent = true;

    if (parent != null && !(parent instanceof ProjectRelBase)
        && !((parent instanceof SortRel) || HiveOptiqUtil.orderRelNode(parent)))
      validParent = false;

    return validParent;
  }

  private static boolean validSortChild(HiveSortRel sortNode) {
    boolean validChild = true;
    RelNode child = sortNode.getChild();

    if (!(HiveOptiqUtil.limitRelNode(sortNode) && HiveOptiqUtil.orderRelNode(child))
        && !(child instanceof ProjectRelBase)) {
      validChild = false;
    }

    return validChild;
  }

  private static boolean validSetopParent(RelNode setop, RelNode parent) {
    boolean validChild = true;

    if (parent != null && !(parent instanceof ProjectRelBase)) {
      validChild = false;
    }

    return validChild;
  }

  private static boolean validSetopChild(RelNode setopChild) {
    boolean validChild = true;

    if (!(setopChild instanceof ProjectRelBase)) {
      validChild = false;
    }

    return validChild;
  }
}
