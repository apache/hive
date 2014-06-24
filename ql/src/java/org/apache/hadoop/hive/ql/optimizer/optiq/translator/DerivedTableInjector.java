package org.apache.hadoop.hive.ql.optimizer.optiq.translator;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveAggregateRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveSortRel;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.EmptyRel;
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.OneRowRelBase;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SetOpRel;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.rel.TableFunctionRelBase;
import org.eigenbase.rel.ValuesRelBase;
import org.eigenbase.rel.rules.MultiJoinRel;
import org.eigenbase.relopt.hep.HepRelVertex;
import org.eigenbase.relopt.volcano.RelSubset;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class DerivedTableInjector {

  public static RelNode convertOpTree(RelNode rel, List<FieldSchema> resultSchema) {
    // Disable introducing top level select since Hive seems to have bugs with
    // OB, Limit in sub query.
    // RelNode newTopSelect = introduceTopLevelSelectInResultSchema(rel,
    // resultSchema);
    RelNode newTopSelect = rel;
    convertOpTree(newTopSelect, (RelNode) null);
    return newTopSelect;
  }

  private static void convertOpTree(RelNode rel, RelNode parent) {

    if (rel instanceof EmptyRel) {
      // TODO: replace with null scan
    } else if (rel instanceof HepRelVertex) {
      // TODO: is this relevant?
    } else if (rel instanceof HiveJoinRel) {
      if (!validJoinParent(rel, parent)) {
        introduceDerivedTable(rel, parent);
      }
    } else if (rel instanceof MultiJoinRel) {

    } else if (rel instanceof OneRowRelBase) {

    } else if (rel instanceof RelSubset) {

    } else if (rel instanceof SetOpRel) {

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
    } else if (rel instanceof TableAccessRelBase) {

    } else if (rel instanceof TableFunctionRelBase) {

    } else if (rel instanceof ValuesRelBase) {

    }

    List<RelNode> childNodes = rel.getInputs();
    if (childNodes != null) {
      for (RelNode r : childNodes) {
        convertOpTree(r, rel);
      }
    }
  }

  private static HiveProjectRel introduceTopLevelSelectInResultSchema(final RelNode rootRel,
      List<FieldSchema> resultSchema) {
    RelNode curNode = rootRel;
    HiveProjectRel rootProjRel = null;
    while (curNode != null) {
      if (curNode instanceof HiveProjectRel) {
        rootProjRel = (HiveProjectRel) curNode;
        break;
      }
      curNode = curNode.getInput(0);
    }

    //Assumption: tree could only be (limit)?(OB)?(ProjectRelBase)....
    List<RexNode> rootChildExps = rootProjRel.getChildExps();
    if (resultSchema.size() != rootChildExps.size()) {
      throw new RuntimeException("Result Schema didn't match Optiq Optimized Op Tree Schema");
    }

    List<RexNode> newSelExps = new ArrayList<RexNode>();
    List<String> newSelAliases = new ArrayList<String>();
    for (int i = 0; i < rootChildExps.size(); i++) {
      newSelExps.add(new RexInputRef(i, rootChildExps.get(i).getType()));
      newSelAliases.add(resultSchema.get(i).getName());
    }

    return HiveProjectRel.create(rootRel, newSelExps, newSelAliases);
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

    List<RexNode> projectList = Lists.transform(rel.getRowType().getFieldList(),
        new Function<RelDataTypeField, RexNode>() {
          public RexNode apply(RelDataTypeField field) {
            return rel.getCluster().getRexBuilder().makeInputRef(field.getType(), field.getIndex());
          }
        });

    HiveProjectRel select = HiveProjectRel.create(rel.getCluster(), rel, projectList,
        rel.getRowType(), rel.getCollationList());
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
        || parent instanceof AggregateRelBase) {
      validParent = false;
    }

    return validParent;
  }

  private static boolean validSortParent(RelNode sortNode, RelNode parent) {
    boolean validParent = true;

    if (parent != null && !(parent instanceof ProjectRelBase)) {
      validParent = false;
    }

    return validParent;
  }

  private static boolean validSortChild(HiveSortRel sortNode) {
    boolean validChild = true;
    RelNode child = sortNode.getChild();

    if (!(child instanceof ProjectRelBase)) {
      validChild = false;
    }

    return validChild;
  }
}
