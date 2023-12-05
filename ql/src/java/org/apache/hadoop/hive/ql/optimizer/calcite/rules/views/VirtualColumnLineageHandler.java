package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;

import java.util.Set;

public class VirtualColumnLineageHandler {
  private final RelMetadataQuery metadataQuery;

  public VirtualColumnLineageHandler(RelMetadataQuery metadataQuery) {
    this.metadataQuery = metadataQuery;
  }

  public RelOptTable getVirtualColumnLineage(RelNode startNode, RexNode rexNode,
                                             VirtualColumn virtualColumn) {
    if (!(rexNode instanceof RexInputRef)) {
      return null;
    }

    RexInputRef rexInputRef = (RexInputRef) rexNode;
    Set<RexNode> rexNodeSet = metadataQuery.getExpressionLineage(startNode, rexInputRef);
    if (rexNodeSet == null || rexNodeSet.size() != 1) {
      return null;
    }

    RexNode resultRexNode = rexNodeSet.iterator().next();
    if (!(resultRexNode instanceof RexTableInputRef)) {
      return null;
    }
    RexTableInputRef tableInputRef = (RexTableInputRef) resultRexNode;

    RelOptTable relOptTable = tableInputRef.getTableRef().getTable();
    RelDataTypeField virtualColumnField = relOptTable.getRowType().getField(virtualColumn.getName(), false, false);
    if (virtualColumnField == null) {
      return null;
    }

    return virtualColumnField.getIndex() == tableInputRef.getIndex() ? relOptTable : null;
  }
}
