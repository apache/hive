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
