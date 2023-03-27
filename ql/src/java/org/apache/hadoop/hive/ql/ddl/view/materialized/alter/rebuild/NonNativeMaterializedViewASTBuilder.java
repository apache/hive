package org.apache.hadoop.hive.ql.ddl.view.materialized.alter.rebuild;

import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.ASTNode;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

public class NonNativeMaterializedViewASTBuilder extends MaterializedViewASTBuilder {
  private final Table mvTable;

  public NonNativeMaterializedViewASTBuilder(Table mvTable) {
    this.mvTable = mvTable;
  }

  @Override
  public List<ASTNode> createDeleteSelectNodes(String tableName) {
    return wrapIntoSelExpr(mvTable.getStorageHandler().acidSelectColumns(mvTable, Context.Operation.DELETE)
            .stream().map(fieldSchema -> createQualifiedColumnNode(tableName, fieldSchema.getName()))
            .collect(Collectors.toList()));
  }

  @Override
  protected List<ASTNode> createAcidSortNodesInternal(String tableName) {
    return singletonList(createQualifiedColumnNode(tableName, VirtualColumn.ROWID.getName()));
  }
}
