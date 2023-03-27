package org.apache.hadoop.hive.ql.ddl.view.materialized.alter.rebuild;

import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.ASTNode;

import java.util.List;

import static java.util.Collections.singletonList;

public class FullAcidMaterializedViewASTBuilder extends MaterializedViewASTBuilder {
  @Override
  public List<ASTNode> createDeleteSelectNodes(String tableName) {
    return wrapIntoSelExpr(singletonList(createQualifiedColumnNode(tableName, VirtualColumn.ROWID.getName())));
  }

  @Override
  protected List<ASTNode> createAcidSortNodesInternal(String tableName) {
    return singletonList(createQualifiedColumnNode(tableName, VirtualColumn.ROWID.getName()));
  }
}
