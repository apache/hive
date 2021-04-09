package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.ImportTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;

public class DelayExecUtil {
  private final boolean inReplScope;
  private final boolean replace;
  private final Long writeId;
  private final int stmtId;
  private final Hive hive;
  private final Context ctx;
  private final ImportTableDesc tblDesc;
  private Path destPath = null, loadPath = null;
  private LoadTableDesc.LoadFileType lft;
  private boolean isSkipTrash = false;
  private boolean needRecycle = false;
  private final Path tgtPath;

  public DelayExecUtil(boolean replace, Long writeId, int stmtId, Hive hive, Context ctx, ImportTableDesc tblDesc,
      boolean inReplScope) {
    this.replace = replace;
    this.writeId = writeId;
    this.stmtId = stmtId;
    this.hive = hive;
    this.ctx = ctx;
    this.tblDesc = tblDesc;
    this.inReplScope = inReplScope;
    tgtPath = tblDesc == null ? null : new Path(tblDesc.getLocation());
  }

  public Table getTableIfExists() throws HiveException {
    Table table = ImportSemanticAnalyzer.tableIfExists(tblDesc, hive);
    if (table == null) {
      table = ImportSemanticAnalyzer.createNewTableMetadataObject(tblDesc, true);
    }

    return table;
  }

  public void calculateValues(Table table) throws HiveException {
    assert table != null;
    assert table.getParameters() != null;

    if (inReplScope) {
      isSkipTrash = MetaStoreUtils.isSkipTrash(table.getParameters());
      if (table.isTemporary()) {
        needRecycle = false;
      } else {
        org.apache.hadoop.hive.metastore.api.Database db = hive.getDatabase(table.getDbName());
        needRecycle = db != null && ReplChangeManager.shouldEnableCm(db, table.getTTable());
      }
    }

    if (AcidUtils.isTransactionalTable(table)) {
      String mmSubdir = replace ? AcidUtils.baseDir(writeId)
          : AcidUtils.deltaSubdir(writeId, writeId, stmtId);
      destPath = new Path(tgtPath, mmSubdir);
      loadPath = tgtPath;
      lft = LoadTableDesc.LoadFileType.KEEP_EXISTING;
    } else {
      destPath = loadPath = ctx.getExternalTmpPath(tgtPath);
      lft = replace ? LoadTableDesc.LoadFileType.REPLACE_ALL :
          LoadTableDesc.LoadFileType.OVERWRITE_EXISTING;
    }
  }

  public Long getWriteId() {
    return writeId;
  }

  public int getStmtId() {
    return stmtId;
  }

  public boolean isReplace() {
    return replace;
  }

  public Path getTgtPath() {
    return tgtPath;
  }

  public boolean isInReplScope() {
    return inReplScope;
  }

  public Path getDestPath() {
    return destPath;
  }

  public Path getLoadPath() {
    return loadPath;
  }

  public LoadTableDesc.LoadFileType getLft() {
    return lft;
  }

  public boolean isSkipTrash() {
    return isSkipTrash;
  }

  public boolean isNeedRecycle() {
    return needRecycle;
  }
}
