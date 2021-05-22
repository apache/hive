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

package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ImportSemanticAnalyzer;

import java.util.Collections;
import java.util.TreeMap;

public class TablePathResolver implements PathResolver {
  private final boolean inReplScope;
  private final boolean replace;
  private final Long writeId;
  private final int stmtId;
  private final Hive hive;
  private final Context ctx;
  private final ImportTableDesc tblDesc;
  private final Path tgtPath;
  private Path destPath = null, loadPath = null;
  private LoadTableDesc.LoadFileType loadFileType;
  private boolean isSkipTrash;
  private boolean needRecycle;
  private boolean isCalculated = false;
  private Table table;

  public TablePathResolver(boolean replace, Path tgtPath, Long writeId, int stmtId, Hive hive, Context ctx,
      ImportTableDesc tblDesc, boolean inReplScope) {
    this.replace = replace;
    this.writeId = writeId;
    this.stmtId = stmtId;
    this.hive = hive;
    this.ctx = ctx;
    this.tblDesc = tblDesc;
    this.inReplScope = inReplScope;
    this.tgtPath = tgtPath;
  }

  private void calculateValues() throws HiveException {
    if (!isCalculated) {
      table = ImportSemanticAnalyzer.tableIfExists(tblDesc, hive);
      if (table == null) {
        table = ImportSemanticAnalyzer.createNewTableMetadataObject(tblDesc, true);
      }

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
        String mmSubdir = replace ? AcidUtils.baseDir(writeId) : AcidUtils.deltaSubdir(writeId, writeId, stmtId);
        destPath = new Path(tgtPath, mmSubdir);
        /*
          CopyTask will copy files from the 'archive' to a delta_x_x in the table/partition
          directory, i.e. the final destination for these files.  This has to be a copy to preserve
          the archive.  MoveTask is optimized to do a 'rename' if files are on the same FileSystem.
          So setting 'loadPath' this way will make
          {@link Hive#loadTable(Path, String, LoadTableDesc.LoadFileType, boolean, boolean, boolean,
          boolean, Long, int)}
          skip the unnecessary file (rename) operation but it will perform other things.
         */
        loadPath = tgtPath;
        loadFileType = LoadTableDesc.LoadFileType.KEEP_EXISTING;
      } else {
        destPath = loadPath = ctx.getExternalTmpPath(tgtPath);
        loadFileType = replace ? LoadTableDesc.LoadFileType.REPLACE_ALL : LoadTableDesc.LoadFileType.OVERWRITE_EXISTING;
      }

      isCalculated = true;
    }
  }

  public boolean isCalculated() {
    return isCalculated;
  }

  @Override public void setupWork(CopyWork copyWork) throws HiveException {
    calculateValues();
    copyWork.setToPath(new Path[] { destPath });
  }

  @Override public void setupWork(ReplCopyWork replCopyWork) throws HiveException {
    calculateValues();
    replCopyWork.setToPath(new Path[] { destPath });
    if (replace) {
      replCopyWork.setDeleteDestIfExist(true);
      replCopyWork.setAutoPurge(isSkipTrash);
      replCopyWork.setNeedRecycle(needRecycle);
    }
  }

  @Override public void setupWork(MoveWork moveWork) throws HiveException {
    calculateValues();

    if (inReplScope && AcidUtils.isTransactionalTable(table)) {
      LoadMultiFilesDesc loadFilesWork = new LoadMultiFilesDesc(
          Collections.singletonList(destPath),
          Collections.singletonList(tgtPath),
          true, null, null);
      moveWork.setMultiFilesDesc(loadFilesWork);
      moveWork.setNeedCleanTarget(replace);
    } else {
      LoadTableDesc loadTableWork = new LoadTableDesc(
          loadPath, Utilities.getTableDesc(table), new TreeMap<>(), loadFileType, writeId);
      loadTableWork.setStmtId(stmtId);
      moveWork.setLoadTableWork(loadTableWork);
    }
  }
}
