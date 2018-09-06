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
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * LoadTableDesc.
 *
 */
public class LoadTableDesc extends LoadDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private LoadFileType loadFileType;
  private DynamicPartitionCtx dpCtx;
  private ListBucketingCtx lbCtx;
  private boolean inheritTableSpecs = true; //For partitions, flag controlling whether the current
                                            //table specs are to be used
  private boolean inheritLocation = false; // A silly setting.
  private int stmtId;
  private Long currentWriteId;
  private boolean isInsertOverwrite;

  // TODO: the below seem like they should just be combined into partitionDesc
  private Table mdTable;
  private org.apache.hadoop.hive.ql.plan.TableDesc table;
  private Map<String, String> partitionSpec; // NOTE: this partitionSpec has to be ordered map

  public enum LoadFileType {
    /**
     * This corresponds to INSERT OVERWRITE and REPL LOAD for INSERT OVERWRITE event.
     * Remove all existing data before copy/move
     */
    REPLACE_ALL,
    /**
     * This corresponds to INSERT INTO and LOAD DATA.
     * If any file exist while copy, then just duplicate the file
     */
    KEEP_EXISTING,
    /**
     * This corresponds to REPL LOAD where if we re-apply the same event then need to overwrite
     * the file instead of making a duplicate copy.
     * If any file exist while copy, then just overwrite the file
     */
    OVERWRITE_EXISTING
  }
  public LoadTableDesc(final LoadTableDesc o) {
    super(o.getSourcePath(), o.getWriteType());

    this.loadFileType = o.loadFileType;
    this.dpCtx = o.dpCtx;
    this.lbCtx = o.lbCtx;
    this.inheritTableSpecs = o.inheritTableSpecs;
    this.inheritLocation = o.inheritLocation;
    this.currentWriteId = o.currentWriteId;
    this.table = o.table;
    this.partitionSpec = o.partitionSpec;
  }

  public LoadTableDesc(final Path sourcePath,
      final TableDesc table,
      final Map<String, String> partitionSpec,
      final LoadFileType loadFileType,
      final AcidUtils.Operation writeType, Long currentWriteId) {
    super(sourcePath, writeType);
    if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
      Utilities.FILE_OP_LOGGER.trace("creating part LTD from " + sourcePath + " to "
        + ((table.getProperties() == null) ? "null" : table.getTableName()));
    }
    init(table, partitionSpec, loadFileType, currentWriteId);
  }

  /**
   * For use with non-ACID compliant operations, such as LOAD
   * @param sourcePath
   * @param table
   * @param partitionSpec
   * @param loadFileType
   * @param writeId
   */
  public LoadTableDesc(final Path sourcePath,
                       final TableDesc table,
                       final Map<String, String> partitionSpec,
                       final LoadFileType loadFileType,
                       final Long writeId) {
    this(sourcePath, table, partitionSpec, loadFileType, AcidUtils.Operation.NOT_ACID, writeId);
  }

  public LoadTableDesc(final Path sourcePath,
      final TableDesc table,
      final Map<String, String> partitionSpec,
      final AcidUtils.Operation writeType, Long currentWriteId) {
    this(sourcePath, table, partitionSpec, LoadFileType.REPLACE_ALL,
            writeType, currentWriteId);
  }

  /**
   * For DDL operations that are not ACID compliant.
   * @param sourcePath
   * @param table
   * @param partitionSpec
   */
  public LoadTableDesc(final Path sourcePath,
                       final org.apache.hadoop.hive.ql.plan.TableDesc table,
                       final Map<String, String> partitionSpec) {
    this(sourcePath, table, partitionSpec, LoadFileType.REPLACE_ALL,
      AcidUtils.Operation.NOT_ACID, null);
  }

  public LoadTableDesc(final Path sourcePath,
      final TableDesc table,
      final DynamicPartitionCtx dpCtx,
      final AcidUtils.Operation writeType,
      boolean isReplace, Long writeId) {
    super(sourcePath, writeType);
    if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
      Utilities.FILE_OP_LOGGER.trace("creating LTD from " + sourcePath + " to " + table.getTableName());
    }
    this.dpCtx = dpCtx;
    LoadFileType lft = isReplace ?  LoadFileType.REPLACE_ALL :  LoadFileType.OVERWRITE_EXISTING;
    if (dpCtx != null && dpCtx.getPartSpec() != null && partitionSpec == null) {
      init(table, dpCtx.getPartSpec(), lft, writeId);
    } else {
      init(table, new LinkedHashMap<String, String>(), lft, writeId);
    }
  }

  private void init(
      final org.apache.hadoop.hive.ql.plan.TableDesc table,
      final Map<String, String> partitionSpec,
      final LoadFileType loadFileType,
      Long writeId) {
    this.table = table;
    this.partitionSpec = partitionSpec;
    this.loadFileType = loadFileType;
    this.currentWriteId = writeId;
  }

  @Explain(displayName = "table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public TableDesc getTable() {
    return table;
  }

  public void setTable(final org.apache.hadoop.hive.ql.plan.TableDesc table) {
    this.table = table;
  }

  @Explain(displayName = "partition")
  public Map<String, String> getPartitionSpec() {
    return partitionSpec;
  }

  public void setPartitionSpec(final Map<String, String> partitionSpec) {
    this.partitionSpec = partitionSpec;
  }

  @Explain(displayName = "replace")
  public boolean getReplace() {
    return (loadFileType == LoadFileType.REPLACE_ALL);
  }

  public LoadFileType getLoadFileType() {
    return loadFileType;
  }

  @Explain(displayName = "micromanaged table")
  public Boolean isMmTableExplain() {
    return isMmTable() ? true : null;
  }

  public boolean isMmTable() {
    return AcidUtils.isInsertOnlyTable(table.getProperties());
  }

  public void setLoadFileType(LoadFileType loadFileType) {
    this.loadFileType = loadFileType;
  }

  public DynamicPartitionCtx getDPCtx() {
    return dpCtx;
  }

  public void setDPCtx(final DynamicPartitionCtx dpCtx) {
    this.dpCtx = dpCtx;
  }

  public boolean getInheritTableSpecs() {
    return inheritTableSpecs;
  }

  public boolean getInheritLocation() {
    return inheritLocation;
  }

  public void setInheritTableSpecs(boolean inheritTableSpecs) {
    // Set inheritLocation if this is set to true explicitly.
    // TODO: Who actually needs this? Might just be some be pointless legacy code.
    this.inheritTableSpecs = inheritLocation = inheritTableSpecs;
  }

  public boolean isInsertOverwrite() {
    return this.isInsertOverwrite;
  }

  public void setInsertOverwrite(boolean v) {
   this.isInsertOverwrite = v;
  }

  /**
   * @return the lbCtx
   */
  public ListBucketingCtx getLbCtx() {
    return lbCtx;
  }

  /**
   * @param lbCtx the lbCtx to set
   */
  public void setLbCtx(ListBucketingCtx lbCtx) {
    this.lbCtx = lbCtx;
  }

  public long getWriteId() {
    return currentWriteId == null ? 0 : currentWriteId;
  }

  public int getStmtId() {
    return stmtId;
  }
  //todo: should this not be passed in the c'tor?
  public void setStmtId(int stmtId) {
    this.stmtId = stmtId;
  }

  public Table getMdTable() {
    return mdTable;
  }

  public void setMdTable(Table mdTable) {
    this.mdTable = mdTable;
  }
}
