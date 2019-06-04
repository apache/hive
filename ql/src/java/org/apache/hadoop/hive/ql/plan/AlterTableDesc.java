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

import org.apache.hadoop.hive.ql.io.AcidUtils;

import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.ql.ddl.privilege.PrincipalDesc;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * AlterTableDesc.
 *
 */
@Explain(displayName = "Alter Table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableDesc extends DDLDesc implements Serializable, DDLDesc.DDLDescWithWriteId {
  private static final long serialVersionUID = 1L;

  /**
   * alterTableTypes.
   *
   */
  public static enum AlterTableTypes {
    RENAME("rename"), ADD_COLUMNS("add columns"), REPLACE_COLUMNS("replace columns"),
    ADDPROPS("add props"), DROPPROPS("drop props"), SET_SERDE("set serde"), SET_SERDE_PROPS("set serde props"),
    SET_FILE_FORMAT("add fileformat"), CLUSTERED_BY("clustered by"), NOT_SORTED("not sorted"),
    NOT_CLUSTERED("not clustered"),
    RENAME_COLUMN("rename column"), ADDPARTITION("add partition"), TOUCH("touch"), ARCHIVE("archieve"),
    UNARCHIVE("unarchieve"), SET_LOCATION("set location"),
    DROPPARTITION("drop partition"),
    RENAMEPARTITION("rename partition"), // Note: used in RenamePartitionDesc, not here.
    SKEWED_BY("skewed by"), NOT_SKEWED("not skewed"),
    SET_SKEWED_LOCATION("alter skew location"), INTO_BUCKETS("alter bucket number"),
    ALTERPARTITION("alter partition"), // Note: this is never used in AlterTableDesc.
    COMPACT("compact"),
    TRUNCATE("truncate"), MERGEFILES("merge files"), DROP_CONSTRAINT("drop constraint"),
    ADD_CONSTRAINT("add constraint"),
    UPDATE_COLUMNS("update columns"), OWNER("set owner"),
    UPDATESTATS("update stats"); // Note: used in ColumnStatsUpdateWork, not here.
    ;

    private final String name;
    private AlterTableTypes(String name) { this.name = name; }
    public String getName() { return name; }

    public static final List<AlterTableTypes> nonNativeTableAllowedTypes =
        ImmutableList.of(ADDPROPS, DROPPROPS, ADD_COLUMNS);
  }

  public static enum ProtectModeType {
    NO_DROP, OFFLINE, READ_ONLY, NO_DROP_CASCADE
  }

  public static final Set<AlterTableTypes> alterTableTypesWithPartialSpec =
      new HashSet<AlterTableDesc.AlterTableTypes>();

  static {
    alterTableTypesWithPartialSpec.add(AlterTableDesc.AlterTableTypes.ADD_COLUMNS);
    alterTableTypesWithPartialSpec.add(AlterTableDesc.AlterTableTypes.REPLACE_COLUMNS);
    alterTableTypesWithPartialSpec.add(AlterTableDesc.AlterTableTypes.RENAME_COLUMN);
    alterTableTypesWithPartialSpec.add(AlterTableDesc.AlterTableTypes.ADDPROPS);
    alterTableTypesWithPartialSpec.add(AlterTableDesc.AlterTableTypes.DROPPROPS);
    alterTableTypesWithPartialSpec.add(AlterTableDesc.AlterTableTypes.SET_SERDE);
    alterTableTypesWithPartialSpec.add(AlterTableDesc.AlterTableTypes.SET_SERDE_PROPS);
    alterTableTypesWithPartialSpec.add(AlterTableDesc.AlterTableTypes.SET_FILE_FORMAT);
  }

  AlterTableTypes op;
  String oldName;
  String newName;
  Map<String, String> props;

  boolean expectView;
  HashMap<String, String> partSpec;
  boolean protectModeEnable;
  ProtectModeType protectModeType;
  boolean isTurnOffSkewed = false;
  boolean isDropIfExists = false;
  boolean isCascade = false;
  EnvironmentContext environmentContext;
  ReplicationSpec replicationSpec;
  private Long writeId = null;
  PrincipalDesc ownerPrincipal;
  private boolean isExplicitStatsUpdate, isFullAcidConversion;

  public AlterTableDesc() {
  }

  /**
   * @param oldName
   *          old name of the table
   * @param newName
   *          new name of the table
   * @param expectView
   *          Flag to denote if current table can be a view
   * @param replicationSpec
   *          Replication specification with current event ID
   * @throws SemanticException
   */
  public AlterTableDesc(String oldName, String newName, boolean expectView, ReplicationSpec replicationSpec) throws SemanticException {
    op = AlterTableTypes.RENAME;
    setOldName(oldName);
    this.newName = newName;
    this.expectView = expectView;
    this.replicationSpec = replicationSpec;
  }

  /**
   * @param alterType
   *          type of alter op
   * @param replicationSpec
   *          Replication specification with current event ID
   */
  public AlterTableDesc(AlterTableTypes alterType, ReplicationSpec replicationSpec) {
    op = alterType;
    this.replicationSpec = replicationSpec;
  }

  /**
   * @param alterType
   *          type of alter op
   */
  public AlterTableDesc(AlterTableTypes alterType) {
    this(alterType, null, false);
  }

  /**
   * @param alterType
   *          type of alter op
   * @param expectView
   *          Flag to denote if current table can be a view
   * @param partSpec
   *          Partition specifier with map of key and values.
   */
  public AlterTableDesc(AlterTableTypes alterType, HashMap<String, String> partSpec, boolean expectView) {
    op = alterType;
    this.partSpec = partSpec;
    this.expectView = expectView;
  }

  public AlterTableDesc(String tableName, PrincipalDesc ownerPrincipal) {
    op  = AlterTableTypes.OWNER;
    this.oldName = tableName;
    this.ownerPrincipal = ownerPrincipal;
  }

  /**
   * @param ownerPrincipal the owner principal of the table
   */
  public void setOwnerPrincipal(PrincipalDesc ownerPrincipal) {
    this.ownerPrincipal = ownerPrincipal;
  }

  @Explain(displayName="owner")
  public PrincipalDesc getOwnerPrincipal() {
    return this.ownerPrincipal;
  }

  @Explain(displayName = "type", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getAlterTableTypeString() {
    return op.getName();
  }

  /**
   * @return the old name of the table
   */
  @Explain(displayName = "old name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getOldName() {
    return oldName;
  }

  /**
   * @param oldName
   *          the oldName to set
   */
  public void setOldName(String oldName) throws SemanticException {
    // Make sure we qualify the name from the outset so there's no ambiguity.
    this.oldName = String.join(".", Utilities.getDbTableName(oldName));
  }

  /**
   * @return the newName
   */
  @Explain(displayName = "new name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getNewName() {
    return newName;
  }

  /**
   * @param newName
   *          the newName to set
   */
  public void setNewName(String newName) {
    this.newName = newName;
  }

  /**
   * @return the op
   */
  public AlterTableTypes getOp() {
    return op;
  }

  /**
   * @param op
   *          the op to set
   */
  public void setOp(AlterTableTypes op) {
    this.op = op;
  }

  /**
   * @return the props
   */
  @Explain(displayName = "properties")
  public Map<String, String> getProps() {
    return props;
  }

  /**
   * @param props
   *          the props to set
   */
  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  /**
   * @return whether to expect a view being altered
   */
  public boolean getExpectView() {
    return expectView;
  }

  /**
   * @param expectView
   *          set whether to expect a view being altered
   */
  public void setExpectView(boolean expectView) {
    this.expectView = expectView;
  }

  /**
   * @return part specification
   */
  public HashMap<String, String> getPartSpec() {
    return partSpec;
  }

  /**
   * @param partSpec
   */
  public void setPartSpec(HashMap<String, String> partSpec) {
    this.partSpec = partSpec;
  }

  public boolean isProtectModeEnable() {
    return protectModeEnable;
  }

  public void setProtectModeEnable(boolean protectModeEnable) {
    this.protectModeEnable = protectModeEnable;
  }

  public ProtectModeType getProtectModeType() {
    return protectModeType;
  }

  public void setProtectModeType(ProtectModeType protectModeType) {
    this.protectModeType = protectModeType;
  }

  /**
   * @return the turnOffSkewed
   */
  public boolean isTurnOffSkewed() {
    return isTurnOffSkewed;
  }

  /**
   * @param turnOffSkewed the turnOffSkewed to set
   */
  public void setTurnOffSkewed(boolean turnOffSkewed) {
    this.isTurnOffSkewed = turnOffSkewed;
  }

  /**
   * @param isDropIfExists the isDropIfExists to set
   */
  public void setDropIfExists(boolean isDropIfExists) {
    this.isDropIfExists = isDropIfExists;
  }

  /**
   * @return isDropIfExists
   */
  public boolean getIsDropIfExists() {
    return isDropIfExists;
  }

  /**
   * @return isCascade
   */
  public boolean getIsCascade() {
    return isCascade;
  }

  /**
   * @param cascade the isCascade to set
   */
  public void setIsCascade(boolean isCascade) {
    this.isCascade = isCascade;
  }

  public static boolean doesAlterTableTypeSupportPartialPartitionSpec(AlterTableTypes type) {
    return alterTableTypesWithPartialSpec.contains(type);
  }

  public EnvironmentContext getEnvironmentContext() {
    return environmentContext;
  }

  public void setEnvironmentContext(EnvironmentContext environmentContext) {
    this.environmentContext = environmentContext;
  }

  /**
   * @return what kind of replication scope this alter is running under.
   * This can result in a "ALTER IF NEWER THAN" kind of semantic
   */
  public ReplicationSpec getReplicationSpec(){ return this.replicationSpec; }

  @Override
  public void setWriteId(long writeId) {
    this.writeId = writeId;
  }

  @Override
  public String getFullTableName() {
    return getOldName();
  }

  @Override
  public boolean mayNeedWriteId() {
    switch (getOp()) {
    case ADDPROPS: {
      return isExplicitStatsUpdate || AcidUtils.isToInsertOnlyTable(null, getProps())
          || (AcidUtils.isTransactionalTable(getProps()) && !isFullAcidConversion);
    }
    case DROPPROPS: return isExplicitStatsUpdate;
    // The check for the following ones is performed before setting AlterTableDesc into the acid field.
    // These need write ID and stuff because they invalidate column stats.
    case RENAME_COLUMN:
    case RENAME:
    case REPLACE_COLUMNS:
    case ADD_COLUMNS:
    case SET_LOCATION:
    case UPDATE_COLUMNS: return true;
    // RENAMEPARTITION is handled in RenamePartitionDesc
    default: return false;
    }
  }

  public Long getWriteId() {
    return this.writeId;
  }

  public void setIsExplicitStatsUpdate(boolean b) {
    this.isExplicitStatsUpdate = b;
  }

  public void setIsFullAcidConversion(boolean b) {
    this.isFullAcidConversion = b;
  }
}
