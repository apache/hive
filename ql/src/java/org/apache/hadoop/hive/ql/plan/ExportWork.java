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

import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

@Explain(displayName = "Export Work", explainLevels = { Explain.Level.USER, Explain.Level.DEFAULT,
    Explain.Level.EXTENDED })
public class ExportWork implements Serializable {
  private Logger LOG = LoggerFactory.getLogger(ExportWork.class);

  private static final long serialVersionUID = 1L;

  private final String exportRootDirName;
  private TableSpec tableSpec;
  private ReplicationSpec replicationSpec;
  private String astRepresentationForErrorMsg;
  private String qualifiedTableName;

  /**
   * @param qualifiedTable if exporting Acid table, this is temp table - null otherwise
   */
  public ExportWork(String exportRootDirName, TableSpec tableSpec, ReplicationSpec replicationSpec,
      String astRepresentationForErrorMsg, String qualifiedTable) {
    this.exportRootDirName = exportRootDirName;
    this.tableSpec = tableSpec;
    this.replicationSpec = replicationSpec;
    this.astRepresentationForErrorMsg = astRepresentationForErrorMsg;
    this.qualifiedTableName = qualifiedTable;
  }

  public String getExportRootDir() {
    return exportRootDirName;
  }

  public TableSpec getTableSpec() {
    return tableSpec;
  }

  public void setTableSpec(TableSpec tableSpec) {
    this.tableSpec = tableSpec;
  }

  public ReplicationSpec getReplicationSpec() {
    return replicationSpec;
  }

  public void setReplicationSpec(ReplicationSpec replicationSpec) {
    this.replicationSpec = replicationSpec;
  }

  public String getAstRepresentationForErrorMsg() {
    return astRepresentationForErrorMsg;
  }

  public void setAstRepresentationForErrorMsg(String astRepresentationForErrorMsg) {
    this.astRepresentationForErrorMsg = astRepresentationForErrorMsg;
  }

  /**
   * For exporting Acid table, change the "pointer" to the temp table.
   * This has to be done after the temp table is populated and all necessary Partition objects
   * exist in the metastore.
   * See {@link org.apache.hadoop.hive.ql.parse.UpdateDeleteSemanticAnalyzer#isAcidExport(ASTNode)}
   * for more info.
   */
  public void acidPostProcess(Hive db) throws HiveException {
    if(qualifiedTableName != null) {
      LOG.info("Swapping export of " + tableSpec.tableName + " to " + qualifiedTableName +
          " using partSpec=" + tableSpec.partSpec);
      tableSpec = new TableSpec(db, qualifiedTableName, tableSpec.partSpec, true);
    }
  }
}
