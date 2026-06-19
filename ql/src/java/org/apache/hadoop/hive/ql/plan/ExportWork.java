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

import java.io.Serializable;

import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Explain(displayName = "Export Work", explainLevels = { Explain.Level.USER, Explain.Level.DEFAULT,
    Explain.Level.EXTENDED })
public class ExportWork implements Serializable {
  private static Logger LOG = LoggerFactory.getLogger(ExportWork.class);

  private static final long serialVersionUID = 1L;

  public final static class MmContext {
    private final String fqTableName;

    private MmContext(String fqTableName) {
      this.fqTableName = fqTableName;
    }

    @Override
    public String toString() {
      return "[" + fqTableName + "]";
    }

    public static MmContext createIfNeeded(Table t) {
      if (t == null) return null;
      if (!AcidUtils.isInsertOnlyTable(t.getParameters())) return null;
      return new MmContext(AcidUtils.getFullTableName(t.getDbName(), t.getTableName()));
    }

    public String getFqTableName() {
      return fqTableName;
    }
  }

  private final String exportRootDirName;
  private TableSpec tableSpec;
  private ReplicationSpec replicationSpec;
  private String astRepresentationForErrorMsg;
  private String acidFqTableName;
  private final MmContext mmContext;

  /**
   * @param acidFqTableName if exporting Acid table, this is temp table - null otherwise
   */
  public ExportWork(String exportRootDirName, TableSpec tableSpec, ReplicationSpec replicationSpec,
      String astRepresentationForErrorMsg, String acidFqTableName, MmContext mmContext) {
    this.exportRootDirName = exportRootDirName;
    this.tableSpec = tableSpec;
    this.replicationSpec = replicationSpec;
    this.astRepresentationForErrorMsg = astRepresentationForErrorMsg;
    this.mmContext = mmContext;
    this.acidFqTableName = acidFqTableName;
  }

  public String getExportRootDir() {
    return exportRootDirName;
  }

  public TableSpec getTableSpec() {
    return tableSpec;
  }

  public ReplicationSpec getReplicationSpec() {
    return replicationSpec;
  }

  public String getAstRepresentationForErrorMsg() {
    return astRepresentationForErrorMsg;
  }

  public MmContext getMmContext() {
    return mmContext;
  }

  /**
   * For exporting Acid table, change the "pointer" to the temp table.
   * This has to be done after the temp table is populated and all necessary Partition objects
   * exist in the metastore.
   * See {@link org.apache.hadoop.hive.ql.parse.AcidExportSemanticAnalyzer#isAcidExport(ASTNode)}
   * for more info.
   */
  public void acidPostProcess(Hive db) throws HiveException {
    if (acidFqTableName != null) {
      LOG.info("Swapping export of " + tableSpec.getTableName().getTable() + " to " + acidFqTableName +
          " using partSpec=" + tableSpec.partSpec);
      tableSpec = new TableSpec(db, acidFqTableName, tableSpec.partSpec, true);
    }
  }
}
