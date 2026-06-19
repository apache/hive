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

package org.apache.hadoop.hive.ql.ddl.index;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.IndexType;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.anon.ConstCode;
import org.apache.hadoop.hive.ql.anon.policy.DataErasurePolicy;
import org.apache.hadoop.hive.ql.anon.utils.Utils;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.List;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.*;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.*;
import static org.apache.hadoop.hive.ql.anon.utils.Utils.getAddrType;

@DDLSemanticAnalyzerFactory.DDLType(types = HiveParser.TOK_AIR)
public class AlterIndexRebuildAnalyzer extends BaseSemanticAnalyzer {

  public AlterIndexRebuildAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  private java.util.List<org.apache.hadoop.hive.metastore.api.ErasurePolicyBindingMember>
      safeGetMembers(final long bindingId) {
    try {
      return db.getBindingMembers(bindingId);
    } catch (HiveException he) {
      return java.util.Collections.emptyList();
    }
  }

  private java.util.List<org.apache.hadoop.hive.metastore.api.PolicyInfo>
      safeGetAllPolicyNames() {
    try {
      final java.util.List<org.apache.hadoop.hive.metastore.api.PolicyInfo> v =
          db.getAllErasurePolicyNames();
      return v == null ? java.util.Collections.emptyList() : v;
    } catch (HiveException he) {
      return java.util.Collections.emptyList();
    }
  }

  /**
   * Mirror of {@code AnonStatementAnalyzer.composeActiveVersionJson}:
   * rebuild the {@link DataErasurePolicy} JSON view from the active
   * version's parsed metadata plus its statement/rule rows. Used by
   * REBUILD to obtain the policy body from the active version (the
   * single source of truth), whether it was stored as JSON or DSL.
   */
  private String buildActiveVersionJson(final String policyName) throws HiveException {
    final org.apache.hadoop.hive.metastore.api.ErasurePolicyVersion active =
        db.getActiveErasurePolicyVersion(policyName);
    if (active == null) return null;
    final com.fasterxml.jackson.databind.node.ObjectNode root =
        DataErasurePolicy.mapper.createObjectNode();
    root.put("identityFieldName", active.getIdentityFieldName());
    root.put("identityFieldType",
        active.getIdentityFieldType().name().toLowerCase());
    root.put("schemaFieldType", active.getSchemaType().name().toLowerCase());
    final com.fasterxml.jackson.databind.node.ArrayNode statementsArr =
        root.putArray("statements");
    for (final org.apache.hadoop.hive.metastore.api.ErasurePolicyStatement s
        : db.getErasurePolicyStatements(active.getVersionId())) {
      final com.fasterxml.jackson.databind.node.ObjectNode sNode =
          statementsArr.addObject();
      sNode.put("schemaId", s.getSchemaValue());
      final com.fasterxml.jackson.databind.node.ArrayNode rulesArr =
          sNode.putArray("rules");
      for (final org.apache.hadoop.hive.metastore.api.ErasurePolicyRule r
          : db.getErasurePolicyRules(s.getStatementId())) {
        final com.fasterxml.jackson.databind.node.ObjectNode rNode =
            rulesArr.addObject();
        rNode.put("path", r.getFieldPath());
        rNode.put("action", r.getAction() == null ? null : r.getAction().name());
        if (r.isSetLiteralValue()) {
          rNode.put("value", r.getLiteralValue());
        }
      }
    }
    try {
      return DataErasurePolicy.mapper.writeValueAsString(root);
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      throw new HiveException("active-version JSON serialisation failed: "
          + e.getMessage(), e);
    }
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    final String indexName = ast.getChild(0).getChild(0).getText();
    final TableName qualifiedTableName = getQualifiedTableName((ASTNode) ast.getChild(1));
    // Same POLICY_MANAGE gate as CREATE/DROP IDENTITY INDEX: a rebuild
    // rewrites the structure the erasure path trusts. Checked before any
    // metadata resolution so unprivileged callers learn nothing.
    org.apache.hadoop.hive.ql.anon.utils.PolicyPrivilegeUtils.requirePrivilege(
        conf, db, org.apache.hadoop.hive.ql.anon.consts.AnonConst.PRIV_POLICY_MANAGE,
        qualifiedTableName.getDb() + "." + qualifiedTableName.getTable());

    Index index = null;
    try {
      final String baseTableName = qualifiedTableName.getTable();
      index = db.getIndex(baseTableName, indexName);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
    if (index == null) {
      throw new SemanticException("Index " + indexName + " not found");
    }
    final String tblName = index.getOrigTableName();
    final Table table = getTable(tblName);
    if (table == null) {
      throw new SemanticException("Table " + tblName + " not found");
    }
    // §4.6 binding-row read: the ATTACH path writes the
    // ErasurePolicyBinding row, the single source of the bound-column
    // metadata. Iterate the table's columns to find the bound one.
    final Table ixTable = getTable(index.getIndexTableName());

    org.apache.hadoop.hive.metastore.api.ErasurePolicyBinding b = null;
    final long tblIdLocal = (table.getTTable() != null
        && table.getTTable().isSetId()) ? table.getTTable().getId() : 0L;
    for (org.apache.hadoop.hive.metastore.api.FieldSchema fs
        : table.getTTable().getSd().getCols()) {
      try {
        b = db.getErasurePolicyBinding(tblIdLocal, fs.getName());
      } catch (HiveException ignored) { /* try next */ }
      if (b != null && b.isSetSchemaField() && b.isSetRowLocator()
          && b.isSetColumnFormat()) break;
      b = null;
    }
    if (b == null) {
      throw new SemanticException("ALTER INDEX REBUILD: no erasure-policy "
          + "binding on " + tblName + ". ATTACH first.");
    }
    final String columnWithPolicy = b.getColumnName();
    final ColumnInternalFormat internalFormat = b.getColumnFormat();
    final String schemaColumnName = b.getSchemaField();
    final String rowLocatorName   = b.getRowLocator();
    // Resolve the policy JSON from the active version of the
    // binding's first member policy.
    org.apache.hadoop.hive.metastore.api.ErasurePolicy ep = null;
    for (org.apache.hadoop.hive.metastore.api.ErasurePolicyBindingMember m
        : safeGetMembers(b.getBindingId())) {
      for (org.apache.hadoop.hive.metastore.api.PolicyInfo pi
          : safeGetAllPolicyNames()) {
        try {
          ep = db.getErasurePolicy(pi.getName());
        } catch (HiveException ignored) { ep = null; }
        if (ep != null && ep.isSetPolicyId() && ep.getPolicyId() == m.getPolicyId()) {
          break;
        }
        ep = null;
      }
      if (ep != null) break;
    }
    if (ep == null) {
      throw new SemanticException("ALTER INDEX REBUILD: cannot resolve "
          + "policy for binding on " + tblName);
    }
    // Build JSON form from the active version (parsed metadata, falling
    // back to its SOURCE_TEXT) -- the single source of truth.
    String policyJson;
    try {
      policyJson = buildActiveVersionJson(ep.getPolicyName());
    } catch (HiveException he) {
      throw new SemanticException("ALTER INDEX REBUILD: failed to build "
          + "policy JSON for '" + ep.getPolicyName() + "': " + he.getMessage());
    }
    if (policyJson == null) {
      throw new SemanticException("ALTER INDEX REBUILD: policy '"
          + ep.getPolicyName() + "' has no ACTIVE version");
    }
    final String json = policyJson;
    DataErasurePolicy erasurePolicy = DataErasurePolicy.fromString(json);
    final String indexTableName = index.getIndexTableName();

    conf.set(ANON_POLICY_DOC, json);
    IndexType indexType = index.getIndexType();
    conf.set(ANON_INDEX_TYPE, indexType.toString());
    if (indexType == IndexType.BTREE) {
      final int pageSize = index.getPageSize();
      final int bufferSize = index.getBufferPoolSize() * pageSize;

      conf.setInt(BTREE_CONF_BUFFER_SIZE, bufferSize);
      conf.setInt(BTREE_CONF_PAGE_SIZE, pageSize);
      conf.setInt(BTREE_CONF_PAGE_HEADER_SIZE, INDEX_HEADER_SIZE);
      conf.setInt(BTREE_CONF_DUMP_BYTES_PER_ROW, pageSize);

      conf.set(INDEX_ADDR_TYPE, getAddrType(index));
    } else if (indexType == IndexType.DIRECTORY) {
      conf.set(INDEX_ADDR_TYPE, getAddrType(index));

    } else if (indexType == IndexType.TABULAR) {

    } else {
      throw new SemanticException("Unsupported index type: " + indexType);
    }

    ConstCode constCode = Utils.getConstCode(internalFormat);
    conf.set(ANON_COLUMN_INTERNAL_FORMAT, internalFormat.name());
    // The type letters seeded here end up in the rebuilt index file's header
    // (DirectoryIndex/TabularIndex/PageManager read them from the job conf),
    // so they must match what the rebuild query serialises: the key is typed
    // by the policy's identity declaration, the value struct by the base
    // table's locator/schema columns (filePath is always Text).
    conf.set(INDEX_KEY_TYPE, Utils.getType(erasurePolicy.identityFieldType));
    String offsetColType = null;
    String msgIdColType = null;
    for (org.apache.hadoop.hive.metastore.api.FieldSchema fs : table.getCols()) {
      if (fs.getName().equalsIgnoreCase(rowLocatorName)) {
        offsetColType = fs.getType();
      }
      if (fs.getName().equalsIgnoreCase(schemaColumnName)) {
        msgIdColType = fs.getType();
      }
    }
    if (offsetColType == null || msgIdColType == null) {
      throw new SemanticException("ALTER INDEX REBUILD: row-locator/schema "
          + "columns (" + rowLocatorName + ", " + schemaColumnName
          + ") not found on " + tblName);
    }
    conf.set(INDEX_VALUE_TYPES,
        "T" + Utils.getType(offsetColType) + Utils.getType(msgIdColType));

    HiveConf builderConf = new HiveConf(conf);
    HiveConf.setBoolVar(builderConf, HiveConf.ConfVars.HIVE_RPC_QUERY_PLAN, true);
    HiveConf.setBoolVar(builderConf, HiveConf.ConfVars.HIVE_STATS_AUTOGATHER, false);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_RPC_QUERY_PLAN, true);
    Driver driver = new Driver(builderConf);

    String constColumn = String.format("'dBOMFI%s'", constCode);
    String idFieldName = erasurePolicy.identityFieldName;
    String cols = String.format("%s, %s, %s, %s, INPUT__FILE__NAME, '%s'",
      constColumn, // '_BOMFI_' // trailing '_' represents the constCode
      columnWithPolicy, // B
      rowLocatorName,   // O
      schemaColumnName, // M
      idFieldName // I
    );
    String command = String.format("WITH src AS (SELECT umfo(%s) FROM %s AS t) " +
      ", grp AS (SELECT key, val_2, COLLECT_LIST(struct(val_0, val_1)) AS mo FROM src GROUP BY key, val_2) " +
      "INSERT OVERWRITE TABLE %s " +
      "SELECT key, collect_list(struct(val_2, mo)) " +
      "FROM grp " +
      "GROUP BY key " +
      "ORDER BY key", cols, tblName, indexTableName
    );

    driver.compile(command, false);

    List<Task<?>> lst = driver.getPlan().getRootTasks();
    Task<?> rootTask = lst.get(0);
    inputs.addAll(driver.getPlan().getInputs());
    outputs.addAll(driver.getPlan().getOutputs());
    rootTasks.add(rootTask);

    AlterIndexRebuildDesc desc = new AlterIndexRebuildDesc();
    DDLWork work = new DDLWork(getInputs(), getOutputs(), desc);
    Task<?> task = TaskFactory.get(work);
    rootTask.addDependentTask(task);
  }

}
