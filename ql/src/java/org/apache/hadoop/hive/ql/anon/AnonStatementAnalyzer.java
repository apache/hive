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

package org.apache.hadoop.hive.ql.anon;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.metastore.api.ErasurePolicy;
import org.apache.hadoop.hive.metastore.api.ErasurePolicyBinding;
import org.apache.hadoop.hive.metastore.api.ErasurePolicyBindingMember;
import org.apache.hadoop.hive.metastore.api.ErasurePolicyBindingResolved;
import org.apache.hadoop.hive.metastore.api.ErasurePolicyLifecycleEvent;
import org.apache.hadoop.hive.metastore.api.ErasurePolicyRule;
import org.apache.hadoop.hive.metastore.api.ErasurePolicyStatement;
import org.apache.hadoop.hive.metastore.api.ErasurePolicyVersion;
import org.apache.hadoop.hive.metastore.api.ErasureRunAudit;
import org.apache.hadoop.hive.metastore.api.ErasureRunLock;
import org.apache.hadoop.hive.metastore.api.ErasureRunStatus;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.IndexType;
import org.apache.hadoop.hive.metastore.api.PolicyActionKind;
import org.apache.hadoop.hive.metastore.api.PolicyInfo;
import org.apache.hadoop.hive.metastore.api.PolicyLifecycleEventType;
import org.apache.hadoop.hive.metastore.api.PolicyLiteralKind;
import org.apache.hadoop.hive.metastore.api.PolicyPriv;
import org.apache.hadoop.hive.metastore.api.PolicyResolutionMode;
import org.apache.hadoop.hive.metastore.api.PolicyVersionStatus;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.anon.btree.IndexSerde;
import org.apache.hadoop.hive.ql.anon.consts.AnonConst;
import org.apache.hadoop.hive.ql.anon.hooks.ErasureRunCompletionHook;
import org.apache.hadoop.hive.ql.anon.policy.DataErasurePolicy;
import org.apache.hadoop.hive.ql.anon.tez.AnonWork;
import org.apache.hadoop.hive.ql.anon.tez.ExtractProcessor;
import org.apache.hadoop.hive.ql.anon.tez.ExtractWork;
import org.apache.hadoop.hive.ql.anon.tez.IndexWork;
import org.apache.hadoop.hive.ql.anon.utils.PolicyPrivilegeUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PartitionIterable;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hive.hep.ErasurePolicyValidator.ValidationError;
import org.apache.hive.hep.ErasurePolicyValidator.ValidationResult;
import org.apache.hive.hep.ErasurePolicyValidator;
import org.apache.hive.hep.ParsedPolicy;
import org.apache.hive.hep.PolicyConflictDetector.Conflict;
import org.apache.hive.hep.PolicyConflictDetector.Report;
import org.apache.hive.hep.PolicyConflictDetector.ResolutionMode;
import org.apache.hive.hep.PolicyConflictDetector.ResolvedRule;
import org.apache.hive.hep.PolicyConflictDetector;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.*;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.*;
import static org.apache.hadoop.hive.ql.anon.utils.Utils.getAddrType;
import static org.apache.hadoop.hive.ql.anon.utils.Utils.getType;
import static org.apache.hadoop.hive.ql.io.IOConstants.*;
public class AnonStatementAnalyzer extends BaseSemanticAnalyzer {

  private List<FieldSchema> extractResultSchema;

  public AnonStatementAnalyzer(final QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public List<FieldSchema> getResultSchema() {
    return extractResultSchema;
  }

  @Override
  public void analyzeInternal(final ASTNode ast) throws SemanticException {
    if (ast.getType() == HiveParser.TOK_AT) {
      analyzeAnonymizeTable(ast);
    } else if (ast.getType() == HiveParser.TOK_DAP) {
      analyzeDropErasurePolicy(ast);
    } else if (ast.getType() == HiveParser.TOK_CAI) {
      analyzeCreateIndex(ast);
    } else if (ast.getType() == HiveParser.TOK_DAI) {
      analyzeDropIndex(ast);
    } else if (ast.getType() == HiveParser.TOK_EXPLAIN_ATTACH) {
      analyzeExplainAttachPolicy(ast);
    } else if (ast.getType() == HiveParser.TOK_VALIDATE_BINDING) {
      analyzeValidateErasureBinding(ast);
    } else if (ast.getType() == HiveParser.TOK_AUDIT_POLICY) {
      analyzeAuditErasurePolicy(ast);
    } else if (ast.getType() == HiveParser.TOK_AUDIT_BINDING) {
      analyzeAuditErasureBinding(ast);
    } else if (ast.getType() == HiveParser.TOK_AUDIT_EXEC) {
      analyzeAuditErasureExecutions(ast);
    } else if (ast.getType() == HiveParser.TOK_AUDIT_CONFLICTS) {
      analyzeAuditErasureConflicts(ast);
    } else if (ast.getType() == HiveParser.TOK_AUDIT_COMPLIANCE) {
      analyzeAuditErasureCompliance(ast);
    } else if (ast.getType() == HiveParser.TOK_LEP) {
      analyzeLoadErasurePolicy(ast);
    } else if (ast.getType() == HiveParser.TOK_VEP) {
      analyzeValidateErasurePolicy(ast);
    } else if (ast.getType() == HiveParser.TOK_AEP) {
      analyzeActivateErasurePolicy(ast);
    } else if (ast.getType() == HiveParser.TOK_DEP) {
      analyzeDeactivateErasurePolicy(ast);
    } else if (ast.getType() == HiveParser.TOK_INVALIDATE_EP) {
      analyzeInvalidateErasurePolicy(ast);
    } else if (ast.getType() == HiveParser.TOK_ATTACH_POLICY) {
      analyzeAttachErasurePolicyCmd(ast);
    } else if (ast.getType() == HiveParser.TOK_DETACH_POLICY) {
      analyzeDetachErasurePolicyCmd(ast);
    } else if (ast.getType() == HiveParser.TOK_SHOW_ERASURE_LOCKS) {
      analyzeShowAllErasureLocks(ast);
    } else if (ast.getType() == HiveParser.TOK_RELEASE_ERASURE_LOCK) {
      analyzeReleaseErasureLock(ast);
    } else if (ast.getType() == HiveParser.TOK_POLICY_GRANT
        || ast.getType() == HiveParser.TOK_POLICY_GRANT_ALL) {
      analyzeGrantPolicyPriv(ast);
    } else if (ast.getType() == HiveParser.TOK_POLICY_REVOKE
        || ast.getType() == HiveParser.TOK_POLICY_REVOKE_ALL) {
      analyzeRevokePolicyPriv(ast);
    } else if (ast.getType() == HiveParser.TOK_EXTRACT_FROM_TABLE) {
      analyzeExtractFromTable(ast);
    } else if (ast.getType() == HiveParser.TOK_AUDIT_BY_IDENTITY) {
      analyzeAuditByIdentity(ast);
    }
  }

  private void analyzeAttachErasurePolicyCmd(final ASTNode ast) throws SemanticException {
    final ASTNode policyList = (ASTNode) ast.getChild(0);
    final TableName tn = getQualifiedTableName((ASTNode) ast.getChild(1));
    final String column = ast.getChild(2).getText().toLowerCase();
    requirePrivilege(PRIV_POLICY_MANAGE, qualified(tn) + "." + column);
    final List<String> policyNames = new ArrayList<>(policyList.getChildCount());
    for (int i = 0; i < policyList.getChildCount(); i++) {
      policyNames.add(policyList.getChild(i).getText());
    }

    String schemaField = null;
    String rowLocator = null;
    ColumnInternalFormat columnFormat = null;
    PolicyResolutionMode resolutionMode = null;
    if (ast.getChildCount() >= 4) {
      final ASTNode opts = (ASTNode) ast.getChild(3);
      for (int i = 0; i < opts.getChildCount(); i++) {
        final ASTNode c = (ASTNode) opts.getChild(i);
        switch (c.getType()) {
        case HiveParser.TOK_JSON:     columnFormat = ColumnInternalFormat.JSON;     break;
        case HiveParser.TOK_MSGPACK:  columnFormat = ColumnInternalFormat.MSGPACK;  break;
        case HiveParser.TOK_XML:      columnFormat = ColumnInternalFormat.XML;      break;
        case HiveParser.TOK_PROTOBUF: columnFormat = ColumnInternalFormat.PROTOBUF; break;
        case HiveParser.TOK_AVRO:     columnFormat = ColumnInternalFormat.AVRO;     break;
        case HiveParser.TOK_RESOLUTION_EXPLICIT:  resolutionMode = PolicyResolutionMode.EXPLICIT;  break;
        case HiveParser.TOK_RESOLUTION_STRICTEST: resolutionMode = PolicyResolutionMode.STRICTEST; break;
        default:
          if (schemaField == null) {
            schemaField = c.getText().toLowerCase();
          } else if (rowLocator == null) {
            rowLocator = c.getText().toLowerCase();
          }
        }
      }
    }
    final boolean schemaFieldSpecified  = schemaField != null;
    final boolean rowLocatorSpecified   = rowLocator != null;
    final boolean columnFormatSpecified = columnFormat != null;
    final boolean resolutionSpecified   = resolutionMode != null;

    final long tblId = resolveTableId(tn);
    final String otherCol = otherBoundColumn(tn, tblId, column);
    if (otherCol != null) {
      throw new SemanticException("ATTACH POLICY: table " + qualified(tn)
          + " already has an erasure-policy binding on column '" + otherCol
          + "'. All policies attached to a table must target the same column. "
          + "Attach to '" + otherCol + "', or DETACH the existing binding to "
          + "switch columns.");
    }
    ErasurePolicyBinding binding;
    try {
      binding = db.getErasurePolicyBinding(tblId, column);
    } catch (HiveException missing) {
      binding = null;
    }

    if (binding != null) {
      final List<String> diffs = new ArrayList<>();
      if (resolutionSpecified && binding.getResolutionMode() != null
          && binding.getResolutionMode() != resolutionMode) {
        diffs.add("RESOLUTION " + binding.getResolutionMode().name()
            + " -> " + resolutionMode.name());
      }
      if (schemaFieldSpecified && binding.isSetSchemaField()
          && !schemaField.equals(binding.getSchemaField())) {
        diffs.add("SCHEMA FIELD '" + binding.getSchemaField() + "' -> '" + schemaField + "'");
      }
      if (rowLocatorSpecified && binding.isSetRowLocator()
          && !rowLocator.equals(binding.getRowLocator())) {
        diffs.add("ROW LOCATOR '" + binding.getRowLocator() + "' -> '" + rowLocator + "'");
      }
      if (columnFormatSpecified && binding.getColumnFormat() != null
          && binding.getColumnFormat() != columnFormat) {
        diffs.add("COLUMN FORMAT " + binding.getColumnFormat().name()
            + " -> " + columnFormat.name());
      }
      if (!diffs.isEmpty()) {
        throw new SemanticException("ATTACH POLICY: binding settings are fixed "
            + "at the first ATTACH; DETACH and re-ATTACH to change "
            + "RESOLUTION/WITH options. Differing: " + String.join("; ", diffs) + ".");
      }
    }

    final PolicyResolutionMode effectiveMode;
    if (binding != null && binding.getResolutionMode() != null) {
      effectiveMode = binding.getResolutionMode();
    } else if (resolutionSpecified) {
      effectiveMode = resolutionMode;
    } else {
      effectiveMode = PolicyResolutionMode.EXPLICIT;
    }

    final Map<String, ParsedPolicy> attached = new LinkedHashMap<>();
    final Set<String> existingMemberNames = new LinkedHashSet<>();
    if (binding != null) {
      final List<ErasurePolicyBindingMember> existing;
      try {
        existing = db.getBindingMembers(binding.getBindingId());
      } catch (HiveException he) {
        throw new SemanticException("ATTACH POLICY: member lookup failed for binding "
            + binding.getBindingId() + ": " + he.getMessage());
      }
      for (final ErasurePolicyBindingMember m
          : existing == null ? Collections.<ErasurePolicyBindingMember>emptyList() : existing) {
        final ErasurePolicy member = lookupPolicyById(m.getPolicyId());
        if (member == null) {
          throw new SemanticException("ATTACH POLICY: existing binding member with "
              + "policy id " + m.getPolicyId() + " does not resolve to a policy; "
              + "DETACH the binding and re-ATTACH the surviving policies.");
        }
        final String name = member.getPolicyName();
        if (!existingMemberNames.add(name)) {
          continue;
        }
        final String body;
        try {
          body = activePolicySource(name);
        } catch (HiveException he) {
          throw new SemanticException("ATTACH POLICY: active-version lookup failed for "
              + "existing binding member '" + name + "': " + he.getMessage());
        }
        if (body == null || body.isEmpty()) {
          throw new SemanticException("ATTACH POLICY: existing binding member '" + name
              + "' has no ACTIVE version; ACTIVATE it or DETACH the binding.");
        }
        try {
          attached.put(name, ErasurePolicyValidator.parse(body));
        } catch (IllegalArgumentException iae) {
          throw new SemanticException("ATTACH POLICY: existing binding member '" + name
              + "' failed to parse as .erp DSL: " + iae.getMessage());
        }
      }
    }

    for (String name : policyNames) {
      if (attached.containsKey(name)) {
        continue;
      }
      final ErasurePolicy policy;
      try {
        policy = db.getErasurePolicy(name);
      } catch (HiveException he) {
        throw new SemanticException("ATTACH POLICY: lookup failed for '" + name + "': " + he.getMessage());
      }
      if (policy == null) {
        throw new SemanticException("ATTACH POLICY: '" + name + "' not found.");
      }
      final String body;
      try {
        body = activePolicySource(name);
      } catch (HiveException he) {
        throw new SemanticException("ATTACH POLICY: active-version lookup failed for '"
            + name + "': " + he.getMessage());
      }
      if (body == null || body.isEmpty()) {
        throw new SemanticException("ATTACH POLICY: '" + name
            + "' has no ACTIVE version; ACTIVATE before ATTACH.");
      }
      try {
        attached.put(name, ErasurePolicyValidator.parse(body));
      } catch (IllegalArgumentException iae) {
        throw new SemanticException("ATTACH POLICY: '" + name
            + "' failed to parse as .erp DSL: " + iae.getMessage());
      }
    }

    final PolicyConflictDetector.ResolutionMode mode =
        (effectiveMode == PolicyResolutionMode.STRICTEST)
            ? PolicyConflictDetector.ResolutionMode.STRICTEST
            : PolicyConflictDetector.ResolutionMode.EXPLICIT;
    final PolicyConflictDetector.Report report = PolicyConflictDetector.analyse(attached, mode);
    if (mode == PolicyConflictDetector.ResolutionMode.EXPLICIT && report.hasConflicts()) {
      final StringBuilder msg = new StringBuilder("ATTACH POLICY refused under EXPLICIT mode; ");
      msg.append(report.conflicts.size()).append(" conflict(s):\n");
      for (PolicyConflictDetector.Conflict c : report.conflicts) {
        msg.append("  ").append(c.toString()).append('\n');
      }
      final String rejectNote = "ATTACH POLICY [" + String.join(", ", policyNames)
          + "] ON " + qualified(tn) + "." + column + " refused (EXPLICIT). "
          + report.conflicts.size() + " conflict(s):\n" + msg.substring(
              msg.indexOf("\n") + 1);
      final String rejectPrincipal = currentPrincipal();
      for (final String pn : attached.keySet()) {
        try {
          final ErasurePolicyVersion active = db.getActiveErasurePolicyVersion(pn);
          if (active != null) {
            recordLifecycle(active.getVersionId(),
                PolicyLifecycleEventType.ATTACH_REJECTED, rejectPrincipal,
                rejectNote);
          }
        } catch (HiveException he) {
          LOG.warn("ATTACH_REJECTED lifecycle write failed for policy '{}': {}",
              pn, he.getMessage());
        }
      }
      throw new SemanticException(msg.toString());
    }

    boolean createdBinding = false;
    if (binding == null) {
      binding = new ErasurePolicyBinding();
      binding.setTblId(tblId);
      binding.setColumnName(column);
      binding.setSchemaField(schemaField != null ? schemaField : column);
      binding.setRowLocator(rowLocator != null ? rowLocator : "offset");
      binding.setColumnFormat(columnFormat != null ? columnFormat : ColumnInternalFormat.JSON);
      binding.setResolutionMode(effectiveMode);
      binding.setCreatedBy(currentPrincipal());
      binding.setCreatedTs(System.currentTimeMillis());
      try {
        binding = db.addErasurePolicyBinding(binding);
        createdBinding = true;
      } catch (HiveException he) {
        throw new SemanticException("ATTACH POLICY: failed to create binding: " + he.getMessage());
      }
    }

    final List<ErasurePolicyBindingResolved> resolvedRows = new ArrayList<>();
    try {
    int ordinal = existingMemberNames.size();
    for (final String name : policyNames) {
      if (existingMemberNames.contains(name)) {
        continue;
      }
      try {
        final ErasurePolicy ep = db.getErasurePolicy(name);
        if (ep == null || !ep.isSetPolicyId() || ep.getPolicyId() == 0L) {
          throw new SemanticException("ATTACH POLICY: cannot resolve policyId for '"
              + name + "'");
        }
        db.attachPolicyToBinding(binding.getBindingId(), ep.getPolicyId(), ordinal++);
      } catch (HiveException he) {
        final String msg = he.getMessage() == null ? "" : he.getMessage();
        if (!msg.contains("AlreadyExists") && !msg.contains("UNIQUE")
            && !msg.contains("already exists")) {
          throw new SemanticException("ATTACH POLICY: failed to record binding member "
              + name + ": " + msg);
        }
      }
    }

    for (PolicyConflictDetector.ResolvedRule r : report.resolvedRules) {
      resolvedRows.add(toThriftResolved(binding.getBindingId(), r));
    }
    if (resolvedRows.isEmpty()) {
      for (Map.Entry<String, ParsedPolicy> e : attached.entrySet()) {
        for (ParsedPolicy.Statement s : e.getValue().getStatements()) {
          for (ParsedPolicy.Rule r : s.rules) {
            resolvedRows.add(toThriftResolvedUnion(
                binding.getBindingId(), s.schemaId, r, e.getKey()));
          }
        }
      }
    }
    try {
      db.replaceBindingResolvedRules(binding.getBindingId(), resolvedRows);
    } catch (HiveException he) {
      throw new SemanticException("ATTACH POLICY: failed to persist resolved rules: " + he.getMessage());
    }
    } catch (SemanticException | RuntimeException e) {
      if (createdBinding) {
        try {
          db.dropErasurePolicyBinding(binding.getBindingId());
        } catch (HiveException drop) {
          LOG.warn("ATTACH POLICY rollback: failed to drop partial binding {}: {}",
              binding.getBindingId(), drop.getMessage());
        }
      }
      throw e;
    }

    final String principal = currentPrincipal();
    for (String name : policyNames) {
      try {
        final ErasurePolicyVersion active = db.getActiveErasurePolicyVersion(name);
        if (active != null) {
          recordLifecycle(active.getVersionId(), PolicyLifecycleEventType.BOUND, principal,
              "ATTACH POLICY " + name + " ON " + qualified(tn) + " COLUMN " + column);
        }
      } catch (HiveException he) {
      }
    }

    LOG.info("ATTACH POLICY {} ON {} COLUMN {} -> binding_id={}, mode={}, members={}, resolved_rows={}",
        policyNames, qualified(tn), column, binding.getBindingId(),
        effectiveMode, attached.size(), resolvedRows.size());
  }

  private void analyzeDetachErasurePolicyCmd(final ASTNode ast) throws SemanticException {
    final TableName tn = getQualifiedTableName((ASTNode) ast.getChild(0));
    final String column = ast.getChild(1).getText().toLowerCase();
    requirePrivilege(PRIV_POLICY_MANAGE, qualified(tn) + "." + column);
    final List<String> policyNames = new ArrayList<>();
    if (ast.getChildCount() >= 3) {
      final ASTNode list = (ASTNode) ast.getChild(2);
      for (int i = 0; i < list.getChildCount(); i++) {
        policyNames.add(list.getChild(i).getText());
      }
    }

    final long tblId = resolveTableId(tn);
    final ErasurePolicyBinding binding;
    try {
      binding = db.getErasurePolicyBinding(tblId, column);
    } catch (HiveException he) {
      throw new SemanticException("DETACH POLICY: no binding on " + qualified(tn) + "." + column);
    }
    if (binding == null) {
      throw new SemanticException("DETACH POLICY: no binding on " + qualified(tn) + "." + column);
    }

    try {
      db.dropErasurePolicyBinding(binding.getBindingId());
    } catch (HiveException he) {
      throw new SemanticException("DETACH POLICY: failed to drop binding: " + he.getMessage());
    }

    final String principal = currentPrincipal();
    final String evtNote = "DETACH POLICY " + (policyNames.isEmpty() ? "(all)" : policyNames)
        + " ON " + qualified(tn) + " COLUMN " + column;
    for (String name : (policyNames.isEmpty() ? Collections.singletonList("*") : policyNames)) {
      if ("*".equals(name)) {
        continue;
      }
      try {
        final ErasurePolicyVersion active = db.getActiveErasurePolicyVersion(name);
        if (active != null) {
          recordLifecycle(active.getVersionId(), PolicyLifecycleEventType.UNBOUND, principal, evtNote);
        }
      } catch (HiveException he) {
      }
    }

    LOG.info("DETACH POLICY {} ON {} COLUMN {} -> binding_id={} dropped",
        policyNames.isEmpty() ? "(all)" : policyNames, qualified(tn), column, binding.getBindingId());
  }

  private long resolveTableId(final TableName tn) {
    try {
      final Table t = getTable(tn.getNotEmptyDbTable());
      return (t.getTTable() != null && t.getTTable().isSetId()) ? t.getTTable().getId() : 0L;
    } catch (Exception e) {
      return 0L;
    }
  }

  private String otherBoundColumn(final TableName tn, final long tblId, final String column) {
    final Table tbl;
    try {
      tbl = getTable(tn.getNotEmptyDbTable());
    } catch (Exception e) {
      return null;
    }
    if (tbl == null) {
      return null;
    }
    for (final FieldSchema fs : tbl.getCols()) {
      final String col = fs.getName().toLowerCase();
      if (col.equals(column)) {
        continue;
      }
      try {
        if (db.getErasurePolicyBinding(tblId, col) != null) {
          return col;
        }
      } catch (HiveException ignored) {
      }
    }
    return null;
  }

  private ErasurePolicy lookupPolicyById(final long policyId) {
    try {
      final List<PolicyInfo> all = db.getAllErasurePolicyNames();
      if (all == null) {
        return null;
      }
      for (final PolicyInfo info : all) {
        final ErasurePolicy ep = db.getErasurePolicy(info.getName());
        if (ep != null && ep.isSetPolicyId() && ep.getPolicyId() == policyId) {
          return ep;
        }
      }
    } catch (HiveException ignored) {  }
    return null;
  }

  private ErasurePolicyBindingResolved toThriftResolved(final long bindingId,
                                                        final PolicyConflictDetector.ResolvedRule r) {
    final ErasurePolicyBindingResolved row = new ErasurePolicyBindingResolved();
    row.setBindingId(bindingId);
    row.setSchemaValue(r.schemaId);
    row.setFieldPath(r.rule.fieldPath);
    row.setAction(toThriftAction(r.rule.action));
    if (r.rule.replaceValue != null) {
      row.setLiteralValue(r.rule.replaceValue);
    }
    if (r.rule.replaceValueKind != null) {
      row.setLiteralType(toThriftLiteralKind(r.rule.replaceValueKind));
    }
    row.setContributingPolicies(joinPolicies(r.contributingPolicies));
    if (r.resolutionNote != null) {
      row.setResolutionNote(r.resolutionNote);
    }
    return row;
  }

  private ErasurePolicyBindingResolved toThriftResolvedUnion(final long bindingId, final String schemaId,
                                                             final ParsedPolicy.Rule r, final String policyName) {
    final ErasurePolicyBindingResolved row = new ErasurePolicyBindingResolved();
    row.setBindingId(bindingId);
    row.setSchemaValue(schemaId);
    row.setFieldPath(r.fieldPath);
    row.setAction(toThriftAction(r.action));
    if (r.replaceValue != null) {
      row.setLiteralValue(r.replaceValue);
    }
    if (r.replaceValueKind != null) {
      row.setLiteralType(toThriftLiteralKind(r.replaceValueKind));
    }
    row.setContributingPolicies("[\"" + policyName + "\"]");
    return row;
  }

  private static PolicyActionKind toThriftAction(final ParsedPolicy.ActionKind a) {
    switch (a) {
    case ERASE:   return PolicyActionKind.ERASE;
    case REPLACE: return PolicyActionKind.REPLACE;
    case INSPECT: return PolicyActionKind.INSPECT;
    case FLAG:    return PolicyActionKind.FLAG;
    default:
      throw new IllegalArgumentException("unmapped policy action kind: " + a);
    }
  }

  private static PolicyLiteralKind
      toThriftLiteralKind(final ParsedPolicy.LiteralKind k) {
    switch (k) {
    case INT:    return PolicyLiteralKind.INT;
    case LONG:   return PolicyLiteralKind.LONG;
    case STRING:
    default:     return PolicyLiteralKind.STRING;
    }
  }

  private static String joinPolicies(final List<String> policies) {
    if (policies == null || policies.isEmpty()) {
      return "[]";
    }
    final StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < policies.size(); i++) {
      if (i > 0) sb.append(',');
      sb.append('"').append(policies.get(i)).append('"');
    }
    return sb.append(']').toString();
  }

  private void analyzeAnonymizeTable(final ASTNode ast) throws SemanticException {
    String tableName = ast.getChild(0).getChild(0).getText();
    requirePrivilege(PRIV_ERASE, tableName);
    int childCount = ast.getChild(1).getChildCount();
    List<String> pidList = new ArrayList<>();
    for (int i = 0; i < childCount; i++) {
      String pid = ast.getChild(1).getChild(i).getText();
      pidList.add(pid);
    }
    Table table = getTable(tableName);
    String dbName = table.getDbName();
    String tblName = table.getTableName();
    short max = -1;

    boolean indexFound = false;
    String indexPath = null;
    Index index2 = null;
    try {
      List<Index> indexes = db.getIndexes(dbName, tblName, max);
      for (Index index : indexes) {
        if (index.isSetIndexType()) {
          indexFound = true;
          String ixTableName = index.getIndexTableName();
          Table ixTable = getTable(ixTableName);
          indexPath = ixTable.getSd().getLocation();
          index2 = index;
          break;
        }
      }
    } catch (HiveException e) {
      throw new SemanticException(e);
    }

    final long erasureRunStartedTs = System.currentTimeMillis();
    final long erasureRunTblId = (table.getTTable() != null && table.getTTable().isSetId())
        ? table.getTTable().getId() : 0L;

    final long erasureRunId = erasureRunStartedTs;
    try {
      db.acquireErasureRunLock(erasureRunTblId, erasureRunId, currentPrincipal());
    } catch (HiveException he) {
      final StringBuilder msg = new StringBuilder("ERASE FROM TABLE ")
          .append(tblName).append(" refused: a prior erasure run still holds the table lock");
      try {
        final ErasureRunLock lock =
            db.getErasureRunLock(erasureRunTblId);
        if (lock != null) {
          final long ageSec = Math.max(0L,
              (System.currentTimeMillis() - lock.getStartedTs()) / 1000L);
          msg.append(", held by principal '").append(lock.getPrincipal())
             .append("' since ").append(new Date(lock.getStartedTs()))
             .append(" (").append(ageSec).append("s ago), runId=").append(lock.getRunId());
        }
      } catch (HiveException ignored) {
        msg.append(" (").append(he.getMessage()).append(')');
      }
      msg.append(". A run that finishes normally releases this lock automatically; a lingering "
              + "lock means the prior run is still in flight or terminated uncleanly. Inspect it "
              + "with 'SHOW ERASURE LOCK ON TABLE ").append(tblName)
         .append("'. If that run is confirmed dead, free it with 'RELEASE ERASURE LOCK ON TABLE ")
         .append(tblName).append(" WITH REASON \"...\"'.");
      throw new SemanticException(msg.toString());
    }
    ensureCompletionHookRegistered();
    try {
    try {
      final ErasureRunAudit run = new ErasureRunAudit();
      run.setTblId(erasureRunTblId);
      run.setColumnName("");
      run.setBindingId(0L);
      run.setPrincipal(currentPrincipal());
      run.setStartedTs(erasureRunStartedTs);
      run.setStatus(ErasureRunStatus.INITIATED);
      run.setIdentityValues(String.join(",", pidList));
      db.recordErasureRun(run);
      try {
        final SessionState ss =
            SessionState.get();
        if (ss != null) {
          ss.getHiveVariables().put("anon.run.tblId", Long.toString(erasureRunTblId));
          ss.getHiveVariables().put("anon.run.startedTs", Long.toString(erasureRunStartedTs));
          ss.getHiveVariables().put("anon.run.runId", Long.toString(erasureRunId));
        }
      } catch (Throwable t) {
      }
      LOG.info("ERASE FROM TABLE {} pinned at started_ts={} principal={}",
          tblName, erasureRunStartedTs, run.getPrincipal());
    } catch (HiveException he) {
      LOG.warn("recordErasureRun failed for {}: {}", tblName, he.getMessage());
    }

    String rowLocatorName  = null;
    String bodyColumnName  = null;
    String schemaColumnName = null;
    String boundPolicyName = null;
    String policyJson      = null;
    ColumnInternalFormat columnFormat = null;
    ErasurePolicyBinding boundBinding = null;
    List<String> boundMemberNames = null;

    final long tblIdLocal = (table.getTTable() != null
        && table.getTTable().isSetId()) ? table.getTTable().getId() : 0L;
    try {
      for (final StructField sf : table.getFields()) {
        final String maybeCol = sf.getFieldName();
        final ErasurePolicyBinding b
            = db.getErasurePolicyBinding(tblIdLocal, maybeCol);
        if (b == null) continue;
        if (b.isSetSchemaField() && b.isSetRowLocator()
            && b.isSetColumnFormat() && b.isSetColumnName()) {
          final List<String> memberNames = new ArrayList<>();
          for (final ErasurePolicyBindingMember m
              : db.getBindingMembers(b.getBindingId())) {
            final ErasurePolicy ep = lookupPolicyById(m.getPolicyId());
            if (ep == null) {
              throw new SemanticException("ERASE FROM TABLE: binding member with "
                  + "policy id " + m.getPolicyId() + " on " + tblName
                  + " does not resolve to a policy; DETACH the binding and "
                  + "re-ATTACH the surviving policies.");
            }
            if (!memberNames.contains(ep.getPolicyName())) {
              memberNames.add(ep.getPolicyName());
            }
          }
          if (memberNames.isEmpty()) continue;
          if (bodyColumnName == null) {
            rowLocatorName   = b.getRowLocator();
            bodyColumnName   = b.getColumnName();
            schemaColumnName = b.getSchemaField();
            columnFormat     = b.getColumnFormat();
            boundPolicyName  = memberNames.get(0);
            boundBinding     = b;
            boundMemberNames = memberNames;
          } else if (!b.getColumnName().equalsIgnoreCase(bodyColumnName)) {
            throw new SemanticException("ERASE FROM TABLE: table " + tblName
                + " has erasure-policy bindings on more than one column ('"
                + bodyColumnName + "' and '" + b.getColumnName()
                + "'). All policies attached to a table must target the same "
                + "column. Detach one binding before erasing.");
          }
        }
      }
    } catch (SemanticException se) {
      throw se;
    } catch (HiveException he) {
      throw new SemanticException("ERASE FROM TABLE: binding lookup failed for "
          + tblName + ": " + he.getMessage());
    }

    if (boundPolicyName == null || bodyColumnName == null) {
      throw new SemanticException("ERASE FROM TABLE: no erasure-policy binding "
          + "found for table " + tblName + ". Run ATTACH DATA ERASURE POLICY "
          + "<name> ON TABLE " + tblName + " COLUMN <col> WITH (...) RESOLUTION "
          + "(EXPLICIT) first.");
    }

    ErasurePolicyVersion activeForErase = null;
    try {
      activeForErase = db.getActiveErasurePolicyVersion(boundPolicyName);
      if (activeForErase == null) {
        throw new SemanticException("ERASE FROM TABLE refused. Policy '"
            + boundPolicyName + "' bound to " + tblName
            + " has no ACTIVE version. Run 'ACTIVATE ERASURE POLICY "
            + boundPolicyName + "' (after LOAD + VALIDATE if no validated "
            + "version exists yet).");
      }
    } catch (HiveException he) {
      throw new SemanticException("ERASE FROM TABLE: active-version lookup failed for '"
          + boundPolicyName + "': " + he.getMessage());
    }
    if (boundMemberNames != null && boundMemberNames.size() > 1) {
      policyJson = composeBindingUnionJson(boundBinding, boundMemberNames,
          "ERASE FROM TABLE");
    } else {
      try {
        policyJson = composeActiveVersionJson(boundPolicyName);
      } catch (HiveException he) {
        throw new SemanticException("ERASE FROM TABLE: active-version "
            + "policy reconstruction failed for '" + boundPolicyName + "': "
            + he.getMessage());
      }
    }
    if (policyJson == null) {
      throw new SemanticException("ERASE FROM TABLE refused. Policy '"
          + boundPolicyName + "' bound to " + tblName
          + " has no ACTIVE version with stored source; ACTIVATE a validated "
          + "version / re-LOAD the policy first.");
    }

    List<StructField> fields = table.getFields();
    final List<String> colNames = new ArrayList<>();
    final List<String> colTypes = new ArrayList<>();
    int msgIdIx = -1;
    int msgOffsetIx = -1;
    int bodyIx = -1;
    String msgIdType = "";
    String offsetType = "";

    for (StructField field : fields) {
      String fldName = field.getFieldName();
      if (fldName.equals(rowLocatorName)) {
        msgOffsetIx = field.getFieldID();
        offsetType = field.getFieldObjectInspector().getTypeName();
      }

      if (fldName.equals(bodyColumnName)) {
        bodyIx = field.getFieldID();
      }

      if (fldName.equals(schemaColumnName)) {
        msgIdIx = field.getFieldID();
        msgIdType = field.getFieldObjectInspector().getTypeName();
      }

      colNames.add(fldName);
      ObjectInspector oi = field.getFieldObjectInspector();
      colTypes.add(oi.getTypeName());
    }

    String valueTypes = "T" + getType(offsetType) + getType(msgIdType);

    String inputDir = table.getPath().toString();
    String tmpDir = inputDir;
    String json = policyJson;
    DataErasurePolicy erasurePolicy = DataErasurePolicy.fromString(json);

    String keyType = getType(erasurePolicy.identityFieldType);

    conf.setStrings(IOConstants.COLUMNS, String.join(",", colNames));
    conf.setStrings(IOConstants.COLUMNS_TYPES, String.join(",", colTypes));
    FileType fileType = getFileType(table);
    conf.set(ANON_FILE_TYPE, fileType.name());

    inputs.add(new ReadEntity(table));

    if (index2 != null) {
      final int pageSize = index2.getPageSize();
      final int bufferSize = index2.getBufferPoolSize() * pageSize;
      final int headerSize = 64;
      conf.setInt(BTREE_CONF_BUFFER_SIZE, bufferSize);
      conf.setInt(BTREE_CONF_PAGE_SIZE, pageSize);
      conf.setInt(BTREE_CONF_PAGE_HEADER_SIZE, headerSize);
      conf.setInt(BTREE_CONF_DUMP_BYTES_PER_ROW, pageSize);
      if (index2.getIndexType() != IndexType.TABULAR) {
        conf.set(INDEX_ADDR_TYPE, getAddrType(index2));
      }
      conf.set(INDEX_KEY_TYPE, keyType);
      conf.set(INDEX_VALUE_TYPES, valueTypes);
      conf.set(ANON_INDEX_TYPE, index2.getIndexType().toString());
    }

    conf.setBoolean(ANON_INDEX_FOUND, indexFound);
    conf.setInt(ANON_MSG_ID_IX, msgIdIx);
    conf.setInt(ANON_MSG_OFFSET_IX, msgOffsetIx);
    conf.setInt(ANON_BODY_IX, bodyIx);
    conf.set(ANON_PID_LIST, String.join(",", pidList));
    conf.set(ANON_POLICY_DOC, json);

    conf.set(ANON_COLUMN_INTERNAL_FORMAT, columnFormat.name());

    AnonWork anonWork = new AnonWork(indexFound);
    anonWork.setName(AnonConst.ANON_VERTEX_ANONYMIZER);
    anonWork.setInputDir(inputDir);
    anonWork.setTmpDir(tmpDir);
    anonWork.setNumReducers(1);

    String queryId = queryState.getQueryId();
    TezWork tezWork = new TezWork(queryId, conf);

    tezWork.add(anonWork);

    if (indexFound) {
      IndexWork indexWork = new IndexWork(indexPath);
      indexWork.setName(AnonConst.ANON_EDGE_INDEX);
      tezWork.add(indexWork);
      TezEdgeProperty edgeProp2 = new TezEdgeProperty(TezEdgeProperty.EdgeType.SIMPLE_EDGE);
      tezWork.connect(indexWork, anonWork, edgeProp2);
    }

    Task<? extends Serializable> tezTask = TaskFactory.get(tezWork, conf);
    rootTasks.add(tezTask);
    } catch (SemanticException | RuntimeException e) {
      releaseErasureRunLockOnAbort(erasureRunTblId, erasureRunId, erasureRunStartedTs);
      throw e;
    }
  }

  private void ensureCompletionHookRegistered() {
    final String hook = ErasureRunCompletionHook.class.getName();
    appendHook("hive.exec.post.hooks", hook);
    appendHook("hive.exec.failure.hooks", hook);
  }

  private void appendHook(final String confName, final String hookClass) {
    final String cur = conf.get(confName);
    if (cur == null || cur.trim().isEmpty()) {
      conf.set(confName, hookClass);
    } else if (!Arrays.asList(cur.split("\\s*,\\s*")).contains(hookClass)) {
      conf.set(confName, cur + "," + hookClass);
    }
  }

  private void releaseErasureRunLockOnAbort(final long tblId, final long runId, final long startedTs) {
    try {
      db.updateErasureRunCompletion(tblId, startedTs, System.currentTimeMillis(),
          ErasureRunStatus.FAILED, 0L, 0L, 0L);
    } catch (Exception ignored) {
    }
    try {
      db.completeErasureRunLock(tblId, runId);
    } catch (Exception ignored) {
    }
  }

  private FileType getFileType(final Table table) {
    final String ifName = table.getTTable().getSd().getInputFormat();
    final String ofName = table.getTTable().getSd().getOutputFormat();

    if (ifName.equals(MapredParquetInputFormat.class.getName())) {
      return FileType.PARQUET;
    }
    if (ifName.equals(OrcInputFormat.class.getName())) {
      return FileType.ORC;
    }
    throw new RuntimeException("unsupported format: " + ifName);
  }

  private static void validateErasurePolicySource(final String policyName,
                                                  final String source)
      throws SemanticException {
    ValidationResult result = ErasurePolicyValidator.validate(source);
    if (!result.isValid()) {
      String detail = result.getErrors().stream()
          .map(ValidationError::toString)
          .collect(Collectors.joining("; "));
      throw new SemanticException(
          "Invalid erasure policy '" + policyName + "': " + detail);
    }
  }

  public static String loadAndValidatePolicyFile(final String policyName,
                                                 final String filePath)
      throws SemanticException {
    Path resolved = Paths.get(filePath);
    final String source;
    try {
      source = new String(Files.readAllBytes(resolved));
    } catch (IOException ioe) {
      throw new SemanticException(
          "Failed to read erasure policy file '" + filePath + "': "
              + ioe.getMessage(), ioe);
    }
    validateErasurePolicySource(policyName, source);
    return source;
  }

  private void analyzeDropErasurePolicy(final ASTNode ast) throws SemanticException {
    final String policyName = ast.getChild(0).getText();
    final boolean ifExists = (ast.getFirstChildWithType(HiveParser.TOK_IFEXISTS) != null);
    requirePrivilege(PRIV_POLICY_MANAGE, policyName);

    final List<ErasurePolicyVersion> versions;
    try {
      versions = db.listErasurePolicyVersions(policyName);
    } catch (HiveException he) {
      if (ifExists) {
        return;
      }
      throw new SemanticException(
          "DROP DATA ERASURE POLICY: version-listing failed for '" + policyName + "': "
              + he.getMessage());
    }
    if (versions == null || versions.isEmpty()) {
    } else {
      final List<String> nonDraftLabels = new ArrayList<>();
      for (final ErasurePolicyVersion v : versions) {
        if (v.getStatus() != PolicyVersionStatus.DRAFT) {
          nonDraftLabels.add(v.getVersionLabel() + "(" + v.getStatus() + ")");
        }
      }
      if (!nonDraftLabels.isEmpty()) {
        throw new SemanticException(
            "DROP DATA ERASURE POLICY: '" + policyName + "' has versions in non-DRAFT "
                + "states " + nonDraftLabels + ". DROP is restricted to policies that "
                + "have never been validated so the §4.4 rows-never-deleted invariant "
                + "holds for audit-relevant state. Use DEACTIVATE ERASURE POLICY "
                + policyName + " for operational retirement.");
      }
    }

    try {
      db.dropErasurePolicy(policyName, !ifExists);
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  public static String getIndexTableName(final TableName qualifiedTableName, final String indexName, final String indexedColumn) {
    return qualifiedTableName.getDb() + ".IX__" + qualifiedTableName.getTable() + "__" + indexName + "__" + indexedColumn;
  }

  public static String getIndexDirName(final TableName qualifiedTableName, final String indexName, final String indexedColumn) {
    return ("IX__" + qualifiedTableName.getTable() + "__" + indexName + "__" + indexedColumn).toLowerCase();
  }

  private void analyzeCreateIndex(final ASTNode ast) throws SemanticException {
    final String indexName = ast.getChild(0).getText();
    final TableName qualifiedTableName = getQualifiedTableName((ASTNode) ast.getChild(1));
    final String indexedColumn = ast.getChild(2).getText();
    final ASTNode indexTypeNode = (ASTNode) ast.getChild(3);
    final boolean ifNotExists = (ast.getFirstChildWithType(HiveParser.TOK_IFNOTEXISTS) != null);

    final String tableName = qualifiedTableName.getDb() + "." + qualifiedTableName.getTable();
    requirePrivilege(PRIV_POLICY_MANAGE, tableName);
    int pageSize = 0;
    int bufferPoolSize = 0;
    String pointerType = "";

    IndexType indexType;
    String inputFormat;
    String outputFormat;
    switch (indexTypeNode.getType()) {
      case HiveParser.TOK_BTREE:
        indexType = IndexType.BTREE;
        inputFormat = BTREE_INPUT;
        outputFormat = BTREE_OUTPUT;
        pageSize = Integer.parseInt(indexTypeNode.getChild(0).getText());
        bufferPoolSize = Integer.parseInt(indexTypeNode.getChild(1).getText());
        pointerType = getPointerType((ASTNode) indexTypeNode.getChild(2));
        break;
      case HiveParser.TOK_DIRECTORY:
        indexType = IndexType.DIRECTORY;
        inputFormat = DIRECTORY_INPUT;
        outputFormat = DIRECTORY_OUTPUT;
        pointerType = getPointerType((ASTNode) indexTypeNode.getChild(0));
        break;
      case HiveParser.TOK_TABULAR:
        indexType = IndexType.TABULAR;
        inputFormat = TABULAR_INPUT;
        outputFormat = TABULAR_OUTPUT;
        break;
      default:
        throw new SemanticException("Unknown index type: " + indexTypeNode.getText());
    }

    try {
      final String indexTableName = getIndexTableName(qualifiedTableName, indexName, indexedColumn);
      final String serde = IndexSerde.class.getName();
      final String location = null;
      final String storageHandler = "";
      db.createIndex(tableName, indexName, indexedColumn, indexTableName, inputFormat, outputFormat, serde, location,
        storageHandler, pageSize, bufferPoolSize, pointerType, indexType, ifNotExists);
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private String getPointerType(final ASTNode ast) throws SemanticException {
    switch (ast.getType()) {
      case HiveParser.TOK_INT:
        return "int";
      case HiveParser.TOK_BIGINT:
        return "bigint";
      default:
        throw new SemanticException("Unknown pointer type: " + ast.getText());
    }
  }

  private void analyzeDropIndex(final ASTNode ast) throws SemanticException {
    final String indexName = ast.getChild(0).getText();
    final TableName qualifiedTableName = getQualifiedTableName((ASTNode) ast.getChild(1));
    final String tableName = qualifiedTableName.getDb() + "." + qualifiedTableName.getTable();
    requirePrivilege(PRIV_POLICY_MANAGE, tableName);
    boolean ifExists = (ast.getFirstChildWithType(HiveParser.TOK_IFEXISTS) != null);
    boolean throwException = !ifExists;
    try {
      db.dropIndex(tableName, indexName, throwException, true);
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private void partTest(final Table table) {
    if (table.isPartitioned()) {
      PartitionIterable partitions = null;
      try {
        partitions = new PartitionIterable(db, table, null, MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX));
      } catch (HiveException e) {
        e.printStackTrace();
      }
      for (Partition p : partitions) {
        int id = 1;
      }
    }
  }


  private static final class BindingOpts {
    final String schemaField;
    final String rowLocator;
    final String columnFormat;
    final String resolutionMode;

    BindingOpts(String sf, String rl, String fmt, String res) {
      this.schemaField = sf;
      this.rowLocator = rl;
      this.columnFormat = fmt;
      this.resolutionMode = res;
    }

    @Override
    public String toString() {
      return "schemaField=" + schemaField + ", rowLocator=" + rowLocator
          + ", columnFormat=" + columnFormat + ", resolutionMode=" + resolutionMode;
    }
  }

  private static final class TimeRange {
    final String fromTs;
    final String untilTs;

    TimeRange(String fromTs, String untilTs) {
      this.fromTs = fromTs;
      this.untilTs = untilTs;
    }

    @Override
    public String toString() {
      return "from=" + (fromTs == null ? "*" : fromTs)
          + ", until=" + (untilTs == null ? "*" : untilTs);
    }
  }

  private static long parseTsOrDefault(final String s, final long fallback) {
    if (s == null || s.isEmpty()) {
      return fallback;
    }
    try {
      return Long.parseLong(s);
    } catch (NumberFormatException nfe) {
      return fallback;
    }
  }

  private void analyzeExplainAttachPolicy(final ASTNode ast) throws SemanticException {
    final List<String> policyNames = extractPolicyList((ASTNode) ast.getChild(0));
    final TableName tableName      = getQualifiedTableName((ASTNode) ast.getChild(1));
    final String columnName        = ast.getChild(2).getText();
    requirePrivilege(PRIV_POLICY_VALIDATE, qualified(tableName));
    final BindingOpts opts         = (ast.getChildCount() >= 4)
        ? extractBindingOpts((ASTNode) ast.getChild(3))
        : null;

    final Map<String, ParsedPolicy> attached = new LinkedHashMap<>();
    for (String pn : policyNames) {
      attached.put(pn, loadAndParsePolicy(pn));
    }

    final ResolutionMode mode = (opts != null && "STRICTEST".equals(opts.resolutionMode))
        ? ResolutionMode.STRICTEST
        : ResolutionMode.EXPLICIT;

    final Report report = PolicyConflictDetector.analyse(attached, mode);

    final StringBuilder out = new StringBuilder();
    out.append("EXPLAIN ATTACH on ").append(qualified(tableName)).append('.').append(columnName);
    out.append(" with policies ").append(policyNames);
    out.append(" under RESOLUTION ").append(mode.name()).append('.').append('\n');
    appendReport(out, report, mode);
    console.printInfo(out.toString());
  }

  private void analyzeValidateErasureBinding(final ASTNode ast) throws SemanticException {
    final TableName tableName = getQualifiedTableName((ASTNode) ast.getChild(0));
    final String columnName   = ast.getChild(1).getText().toLowerCase();
    requirePrivilege(PRIV_POLICY_VALIDATE, qualified(tableName) + "." + columnName);

    final long tblId = resolveTableId(tableName);
    ErasurePolicyBinding newBinding = null;
    if (tblId > 0L) {
      try {
        newBinding = db.getErasurePolicyBinding(tblId, columnName);
      } catch (HiveException ignored) {  }
    }
    if (newBinding == null) {
      throw new SemanticException("VALIDATE ERASURE BINDING: column "
          + qualified(tableName) + "." + columnName + " has no attached policy.");
    }

    final Map<String, ParsedPolicy> attached = new LinkedHashMap<>();
    final ResolutionMode mode = (newBinding.getResolutionMode() != null
            && "STRICTEST".equals(newBinding.getResolutionMode().name()))
        ? ResolutionMode.STRICTEST
        : ResolutionMode.EXPLICIT;
    final List<ErasurePolicyBindingMember> members;
    try {
      members = db.getBindingMembers(newBinding.getBindingId());
    } catch (HiveException he) {
      throw new SemanticException("VALIDATE ERASURE BINDING: member lookup failed: "
          + he.getMessage());
    }
    if (members == null || members.isEmpty()) {
      throw new SemanticException("VALIDATE ERASURE BINDING: binding "
          + newBinding.getBindingId() + " has no attached policies.");
    }
    for (final ErasurePolicyBindingMember m : members) {
      final ErasurePolicy ep = lookupPolicyById(m.getPolicyId());
      if (ep == null) {
        throw new SemanticException("VALIDATE ERASURE BINDING: policy id "
            + m.getPolicyId() + " not found.");
      }
      attached.put(ep.getPolicyName(), parseActivePolicy(ep.getPolicyName()));
    }

    final Report report = PolicyConflictDetector.analyse(attached, mode);

    final StringBuilder out = new StringBuilder();
    out.append("VALIDATE ERASURE BINDING on ").append(qualified(tableName)).append('.').append(columnName);
    out.append(": ").append(attached.size()).append(" policy(ies) attached: ")
       .append(String.join(", ", attached.keySet())).append('\n');
    appendReport(out, report, mode);
    console.printInfo(out.toString());
  }

  private void analyzeAuditErasurePolicy(final ASTNode ast) throws SemanticException {
    final String policyName = ast.getChild(0).getText();
    requirePrivilege(PRIV_POLICY_VALIDATE, policyName);
    final TimeRange range   = (ast.getChildCount() >= 2)
        ? extractTimeRange((ASTNode) ast.getChild(1))
        : null;

    final ErasurePolicy policy;
    try {
      policy = db.getErasurePolicy(policyName);
    } catch (HiveException he) {
      throw new SemanticException("AUDIT ERASURE POLICY: lookup failed: " + he.getMessage());
    }
    if (policy == null) {
      throw new SemanticException("AUDIT ERASURE POLICY: policy '" + policyName + "' not found.");
    }

    final StringBuilder out = new StringBuilder();
    out.append("AUDIT ERASURE POLICY ").append(policyName);
    if (range != null) {
      out.append(" (filter ").append(range).append(", not yet applied -- ");
      out.append("requires ERASURE_POLICY_LIFECYCLE_EVENT history table)");
    }
    out.append('\n');
    out.append("Current state:\n");
    out.append("  policy name : ").append(policy.getPolicyName()).append('\n');
    String auditBody;
    try {
      auditBody = activePolicySource(policyName);
    } catch (HiveException he) {
      throw new SemanticException("AUDIT ERASURE POLICY: active-version lookup failed: "
          + he.getMessage());
    }
    if (auditBody == null || auditBody.isEmpty()) {
      out.append("  body length : (no active version)\n");
      out.append("  body preview: (no active version)\n");
    } else {
      out.append("  body length : ").append(auditBody.length()).append(" chars\n");
      out.append("  body preview: ").append(snippet(auditBody)).append('\n');
    }
    out.append("Note: lifecycle history (VALIDATE/ACTIVATE/DEACTIVATE/SUPERSEDE events) is\n");
    out.append("      not yet recorded; this report shows the policy's current state only.\n");
    emitAuditResult(out.toString());
  }

  private void analyzeAuditErasureBinding(final ASTNode ast) throws SemanticException {
    final TableName tableName = getQualifiedTableName((ASTNode) ast.getChild(0));
    final String columnName   = ast.getChild(1).getText().toLowerCase();
    requirePrivilege(PRIV_POLICY_VALIDATE, qualified(tableName) + "." + columnName);
    final TimeRange range     = (ast.getChildCount() >= 3)
        ? extractTimeRange((ASTNode) ast.getChild(2))
        : null;

    final long tblId = resolveTableId(tableName);
    ErasurePolicyBinding newBinding = null;
    if (tblId > 0L) {
      try {
        newBinding = db.getErasurePolicyBinding(tblId, columnName);
      } catch (HiveException ignored) {
      }
    }

    final StringBuilder out = new StringBuilder();
    out.append("AUDIT ERASURE BINDING on ").append(qualified(tableName)).append('.').append(columnName);
    if (range != null) {
      out.append(" (filter ").append(range)
         .append(", not yet applied -- requires ERASURE_POLICY_LIFECYCLE_EVENT history table)");
    }
    out.append('\n');

    if (newBinding != null) {
      out.append("Current state (").append("§5 binding").append("):\n");
      out.append("  binding id    : ").append(newBinding.getBindingId()).append('\n');
      out.append("  schema field  : ").append(newBinding.getSchemaField()).append('\n');
      out.append("  row locator   : ").append(newBinding.getRowLocator()).append('\n');
      if (newBinding.getColumnFormat() != null) {
        out.append("  column format : ").append(newBinding.getColumnFormat().name()).append('\n');
      }
      if (newBinding.getResolutionMode() != null) {
        out.append("  resolution    : ").append(newBinding.getResolutionMode().name()).append('\n');
      }
      List<ErasurePolicyBindingMember> members = null;
      try {
        members = db.getBindingMembers(newBinding.getBindingId());
      } catch (HiveException he) {
        out.append("  members       : <lookup failed: ").append(he.getMessage()).append(">\n");
      }
      if (members != null && !members.isEmpty()) {
        out.append("  policies      : ").append(members.size()).append(" attached\n");
        for (final ErasurePolicyBindingMember m : members) {
          out.append("    - policy_id=").append(m.getPolicyId())
             .append(" ordinal=").append(m.getOrdinal()).append('\n');
        }
      }
    } else {
      out.append("Current state: no policy attached.\n");
    }
    out.append("Note: attach/detach history is not yet recorded; this report shows the\n");
    out.append("      current binding state only.\n");
    emitAuditResult(out.toString());
  }

  private void analyzeAuditErasureExecutions(final ASTNode ast) throws SemanticException {
    final TableName tableName = getQualifiedTableName((ASTNode) ast.getChild(0));
    requirePrivilege(PRIV_POLICY_VALIDATE, qualified(tableName));

    TimeRange range = null;
    String byUser = null;
    String forIdentity = null;
    for (int i = 1; i < ast.getChildCount(); i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      if (child.getType() == HiveParser.TOK_TIME_RANGE) {
        range = extractTimeRange(child);
      } else if (range == null && byUser == null && child.getType() == HiveParser.Identifier) {
        byUser = child.getText();
      } else if (byUser != null && forIdentity == null) {
        forIdentity = unquoteIfStringLiteral(child.getText());
      } else if (forIdentity == null) {
        forIdentity = unquoteIfStringLiteral(child.getText());
      }
    }

    final Table tbl;
    try {
      tbl = db.getTable(tableName.getDb(), tableName.getTable(), false);
    } catch (HiveException he) {
      throw new SemanticException("AUDIT ERASURE EXECUTIONS: table lookup failed: "
          + he.getMessage());
    }
    if (tbl == null) {
      throw new SemanticException("AUDIT ERASURE EXECUTIONS: table not found: "
          + qualified(tableName));
    }
    final long tblId = tbl.getTTable().getId();
    final long fromTs = parseTsOrDefault(range == null ? null : range.fromTs, Long.MIN_VALUE);
    final long untilTs = parseTsOrDefault(range == null ? null : range.untilTs, Long.MAX_VALUE);

    final List<ErasureRunAudit> runs;
    try {
      runs = db.getErasureRunsForTable(tblId, fromTs, untilTs, byUser, forIdentity);
    } catch (HiveException he) {
      throw new SemanticException("AUDIT ERASURE EXECUTIONS: query failed: "
          + he.getMessage());
    }

    final StringBuilder out = new StringBuilder();
    out.append("AUDIT ERASURE EXECUTIONS on ").append(qualified(tableName))
       .append(" (range=").append(range == null ? "<all-time>" : "[" + range + "]")
       .append(", byUser=").append(byUser == null ? "<any>" : byUser)
       .append(", forIdentity=").append(forIdentity == null ? "<any>" : forIdentity)
       .append(")\n");
    if (runs == null || runs.isEmpty()) {
      out.append("No erasure executions recorded for this table in the requested range.\n");
    } else {
      out.append(runs.size()).append(" execution(s) found:\n");
      for (final ErasureRunAudit r : runs) {
        out.append("  runId=").append(r.getRunId())
           .append(" startedTs=").append(r.getStartedTs());
        if (r.isSetCompletedTs()) {
          out.append(" completedTs=").append(r.getCompletedTs());
        }
        out.append(" principal=").append(r.getPrincipal())
           .append(" status=").append(r.getStatus());
        final boolean isReleaseRow =
            r.isSetStatus()
            && (r.getStatus()
                == ErasureRunStatus.RELEASED
              || r.getStatus()
                == ErasureRunStatus.FORCE_RELEASED);
        if (isReleaseRow) {
          if (r.isSetReleaseReason()) {
            out.append(" reason='").append(r.getReleaseReason()).append('\'');
          }
        } else {
          if (r.isSetFilesRewritten()) {
            out.append(" filesRewritten=").append(r.getFilesRewritten());
          }
          if (r.isSetMatchesInspected()) {
            out.append(" matchesInspected=").append(r.getMatchesInspected());
          }
          if (r.isSetMatchesRedacted()) {
            out.append(" matchesRedacted=").append(r.getMatchesRedacted());
          }
          if (r.isSetMatchesFlagged()) {
            out.append(" matchesFlagged=").append(r.getMatchesFlagged());
          }
        }
        out.append('\n');
      }
    }
    emitAuditResult(out.toString());
  }

  private void analyzeAuditErasureConflicts(final ASTNode ast) throws SemanticException {
    requirePrivilege(PRIV_POLICY_VALIDATE, "*");
    final TimeRange range = (ast.getChildCount() >= 1)
        ? extractTimeRange((ASTNode) ast.getChild(0))
        : null;
    final long fromTs  = parseTsOrDefault(range == null ? null : range.fromTs,  Long.MIN_VALUE);
    final long untilTs = parseTsOrDefault(range == null ? null : range.untilTs, Long.MAX_VALUE);

    final List<ErasurePolicyLifecycleEvent> rows;
    try {
      rows = db.getAttachRejectedEvents(fromTs, untilTs);
    } catch (HiveException he) {
      throw new SemanticException("AUDIT ERASURE CONFLICTS: lookup failed: "
          + he.getMessage());
    }

    final StringBuilder out = new StringBuilder();
    out.append("AUDIT ERASURE CONFLICTS (range=")
       .append(range == null ? "<all-time>" : "[" + range + "]")
       .append(")\n");
    if (rows == null || rows.isEmpty()) {
      out.append("  No ATTACH_REJECTED events recorded in the requested range.\n");
    } else {
      out.append("  ").append(rows.size()).append(" event(s):\n");
      for (final ErasurePolicyLifecycleEvent r : rows) {
        out.append("    eventTs=").append(r.getEventTs())
           .append(" versionId=").append(r.getVersionId())
           .append(" principal=").append(r.getPrincipal())
           .append('\n');
        if (r.isSetNote() && r.getNote() != null && !r.getNote().isEmpty()) {
          for (final String line : r.getNote().split("\n")) {
            out.append("      ").append(line).append('\n');
          }
        }
      }
    }
    emitAuditResult(out.toString());
    LOG.info("AUDIT ERASURE CONFLICTS -> {} event(s)",
        rows == null ? 0 : rows.size());
  }

  private void analyzeAuditErasureCompliance(final ASTNode ast) throws SemanticException {
    requirePrivilege(PRIV_POLICY_VALIDATE, "*");
    String asOf = null;
    if (ast.getChildCount() >= 1) {
      ASTNode asOfNode = (ASTNode) ast.getChild(0);
      if (asOfNode.getType() == HiveParser.TOK_AS_OF) {
        asOf = unquoteIfStringLiteral(asOfNode.getChild(0).getText());
      }
    }

    final List<PolicyInfo> all;
    try {
      all = db.getAllErasurePolicyNames();
    } catch (HiveException he) {
      throw new SemanticException("AUDIT ERASURE COMPLIANCE: policy enumeration failed: "
          + he.getMessage());
    }

    final StringBuilder out = new StringBuilder();
    out.append("AUDIT ERASURE COMPLIANCE");
    if (asOf != null) {
      out.append(" (asOf=").append(asOf).append(", not yet applied -- ");
      out.append("requires versioned policy history)");
    }
    out.append('\n');
    if (all == null || all.isEmpty()) {
      out.append("No erasure policies currently registered.\n");
    } else {
      out.append("Currently registered policies (").append(all.size()).append("):\n");
      for (PolicyInfo info : all) {
        out.append("  - ").append(info.toString()).append('\n');
      }
    }
    out.append("Note: only current-state policies are reported. Version history,\n");
    out.append("      validation evidence (checksum/principal/timestamp), and table-binding\n");
    out.append("      counts require the ERASURE_POLICY_VERSION and\n");
    out.append("      ERASURE_POLICY_BINDING tables proposed in the paper.\n");
    emitAuditResult(out.toString());
  }


  private void emitAuditResult(final String text) throws SemanticException {
    final org.apache.hadoop.fs.Path resFile = ctx.getLocalTmpPath();
    ctx.setResFile(resFile);
    try {
      final FileSystem fs = resFile.getFileSystem(conf);
      try (final DataOutputStream os = fs.create(resFile, true)) {
        for (final String line : text.split("\n")) {
          os.write(line.replace('\t', ' ').getBytes(StandardCharsets.UTF_8));
          os.write('\n');
        }
      }
    } catch (final IOException e) {
      throw new SemanticException("AUDIT: failed to write result file: " + e.getMessage(), e);
    }
    setFetchTask(createFetchTask("audit_result#string"));
    this.extractResultSchema = Collections.singletonList(
        new FieldSchema(
            "audit_result", "string", "AUDIT report rendered one line per row"));
  }

  private ParsedPolicy loadAndParsePolicy(final String policyName) throws SemanticException {
    final ErasurePolicy policy;
    try {
      policy = db.getErasurePolicy(policyName);
    } catch (HiveException he) {
      throw new SemanticException("policy lookup failed for '" + policyName + "': "
          + he.getMessage());
    }
    if (policy == null) {
      throw new SemanticException("policy '" + policyName + "' not found in Metastore.");
    }
    return parseActivePolicy(policyName);
  }

  private ParsedPolicy parseActivePolicy(final String policyName) throws SemanticException {
    final String body;
    try {
      body = activePolicySource(policyName);
    } catch (HiveException he) {
      throw new SemanticException("active-version lookup failed for '" + policyName
          + "': " + he.getMessage());
    }
    if (body == null || body.trim().isEmpty()) {
      throw new SemanticException("policy '" + policyName + "' has no ACTIVE version "
          + "with stored source; ACTIVATE a validated version / re-LOAD the policy first.");
    }
    return parsePolicyDoc(policyName, body);
  }

  private static ParsedPolicy parsePolicyDoc(final String policyName, final String body)
      throws SemanticException {
    if (body == null || body.trim().isEmpty()) {
      throw new SemanticException("policy '" + policyName + "' has an empty body.");
    }
    try {
      return ErasurePolicyValidator.parse(body);
    } catch (IllegalArgumentException iae) {
      throw new SemanticException("policy '" + policyName + "' failed to parse: "
          + iae.getMessage());
    }
  }

  private static void appendReport(final StringBuilder out,
                                   final Report report,
                                   final ResolutionMode mode) {
    if (report.hasConflicts()) {
      out.append("Conflicts (").append(report.conflicts.size()).append("):\n");
      for (Conflict c : report.conflicts) {
        out.append("  - ").append(c).append('\n');
      }
    } else {
      out.append("Conflicts: none\n");
    }
    if (mode == ResolutionMode.STRICTEST) {
      out.append("Resolved rule set (").append(report.resolvedRules.size()).append(" rules):\n");
      for (ResolvedRule rr : report.resolvedRules) {
        out.append("  - FOR SCHEMA '").append(rr.schemaId).append("' ").append(rr.rule);
        if (rr.resolutionNote != null) {
          out.append("   [").append(rr.resolutionNote).append("]");
        }
        out.append("   (from ").append(rr.contributingPolicies).append(")\n");
      }
    } else if (report.hasConflicts()) {
      out.append("Resolution mode EXPLICIT: ATTACH would be rejected.\n");
    } else {
      out.append("Resolution mode EXPLICIT: ATTACH would succeed.\n");
    }
  }

  private static String snippet(final String s) {
    if (s == null) {
      return "<null>";
    }
    String t = s.trim().replace('\n', ' ');
    return t.length() <= 80 ? t : t.substring(0, 77) + "...";
  }


  private static List<String> extractPolicyList(final ASTNode policyListNode) {
    final List<String> names = new ArrayList<>(policyListNode.getChildCount());
    for (int i = 0; i < policyListNode.getChildCount(); i++) {
      names.add(policyListNode.getChild(i).getText());
    }
    return names;
  }

  private static BindingOpts extractBindingOpts(final ASTNode optsNode) {
    final String schemaField   = optsNode.getChild(0).getText();
    final String rowLocator    = optsNode.getChild(1).getText();
    final String columnFormat  = optsNode.getChild(2).getText();
    final String resolution    = resolutionTokenName(((ASTNode) optsNode.getChild(3)).getType());
    return new BindingOpts(schemaField, rowLocator, formatTokenName(columnFormat), resolution);
  }

  private static TimeRange extractTimeRange(final ASTNode tr) {
    if (tr.getChildCount() == 1) {
      return new TimeRange(null, unquoteIfStringLiteral(tr.getChild(0).getText()));
    }
    final String fromTs  = unquoteIfStringLiteral(tr.getChild(0).getText());
    final String untilTs = unquoteIfStringLiteral(tr.getChild(1).getText());
    return new TimeRange(fromTs, untilTs);
  }

  private static String resolutionTokenName(final int tokenType) {
    if (tokenType == HiveParser.TOK_RESOLUTION_STRICTEST) {
      return "STRICTEST";
    }
    if (tokenType == HiveParser.TOK_RESOLUTION_EXPLICIT) {
      return "EXPLICIT";
    }
    return "<unknown>";
  }

  private static String formatTokenName(final String raw) {
    return raw.startsWith("TOK_") ? raw.substring(4) : raw;
  }

  private static String unquoteIfStringLiteral(final String raw) {
    if (raw != null && raw.length() >= 2
        && raw.charAt(0) == '\'' && raw.charAt(raw.length() - 1) == '\'') {
      return raw.substring(1, raw.length() - 1);
    }
    return raw;
  }

  private static String qualified(final TableName t) {
    return t.getDb() + "." + t.getTable();
  }


  private void analyzeLoadErasurePolicy(final ASTNode ast) throws SemanticException {
    final String policyName = ast.getChild(0).getText();
    final String fromPath = unquoteIfStringLiteral(ast.getChild(1).getText());
    requirePrivilege(PRIV_POLICY_VALIDATE, policyName);

    final byte[] sourceBytes;
    try {
      sourceBytes = Files.readAllBytes(Paths.get(fromPath));
    } catch (IOException ioe) {
      throw new SemanticException("LOAD ERASURE POLICY: cannot read '"
          + fromPath + "': " + ioe.getMessage());
    }
    final String source = new String(sourceBytes, StandardCharsets.UTF_8);
    final String sourceChecksum = sha256Hex(sourceBytes);

    ErasurePolicy existing;
    try {
      existing = db.getErasurePolicy(policyName);
    } catch (HiveException he) {
      throw new SemanticException("LOAD ERASURE POLICY: lookup failed: " + he.getMessage());
    }
    if (existing == null) {
      try {
        db.createErasurePolicy(new ErasurePolicy(policyName), true);
        existing = db.getErasurePolicy(policyName);
      } catch (HiveException | AlreadyExistsException he) {
        throw new SemanticException("LOAD ERASURE POLICY: auto-create of header for '"
            + policyName + "' failed: " + he.getMessage());
      }
      if (existing == null) {
        throw new SemanticException("LOAD ERASURE POLICY: header auto-create for '"
            + policyName + "' completed but lookup still returns null.");
      }
    }

    final String principal = currentPrincipal();
    final String fileVersion = ErasurePolicyValidator.extractVersionLabel(source);
    if (fileVersion == null || fileVersion.isEmpty()) {
      throw new SemanticException("LOAD ERASURE POLICY: the policy file '" + fromPath
          + "' must declare a VERSION header (for example 'VERSION v1').");
    }
    final ParsedPolicy parsedForVersion = ErasurePolicyValidator.validate(source).getPolicy();
    final List<ErasurePolicyVersion> priorVersions;
    try {
      priorVersions = db.listErasurePolicyVersions(policyName);
    } catch (HiveException he) {
      throw new SemanticException("LOAD ERASURE POLICY: version-listing failed for '"
          + policyName + "': " + he.getMessage());
    }
    if (priorVersions != null) {
      for (final ErasurePolicyVersion pv : priorVersions) {
        if (fileVersion.equals(pv.getVersionLabel())) {
          throw new SemanticException("LOAD ERASURE POLICY: version '" + fileVersion
              + "' already exists for policy '" + policyName + "'. Bump the VERSION header in '"
              + fromPath + "', or DROP the existing draft first.");
        }
      }
    }
    final String versionLabel = fileVersion;

    final ErasurePolicyVersion version = new ErasurePolicyVersion();
    version.setPolicyName(policyName);
    version.setVersionLabel(versionLabel);
    version.setStatus(PolicyVersionStatus.DRAFT);
    if (parsedForVersion != null) {
      version.setIdentityFieldName(parsedForVersion.getIdentityFieldName());
      version.setIdentityFieldType(toThriftLiteralKind(parsedForVersion.getIdentityFieldType()));
      version.setSchemaType(toThriftLiteralKind(parsedForVersion.getSchemaType()));
    } else {
      version.setIdentityFieldName("userId");
      version.setIdentityFieldType(PolicyLiteralKind.INT);
      version.setSchemaType(PolicyLiteralKind.STRING);
    }
    version.setSourcePath(fromPath);
    version.setSourceChecksum(sourceChecksum);
    version.setSourceText(source);

    final ErasurePolicyVersion persisted;
    try {
      persisted = db.addErasurePolicyVersion(version);
    } catch (HiveException he) {
      throw new SemanticException("LOAD ERASURE POLICY: failed to persist DRAFT version: "
          + he.getMessage());
    }

    recordLifecycle(persisted.getVersionId(), PolicyLifecycleEventType.LOADED, principal,
        "LOAD ERASURE POLICY " + policyName + " -> " + versionLabel
            + " (DRAFT from " + fromPath + ")");
  }

  private void analyzeValidateErasurePolicy(final ASTNode ast) throws SemanticException {
    final String policyName = ast.getChild(0).getText();
    requirePrivilege(PRIV_POLICY_VALIDATE, policyName);

    final ErasurePolicy existing;
    try {
      existing = db.getErasurePolicy(policyName);
    } catch (HiveException he) {
      throw new SemanticException("VALIDATE ERASURE POLICY: lookup failed: " + he.getMessage());
    }
    if (existing == null) {
      throw new SemanticException("VALIDATE ERASURE POLICY: policy '" + policyName
          + "' not found. Run 'LOAD ERASURE POLICY " + policyName + " FROM ''<path>''' first; "
          + "policies enter the lifecycle through LOAD.");
    }

    final String requestedVersion = (ast.getChildCount() > 1)
        ? unquoteIfStringLiteral(ast.getChild(1).getText()) : null;

    final List<ErasurePolicyVersion> versions;
    try {
      versions = db.listErasurePolicyVersions(policyName);
    } catch (HiveException he) {
      throw new SemanticException("VALIDATE ERASURE POLICY: version listing failed: "
          + he.getMessage());
    }

    if (versions != null) {
      for (final ErasurePolicyVersion v : versions) {
        if (v.getStatus() == PolicyVersionStatus.VALIDATED) {
          throw new SemanticException("VALIDATE ERASURE POLICY: policy '" + policyName
              + "' already has a VALIDATED version '" + v.getVersionLabel()
              + "'. ACTIVATE it, or run 'INVALIDATE ERASURE POLICY " + policyName
              + " VERSION ''" + v.getVersionLabel() + "''' to return it to DRAFT, "
              + "before validating another.");
        }
      }
    }

    final ErasurePolicyVersion draft;
    if (requestedVersion != null) {
      ErasurePolicyVersion match = null;
      if (versions != null) {
        for (final ErasurePolicyVersion v : versions) {
          if (requestedVersion.equals(v.getVersionLabel())) {
            match = v;
            break;
          }
        }
      }
      if (match == null) {
        throw new SemanticException("VALIDATE ERASURE POLICY: policy '" + policyName
            + "' has no version '" + requestedVersion + "'.");
      }
      if (match.getStatus() != PolicyVersionStatus.DRAFT) {
        throw new SemanticException("VALIDATE ERASURE POLICY: version '" + requestedVersion
            + "' of policy '" + policyName + "' is " + match.getStatus()
            + ", not DRAFT; only a DRAFT can be validated.");
      }
      draft = match;
    } else {
      final List<ErasurePolicyVersion> drafts = new ArrayList<>();
      if (versions != null) {
        for (final ErasurePolicyVersion v : versions) {
          if (v.getStatus() == PolicyVersionStatus.DRAFT) {
            drafts.add(v);
          }
        }
      }
      if (drafts.isEmpty()) {
        throw new SemanticException("VALIDATE ERASURE POLICY: policy '" + policyName
            + "' has no DRAFT version to validate. Run 'LOAD ERASURE POLICY "
            + policyName + " FROM ''<path>''' first.");
      }
      if (drafts.size() > 1) {
        final List<String> labels = new ArrayList<>();
        for (final ErasurePolicyVersion v : drafts) {
          labels.add(v.getVersionLabel());
        }
        throw new SemanticException("VALIDATE ERASURE POLICY: policy '" + policyName
            + "' has multiple DRAFT versions " + labels + "; name one with "
            + "'VALIDATE ERASURE POLICY " + policyName + " VERSION ''<label>'''.");
      }
      draft = drafts.get(0);
    }

    final String source = draft.getSourceText();
    if (source == null || source.isEmpty()) {
      throw new SemanticException("VALIDATE ERASURE POLICY: DRAFT version "
          + draft.getVersionLabel() + " of policy '" + policyName + "' has no stored "
          + "source text (legacy row persisted before source-text storage was added). "
          + "Re-run 'LOAD ERASURE POLICY " + policyName + " FROM ''<path>''' to register it.");
    }
    validateErasurePolicySource(policyName, source);

    final String principal = currentPrincipal();
    try {
      db.updateErasurePolicyVersionStatus(draft.getVersionId(),
          PolicyVersionStatus.VALIDATED, principal);
    } catch (HiveException he) {
      throw new SemanticException("VALIDATE ERASURE POLICY: failed to promote DRAFT "
          + draft.getVersionLabel() + ": " + he.getMessage());
    }

    recordLifecycle(draft.getVersionId(), PolicyLifecycleEventType.VALIDATED, principal,
        "VALIDATE ERASURE POLICY " + policyName + " -> " + draft.getVersionLabel()
            + " (promoted from DRAFT)");
  }

  private void analyzeInvalidateErasurePolicy(final ASTNode ast) throws SemanticException {
    final String policyName = ast.getChild(0).getText();
    requirePrivilege(PRIV_POLICY_VALIDATE, policyName);
    final String principal = currentPrincipal();

    final String requestedVersion = (ast.getChildCount() > 1)
        ? unquoteIfStringLiteral(ast.getChild(1).getText()) : null;

    final List<ErasurePolicyVersion> versions;
    try {
      versions = db.listErasurePolicyVersions(policyName);
    } catch (HiveException he) {
      throw new SemanticException("INVALIDATE ERASURE POLICY: version listing failed: "
          + he.getMessage());
    }

    final List<ErasurePolicyVersion> validated = new ArrayList<>();
    if (versions != null) {
      for (final ErasurePolicyVersion v : versions) {
        if (v.getStatus() == PolicyVersionStatus.VALIDATED) {
          validated.add(v);
        }
      }
    }
    if (validated.isEmpty()) {
      throw new SemanticException("INVALIDATE ERASURE POLICY: policy '" + policyName
          + "' has no VALIDATED version to invalidate.");
    }

    final ErasurePolicyVersion target;
    if (requestedVersion != null) {
      ErasurePolicyVersion match = null;
      for (final ErasurePolicyVersion v : validated) {
        if (requestedVersion.equals(v.getVersionLabel())) {
          match = v;
          break;
        }
      }
      if (match == null) {
        throw new SemanticException("INVALIDATE ERASURE POLICY: policy '" + policyName
            + "' has no VALIDATED version '" + requestedVersion + "'.");
      }
      target = match;
    } else if (validated.size() > 1) {
      final List<String> labels = new ArrayList<>();
      for (final ErasurePolicyVersion v : validated) {
        labels.add(v.getVersionLabel());
      }
      throw new SemanticException("INVALIDATE ERASURE POLICY: policy '" + policyName
          + "' has multiple VALIDATED versions " + labels + "; name one with "
          + "'INVALIDATE ERASURE POLICY " + policyName + " VERSION ''<label>'''.");
    } else {
      target = validated.get(0);
    }

    try {
      db.updateErasurePolicyVersionStatus(target.getVersionId(),
          PolicyVersionStatus.DRAFT, principal);
    } catch (HiveException he) {
      throw new SemanticException("INVALIDATE ERASURE POLICY: failed to revert "
          + target.getVersionLabel() + " to DRAFT: " + he.getMessage());
    }

    recordLifecycle(target.getVersionId(), PolicyLifecycleEventType.INVALIDATED, principal,
        "INVALIDATE ERASURE POLICY " + policyName + " -> " + target.getVersionLabel()
            + " (reverted VALIDATED to DRAFT)");
  }

  private void analyzeActivateErasurePolicy(final ASTNode ast) throws SemanticException {
    final String policyName = ast.getChild(0).getText();
    requirePrivilege(PRIV_POLICY_ACTIVATE, policyName);
    final String principal = currentPrincipal();

    final List<ErasurePolicyVersion> versions;
    try {
      versions = db.listErasurePolicyVersions(policyName);
    } catch (HiveException he) {
      throw new SemanticException("ACTIVATE ERASURE POLICY: lookup failed: " + he.getMessage());
    }
    if (versions == null || versions.isEmpty()) {
      throw new SemanticException("ACTIVATE ERASURE POLICY: no validated versions for '" + policyName + "'. Run VALIDATE first.");
    }

    ErasurePolicyVersion candidate = null;
    for (int i = versions.size() - 1; i >= 0; i--) {
      ErasurePolicyVersion v = versions.get(i);
      if (v.getStatus() == PolicyVersionStatus.VALIDATED) {
        candidate = v;
        break;
      }
    }
    if (candidate == null) {
      for (int i = versions.size() - 1; i >= 0; i--) {
        ErasurePolicyVersion v = versions.get(i);
        if (v.getStatus() == PolicyVersionStatus.INACTIVE) {
          candidate = v;
          break;
        }
      }
    }
    if (candidate == null) {
      throw new SemanticException("ACTIVATE ERASURE POLICY: no VALIDATED or INACTIVE "
          + "version of '" + policyName + "' available to activate. Run 'LOAD ERASURE "
          + "POLICY " + policyName + " FROM ''<path>''' followed by 'VALIDATE ERASURE "
          + "POLICY " + policyName + "' first.");
    }

    try {
      db.updateErasurePolicyVersionStatus(candidate.getVersionId(), PolicyVersionStatus.ACTIVE, principal);
    } catch (HiveException he) {
      throw new SemanticException("ACTIVATE ERASURE POLICY: state transition failed: " + he.getMessage());
    }

    for (final ErasurePolicyVersion v : versions) {
      if (v.getStatus() == PolicyVersionStatus.ACTIVE
          && v.getVersionId() != candidate.getVersionId()) {
        recordLifecycle(v.getVersionId(), PolicyLifecycleEventType.SUPERSEDED, principal,
            "ACTIVATE ERASURE POLICY " + policyName
                + ": superseded by " + candidate.getVersionLabel());
      }
    }
    recordLifecycle(candidate.getVersionId(), PolicyLifecycleEventType.ACTIVATED, principal,
        "ACTIVATE ERASURE POLICY " + policyName + " -> " + candidate.getVersionLabel());
  }

  private void analyzeDeactivateErasurePolicy(final ASTNode ast) throws SemanticException {
    final String policyName = ast.getChild(0).getText();
    requirePrivilege(PRIV_POLICY_ACTIVATE, policyName);
    final String principal = currentPrincipal();

    final ErasurePolicyVersion active;
    try {
      active = db.getActiveErasurePolicyVersion(policyName);
    } catch (HiveException he) {
      throw new SemanticException("DEACTIVATE ERASURE POLICY: no ACTIVE version for '" + policyName + "' (" + he.getMessage() + ").");
    }
    if (active == null) {
      throw new SemanticException("DEACTIVATE ERASURE POLICY: no ACTIVE version for '" + policyName + "'.");
    }

    try {
      db.updateErasurePolicyVersionStatus(active.getVersionId(), PolicyVersionStatus.INACTIVE, principal);
    } catch (HiveException he) {
      throw new SemanticException("DEACTIVATE ERASURE POLICY: state transition failed: " + he.getMessage());
    }

    recordLifecycle(active.getVersionId(), PolicyLifecycleEventType.DEACTIVATED, principal,
        "DEACTIVATE ERASURE POLICY " + policyName + " -> " + active.getVersionLabel());
  }

  private String currentPrincipal() {
    try {
      return SessionState.get().getAuthenticator().getUserName();
    } catch (Throwable t) {
      return "unknown";
    }
  }

  private static String sha256Hex(final byte[] data) {
    final MessageDigest md;
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable on this JVM", e);
    }
    final byte[] digest = md.digest(data);
    final StringBuilder sb = new StringBuilder(digest.length * 2);
    for (final byte b : digest) {
      sb.append(Character.forDigit((b >> 4) & 0x0F, 16));
      sb.append(Character.forDigit(b & 0x0F, 16));
    }
    return sb.toString();
  }

  private void requirePrivilege(final String privilege, final String targetName) throws SemanticException {
    PolicyPrivilegeUtils.requirePrivilege(
        conf, db, privilege, targetName);
  }

  private void recordLifecycle(final long versionId, final PolicyLifecycleEventType type,
                               final String principal, final String note) {
    final ErasurePolicyLifecycleEvent evt = new ErasurePolicyLifecycleEvent();
    evt.setVersionId(versionId);
    evt.setEventType(type);
    evt.setPrincipal(principal);
    evt.setEventTs(System.currentTimeMillis());
    evt.setNote(note);
    try {
      db.recordLifecycleEvent(evt);
    } catch (HiveException he) {
      LOG.warn("recordLifecycleEvent failed for versionId={} type={}: {}", versionId, type, he.getMessage());
    }
  }


  private void analyzeShowAllErasureLocks(final ASTNode ast) throws SemanticException {
    requirePrivilege(PRIV_POLICY_VALIDATE, "*");
    List<?> locks;
    try {
      locks = db.listErasureRunLocks();
    } catch (HiveException he) {
      throw new SemanticException("SHOW ERASURE LOCKS: " + he.getMessage());
    }
    if (locks == null || locks.isEmpty()) {
      LOG.info("SHOW ERASURE LOCKS: no lock rows recorded");
    } else {
      LOG.info("SHOW ERASURE LOCKS: {} row(s) returned", locks.size());
      for (final Object row : locks) {
        LOG.info("  {}", row);
      }
    }
  }

  private void analyzeReleaseErasureLock(final ASTNode ast) throws SemanticException {
    final TableName tn = getQualifiedTableName((ASTNode) ast.getChild(0));
    requirePrivilege(PRIV_POLICY_MANAGE, qualified(tn));
    final Table table = getTable(qualified(tn));

    final long tblId = (table.getTTable() != null && table.getTTable().isSetId())
        ? table.getTTable().getId() : 0L;

    boolean force = false;
    String suppliedReason = null;
    for (int i = 1; i < ast.getChildCount(); i++) {
      final ASTNode child = (ASTNode) ast.getChild(i);
      if (child.getType() == HiveParser.TOK_FORCE_RELEASE) {
        force = true;
      } else if (child.getType() == HiveParser.TOK_RELEASE_REASON) {
        suppliedReason = unquoteIfStringLiteral(child.getChild(0).getText());
      }
    }
    if (force && (suppliedReason == null || suppliedReason.trim().isEmpty())) {
      throw new SemanticException("RELEASE ERASURE LOCK with FORCE requires a "
          + "WITH REASON '<text>' clause; the operator-supplied reason is recorded "
          + "in the audit trail next to the FORCE_RELEASED status.");
    }
    final String reason = (suppliedReason != null && !suppliedReason.isEmpty())
        ? suppliedReason
        : "manual release by " + currentPrincipal();

    if (!force) {
      try {
        final List<String> recentTmp = findRecentAnonTmpFiles(table,
            DEFAULT_TMP_RECENCY_MS);
        if (!recentTmp.isEmpty()) {
          final StringBuilder msg = new StringBuilder("RELEASE refused. Found ")
              .append(recentTmp.size()).append(" .anon.tmp files modified within the last ")
              .append(DEFAULT_TMP_RECENCY_MS / 1000L).append(" seconds:\n");
          for (final String p : recentTmp) {
            msg.append("  ").append(p).append('\n');
          }
          msg.append("A worker process may still be writing. If you've confirmed the ")
             .append("workers are dead, re-run with FORCE.");
          throw new SemanticException(msg.toString());
        }
      } catch (IOException io) {
        LOG.warn("RELEASE ERASURE LOCK: .anon.tmp safety scan failed for {}: {}",
            qualified(tn), io.getMessage());
        throw new SemanticException("RELEASE ERASURE LOCK: .anon.tmp safety scan failed ("
            + io.getMessage() + "). Use FORCE if you've confirmed workers are dead.");
      }
    }

    try {
      db.manuallyReleaseErasureRunLock(tblId, currentPrincipal(), reason, force);
      LOG.info("RELEASE ERASURE LOCK ON TABLE {} (force={}) by {}",
          qualified(tn), force, currentPrincipal());
    } catch (HiveException he) {
      throw new SemanticException("RELEASE ERASURE LOCK: " + he.getMessage());
    }

    try {
      final long now = System.currentTimeMillis();
      final ErasureRunAudit auditRow = new ErasureRunAudit();
      auditRow.setTblId(tblId);
      auditRow.setColumnName("");
      auditRow.setBindingId(0L);
      auditRow.setPrincipal(currentPrincipal());
      auditRow.setStartedTs(now);
      auditRow.setCompletedTs(now);
      auditRow.setStatus(force
          ? ErasureRunStatus.FORCE_RELEASED
          : ErasureRunStatus.RELEASED);
      auditRow.setReleaseReason(reason);
      db.recordErasureRun(auditRow);
    } catch (HiveException he) {
      LOG.warn("RELEASE ERASURE LOCK: audit-row write failed for {} (release itself succeeded): {}",
          qualified(tn), he.getMessage());
    }
  }


  private static final Set<String> VALID_PRIVILEGES =
      new HashSet<>(Arrays.asList(
          PRIV_POLICY_VALIDATE, PRIV_POLICY_ACTIVATE,
          PRIV_POLICY_MANAGE,   PRIV_ERASE, PRIV_EXPORT));

  private void analyzeGrantPolicyPriv(final ASTNode ast) throws SemanticException {
    final boolean catalogueWide = ast.getType() == HiveParser.TOK_POLICY_GRANT_ALL;
    final String privilege  = ast.getChild(0).getText();
    final String principal  = ast.getChild(catalogueWide ? 1 : 2).getText();
    final String policyName = catalogueWide ? "*" : ast.getChild(1).getText();
    final String target = catalogueWide ? "ALL ERASURE POLICIES" : "ERASURE POLICY " + policyName;
    validateGrantPrivilegeName(privilege, "GRANT");

    PolicyPrivilegeUtils.requireGrantAuthority(
        conf, db, privilege, policyName, principal);

    final long policyId = catalogueWide ? 0L : resolvePolicyId(policyName, "GRANT");

    final PolicyPriv pp = new PolicyPriv();
    pp.setPolicyId(policyId);
    pp.setPrincipalName(principal);
    pp.setPrincipalType("USER");
    pp.setPrivilege(privilege);
    pp.setCreateTime(System.currentTimeMillis());
    pp.setGrantor(currentPrincipal());
    pp.setGrantorType("USER");
    pp.setGrantOption(false);
    try {
      db.grantPolicyPriv(pp);
    } catch (HiveException he) {
      throw new SemanticException("GRANT " + privilege + " on " + target
          + " to " + principal + " failed: " + he.getMessage());
    }
    LOG.info("GRANT {} ON {} TO USER {} by {}", privilege, target, principal, currentPrincipal());
  }

  private void analyzeRevokePolicyPriv(final ASTNode ast) throws SemanticException {
    final boolean catalogueWide = ast.getType() == HiveParser.TOK_POLICY_REVOKE_ALL;
    final String privilege  = ast.getChild(0).getText();
    final String principal  = ast.getChild(catalogueWide ? 1 : 2).getText();
    final String policyName = catalogueWide ? "*" : ast.getChild(1).getText();
    final String target = catalogueWide ? "ALL ERASURE POLICIES" : "ERASURE POLICY " + policyName;
    validateGrantPrivilegeName(privilege, "REVOKE");
    PolicyPrivilegeUtils.requireRevokeAuthority(
        conf, db, privilege, policyName, principal);

    final long policyId = catalogueWide ? 0L : resolvePolicyId(policyName, "REVOKE");

    final List<PolicyPriv> rows;
    try {
      rows = db.listPolicyPrivs(policyId, principal);
    } catch (HiveException he) {
      throw new SemanticException("REVOKE: failed to look up grants for principal '"
          + principal + "': " + he.getMessage());
    }
    long revokedId = -1L;
    if (rows != null) {
      for (final PolicyPriv pp : rows) {
        if (privilege.equals(pp.getPrivilege()) && pp.getPolicyId() == policyId) {
          revokedId = pp.getPolicyPrivId();
          break;
        }
      }
    }
    if (revokedId < 0) {
      throw new SemanticException("REVOKE: no " + privilege + " grant exists "
          + "on " + target + " for user '" + principal + "'.");
    }
    try {
      db.revokePolicyPriv(revokedId);
    } catch (HiveException he) {
      throw new SemanticException("REVOKE " + privilege + " from " + principal
          + " on " + target + " failed: " + he.getMessage());
    }
    LOG.info("REVOKE {} ON {} FROM USER {} by {}", privilege, target, principal, currentPrincipal());
  }

  private void validateGrantPrivilegeName(final String priv, final String cmd)
      throws SemanticException {
    if (!VALID_PRIVILEGES.contains(priv)) {
      throw new SemanticException(cmd + ": '" + priv + "' is not a recognised "
          + "policy privilege. Expected one of "
          + VALID_PRIVILEGES + ".");
    }
  }

  private long resolvePolicyId(final String policyName, final String cmd)
      throws SemanticException {
    final ErasurePolicy policy;
    try {
      policy = db.getErasurePolicy(policyName);
    } catch (HiveException he) {
      throw new SemanticException(cmd + ": policy '" + policyName + "' lookup failed: "
          + he.getMessage());
    }
    if (policy == null || !policy.isSetPolicyId() || policy.getPolicyId() == 0L) {
      throw new SemanticException(cmd + ": policy '" + policyName + "' not found.");
    }
    return policy.getPolicyId();
  }

  private static final long DEFAULT_TMP_RECENCY_MS = 5L * 60L * 1000L;

  private List<String> findRecentAnonTmpFiles(final Table table,
      final long ageThresholdMs) throws IOException {
    final List<String> out = new ArrayList<>();
    if (table == null || table.getSd() == null || table.getSd().getLocation() == null) {
      return out;
    }
    final org.apache.hadoop.fs.Path root = new org.apache.hadoop.fs.Path(
        table.getSd().getLocation());
    final FileSystem fs = root.getFileSystem(conf);
    if (!fs.exists(root)) {
      return out;
    }
    final long cutoff = System.currentTimeMillis() - ageThresholdMs;
    walkForAnonTmp(fs, root, cutoff, out);
    return out;
  }

  private void walkForAnonTmp(final FileSystem fs,
      final org.apache.hadoop.fs.Path dir, final long cutoff,
      final List<String> out) throws IOException {
    final FileStatus[] entries = fs.listStatus(dir);
    if (entries == null) {
      return;
    }
    for (final FileStatus s : entries) {
      if (s.isDirectory()) {
        walkForAnonTmp(fs, s.getPath(), cutoff, out);
      } else if (s.getPath().getName().endsWith(".anon.tmp")
          && s.getModificationTime() >= cutoff) {
        out.add(s.getPath().toString());
      }
    }
  }

  private String activePolicySource(final String policyName) throws HiveException {
    final ErasurePolicyVersion v = db.getActiveErasurePolicyVersion(policyName);
    return (v == null) ? null : v.getSourceText();
  }

  private String composeActiveVersionJson(final String policyName) throws HiveException {
    final ErasurePolicyVersion active = db.getActiveErasurePolicyVersion(policyName);
    if (active == null) {
      return null;
    }
    final ObjectNode root =
        DataErasurePolicy.mapper.createObjectNode();
    root.put("identityFieldName", active.getIdentityFieldName());
    root.put("identityFieldType",
        active.getIdentityFieldType().name().toLowerCase());
    root.put("schemaFieldType", active.getSchemaType().name().toLowerCase());
    final ArrayNode statementsArr =
        root.putArray("statements");
    final List<ErasurePolicyStatement> stmts =
        db.getErasurePolicyStatements(active.getVersionId());
    if (stmts == null || stmts.isEmpty()) {
      final String src = active.getSourceText();
      if (src != null && !src.isEmpty()) {
        return dslPolicyToJson(src);
      }
      return null;
    }
    for (final ErasurePolicyStatement s : stmts) {
      final ObjectNode sNode =
          statementsArr.addObject();
      sNode.put("schemaId", s.getSchemaValue());
      final ArrayNode rulesArr =
          sNode.putArray("rules");
      final List<ErasurePolicyRule> rules =
          db.getErasurePolicyRules(s.getStatementId());
      for (final ErasurePolicyRule r : rules) {
        final ObjectNode rNode =
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
    } catch (JsonProcessingException e) {
      throw new HiveException("active-version JSON serialisation failed: "
          + e.getMessage(), e);
    }
  }

  private static String dslPolicyToJson(final String dsl) {
    try {
      return DataErasurePolicy.mapper.writeValueAsString(DataErasurePolicy.fromDsl(dsl));
    } catch (JsonProcessingException e) {
      throw new RuntimeException("failed to serialise DSL policy to the job-conf wire format: "
          + e.getMessage(), e);
    }
  }

  private String composeBindingUnionJson(final ErasurePolicyBinding binding,
                                         final List<String> memberNames,
                                         final String context) throws SemanticException {
    final Map<String, ParsedPolicy> union = new LinkedHashMap<>();
    for (final String name : memberNames) {
      if (union.containsKey(name)) {
        continue;
      }
      final String body;
      try {
        body = activePolicySource(name);
      } catch (HiveException he) {
        throw new SemanticException(context + ": active-version lookup failed for "
            + "binding member '" + name + "': " + he.getMessage());
      }
      if (body == null || body.isEmpty()) {
        throw new SemanticException(context + ": binding member '" + name
            + "' has no ACTIVE version; ACTIVATE it or DETACH the binding.");
      }
      try {
        union.put(name, ErasurePolicyValidator.parse(body));
      } catch (IllegalArgumentException iae) {
        throw new SemanticException(context + ": binding member '" + name
            + "' failed to parse as .erp DSL: " + iae.getMessage());
      }
    }
    if (union.isEmpty()) {
      throw new SemanticException(context + ": the binding has no member policies.");
    }

    String firstName = null;
    ParsedPolicy first = null;
    for (final Map.Entry<String, ParsedPolicy> e : union.entrySet()) {
      if (first == null) {
        firstName = e.getKey();
        first = e.getValue();
        continue;
      }
      final ParsedPolicy p = e.getValue();
      if (!first.getIdentityFieldName().equals(p.getIdentityFieldName())
          || first.getIdentityFieldType() != p.getIdentityFieldType()
          || first.getSchemaType() != p.getSchemaType()) {
        throw new SemanticException(context + ": binding members declare different "
            + "IDENTITY/SCHEMA ('" + firstName + "' vs '" + e.getKey() + "'); all "
            + "policies bound together must declare the same identity field, "
            + "identity type, and schema type.");
      }
    }

    final PolicyConflictDetector.ResolutionMode mode =
        (binding.getResolutionMode() == PolicyResolutionMode.STRICTEST)
            ? PolicyConflictDetector.ResolutionMode.STRICTEST
            : PolicyConflictDetector.ResolutionMode.EXPLICIT;
    final PolicyConflictDetector.Report report = PolicyConflictDetector.analyse(union, mode);
    if (mode == PolicyConflictDetector.ResolutionMode.EXPLICIT && report.hasConflicts()) {
      final StringBuilder msg = new StringBuilder(context);
      msg.append(" refused: the binding's member policies have conflicting ACTIVE "
          + "versions under EXPLICIT resolution; ")
         .append(report.conflicts.size()).append(" conflict(s):\n");
      for (final PolicyConflictDetector.Conflict c : report.conflicts) {
        msg.append("  ").append(c.toString()).append('\n');
      }
      msg.append("ACTIVATE non-conflicting versions, or DETACH and re-ATTACH under "
          + "RESOLUTION (STRICTEST).");
      throw new SemanticException(msg.toString());
    }

    final Map<String, List<ParsedPolicy.Rule>> bySchema = new LinkedHashMap<>();
    if (mode == PolicyConflictDetector.ResolutionMode.STRICTEST) {
      for (final PolicyConflictDetector.ResolvedRule r : report.resolvedRules) {
        bySchema.computeIfAbsent(r.schemaId, k -> new ArrayList<>()).add(r.rule);
      }
    } else {
      for (final ParsedPolicy p : union.values()) {
        for (final ParsedPolicy.Statement s : p.getStatements()) {
          for (final ParsedPolicy.Rule r : s.rules) {
            bySchema.computeIfAbsent(s.schemaId, k -> new ArrayList<>()).add(r);
          }
        }
      }
    }

    final ObjectNode root =
        DataErasurePolicy.mapper.createObjectNode();
    root.put("identityFieldName", first.getIdentityFieldName());
    root.put("identityFieldType", first.getIdentityFieldType().name().toLowerCase());
    root.put("schemaFieldType", first.getSchemaType().name().toLowerCase());
    final ArrayNode statementsArr =
        root.putArray("statements");
    for (final Map.Entry<String, List<ParsedPolicy.Rule>> e : bySchema.entrySet()) {
      final ObjectNode sNode = statementsArr.addObject();
      sNode.put("schemaId", e.getKey());
      final ArrayNode rulesArr = sNode.putArray("rules");
      for (final ParsedPolicy.Rule r : e.getValue()) {
        final ObjectNode rNode = rulesArr.addObject();
        rNode.put("path", r.fieldPath);
        rNode.put("action", r.action == null ? null : r.action.name());
        if (r.replaceValue != null) {
          rNode.put("value", r.replaceValue);
        }
      }
    }
    try {
      return DataErasurePolicy.mapper.writeValueAsString(root);
    } catch (JsonProcessingException e) {
      throw new SemanticException(context + ": failed to serialise the merged "
          + "policy union to JSON: " + e.getMessage());
    }
  }

  private void analyzeExtractFromTable(final ASTNode ast) throws SemanticException {
    final TableName tn = getQualifiedTableName((ASTNode) ast.getChild(0));
    final String column = ast.getChild(1).getText().toLowerCase();
    final ASTNode pidListAst = (ASTNode) ast.getChild(2);

    final List<String> identityValues = new ArrayList<>();
    for (int i = 0; i < pidListAst.getChildCount(); i++) {
      identityValues.add(unquoteIfStringLiteral(pidListAst.getChild(i).getText()));
    }
    if (identityValues.isEmpty()) {
      throw new SemanticException("EXTRACT FROM TABLE requires at least one identity value");
    }

    final Table table;
    try {
      table = db.getTable(tn.getDb(), tn.getTable(), false);
    } catch (HiveException he) {
      throw new SemanticException("EXTRACT FROM TABLE: table lookup failed for "
          + qualified(tn) + ": " + he.getMessage());
    }
    if (table == null) {
      throw new SemanticException("EXTRACT FROM TABLE: table not found: " + qualified(tn));
    }
    final long tblId = (table.getTTable() != null && table.getTTable().isSetId())
        ? table.getTTable().getId() : 0L;

    final ErasurePolicyBinding binding;
    try {
      binding = db.getErasurePolicyBinding(tblId, column);
    } catch (HiveException he) {
      throw new SemanticException("EXTRACT FROM TABLE: binding lookup failed for "
          + qualified(tn) + "." + column + ": " + he.getMessage());
    }
    if (binding == null) {
      throw new SemanticException("EXTRACT FROM TABLE: no policy attached to "
          + qualified(tn) + "." + column
          + ". Run 'ATTACH DATA ERASURE POLICY <name> ON TABLE " + qualified(tn)
          + " COLUMN " + column + "' first. EXTRACT shares the §4.6 binding "
          + "lookup with ERASE so the (table, column) -> policy mapping covers "
          + "both surfaces.");
    }

    final List<String> memberNames = new ArrayList<>();
    try {
      final List<ErasurePolicyBindingMember> members =
          db.getBindingMembers(binding.getBindingId());
      if (members != null) {
        for (final ErasurePolicyBindingMember m : members) {
          final ErasurePolicy ep = lookupPolicyById(m.getPolicyId());
          if (ep == null) {
            throw new SemanticException("EXTRACT FROM TABLE: binding member with "
                + "policy id " + m.getPolicyId() + " on " + qualified(tn) + "." + column
                + " does not resolve to a policy; DETACH the binding and re-ATTACH "
                + "the surviving policies.");
          }
          if (!memberNames.contains(ep.getPolicyName())) {
            memberNames.add(ep.getPolicyName());
          }
        }
      }
    } catch (SemanticException se) {
      throw se;
    } catch (HiveException he) {
      throw new SemanticException("EXTRACT FROM TABLE: policy-name lookup failed for "
          + qualified(tn) + "." + column + ": " + he.getMessage());
    }
    final String policyName = memberNames.isEmpty() ? null : memberNames.get(0);
    if (policyName == null) {
      throw new SemanticException("EXTRACT FROM TABLE: binding for " + qualified(tn)
          + "." + column + " has no resolvable policy (the binding-members list "
          + "is empty or the policy id does not resolve); attach a policy through "
          + "ATTACH first.");
    }

    for (final String member : memberNames) {
      requirePrivilege(PRIV_EXPORT, member);
    }

    String versionLabel = null;
    String identityFieldName = null;
    try {
      final ErasurePolicyVersion active = db.getActiveErasurePolicyVersion(policyName);
      if (active == null) {
        throw new SemanticException("EXTRACT FROM TABLE refused. Policy '"
            + policyName + "' bound to " + qualified(tn)
            + " has no ACTIVE version. Run 'ACTIVATE ERASURE POLICY "
            + policyName + "' first.");
      }
      versionLabel = active.getVersionLabel();
      if (active.isSetIdentityFieldName()) {
        identityFieldName = active.getIdentityFieldName();
      }
    } catch (HiveException he) {
      throw new SemanticException("EXTRACT FROM TABLE: active-version lookup failed for '"
          + policyName + "': " + he.getMessage());
    }
    if (identityFieldName == null || identityFieldName.isEmpty()) {
      identityFieldName = IDENTITY_FIELD_NAME;
    }

    final long now = System.currentTimeMillis();
    try {
      final ErasureRunAudit row = new ErasureRunAudit();
      row.setTblId(tblId);
      row.setColumnName(column);
      row.setBindingId(binding.getBindingId());
      row.setPrincipal(currentPrincipal());
      row.setStartedTs(now);
      row.setCompletedTs(now);
      row.setStatus(ErasureRunStatus.EXTRACTED);
      row.setIdentityValues(String.join(",", identityValues));
      db.recordErasureRun(row);
    } catch (HiveException he) {
      throw new SemanticException("EXTRACT FROM TABLE: audit-row write failed for "
          + qualified(tn) + "." + column + ": " + he.getMessage());
    }

    if (!binding.isSetSchemaField() || !binding.isSetRowLocator()
        || !binding.isSetColumnFormat()) {
      throw new SemanticException("EXTRACT FROM TABLE: the binding for "
          + qualified(tn) + "." + column + " is missing the row-shape metadata "
          + "the Tez DAG path needs (SCHEMA FIELD / ROW LOCATOR / COLUMN FORMAT). "
          + "Re-issue ATTACH DATA ERASURE POLICY ... WITH (SCHEMA FIELD (sf), "
          + "ROW LOCATOR (rl), COLUMN FORMAT (ft)) RESOLUTION (EXPLICIT).");
    }
    final String bindingSchemaField  = binding.getSchemaField();
    final String bindingRowLocator   = binding.getRowLocator();
    final ColumnInternalFormat bindingColumnFormat = binding.getColumnFormat();
    final ErasurePolicy policyObj;
    final String policyObjSource;
    try {
      policyObj = db.getErasurePolicy(policyName);
      policyObjSource = (policyObj == null) ? null : activePolicySource(policyName);
    } catch (HiveException he) {
      throw new SemanticException("EXTRACT FROM TABLE: policy body lookup failed for '"
          + policyName + "': " + he.getMessage());
    }
    if (policyObj == null || policyObjSource == null) {
      throw new SemanticException("EXTRACT FROM TABLE: policy '" + policyName
          + "' has no ACTIVE version with a recorded body; cannot drive the ExtractProcessor.");
    }

    boolean indexFound = false;
    String indexPath = null;
    Index indexMeta = null;
    try {
      final List<Index> indexes = db.getIndexes(table.getDbName(),
          table.getTableName(), (short) -1);
      for (final Index ix : indexes) {
        if (ix.isSetIndexType()) {
          indexFound = true;
          indexPath = getTable(ix.getIndexTableName()).getSd().getLocation();
          indexMeta = ix;
          break;
        }
      }
    } catch (HiveException he) {
      throw new SemanticException("EXTRACT FROM TABLE: index lookup failed for "
          + qualified(tn) + ": " + he.getMessage());
    }

    int msgIdIx = -1, msgOffsetIx = -1, bodyIx = -1;
    String msgIdType = "", offsetType = "";
    final List<String> colNames = new ArrayList<>();
    final List<String> colTypes = new ArrayList<>();
    for (final StructField field : table.getFields()) {
      final String fldName = field.getFieldName();
      if (fldName.equals(bindingRowLocator)) {
        msgOffsetIx = field.getFieldID();
        offsetType = field.getFieldObjectInspector().getTypeName();
      }
      if (fldName.equals(column)) {
        bodyIx = field.getFieldID();
      }
      if (fldName.equals(bindingSchemaField)) {
        msgIdIx = field.getFieldID();
        msgIdType = field.getFieldObjectInspector().getTypeName();
      }
      colNames.add(fldName);
      colTypes.add(field.getFieldObjectInspector().getTypeName());
    }
    if (msgIdIx < 0 || msgOffsetIx < 0 || bodyIx < 0) {
      throw new SemanticException("EXTRACT FROM TABLE: column-policy on "
          + qualified(tn) + " is incomplete (msgIdIx=" + msgIdIx
          + ", msgOffsetIx=" + msgOffsetIx + ", bodyIx=" + bodyIx + "); "
          + "re-run ATTACH DATA ERASURE POLICY to repopulate.");
    }
    inputs.add(new ReadEntity(table));

    String json;
    if (memberNames.size() > 1) {
      json = composeBindingUnionJson(binding, memberNames, "EXTRACT FROM TABLE");
    } else {
      try {
        json = composeActiveVersionJson(policyName);
      } catch (HiveException he) {
        throw new SemanticException("EXTRACT FROM TABLE: active-version "
            + "policy reconstruction failed for '" + policyName + "': "
            + he.getMessage());
      }
    }
    if (json == null) {
      throw new SemanticException("EXTRACT FROM TABLE refused. Policy '"
          + policyName + "' has no ACTIVE version with stored source; "
          + "ACTIVATE a validated version / re-LOAD the policy first.");
    }
    final DataErasurePolicy erasurePolicy = DataErasurePolicy.fromString(json);
    final String keyType = getType(erasurePolicy.identityFieldType);
    final String valueTypes = "T" + getType(offsetType) + getType(msgIdType);

    conf.setStrings(IOConstants.COLUMNS, String.join(",", colNames));
    conf.setStrings(IOConstants.COLUMNS_TYPES, String.join(",", colTypes));
    final FileType fileType = getFileType(table);
    conf.set(ANON_FILE_TYPE, fileType.name());

    if (indexMeta != null) {
      final int pageSize = indexMeta.getPageSize();
      final int bufferSize = indexMeta.getBufferPoolSize() * pageSize;
      conf.setInt(BTREE_CONF_BUFFER_SIZE, bufferSize);
      conf.setInt(BTREE_CONF_PAGE_SIZE, pageSize);
      conf.setInt(BTREE_CONF_PAGE_HEADER_SIZE, 64);
      conf.setInt(BTREE_CONF_DUMP_BYTES_PER_ROW, pageSize);
      if (indexMeta.getIndexType() != IndexType.TABULAR) {
        conf.set(INDEX_ADDR_TYPE, getAddrType(indexMeta));
      }
      conf.set(INDEX_KEY_TYPE, keyType);
      conf.set(INDEX_VALUE_TYPES, valueTypes);
      conf.set(ANON_INDEX_TYPE, indexMeta.getIndexType().toString());
    }

    conf.setBoolean(ANON_INDEX_FOUND, indexFound);
    conf.setInt(ANON_MSG_ID_IX, msgIdIx);
    conf.setInt(ANON_MSG_OFFSET_IX, msgOffsetIx);
    conf.setInt(ANON_BODY_IX, bodyIx);
    conf.set(ANON_PID_LIST, String.join(",", identityValues));
    conf.set(ANON_POLICY_DOC, json);
    conf.set(ANON_COLUMN_INTERNAL_FORMAT, bindingColumnFormat.name());

    final org.apache.hadoop.fs.Path stagingDir = new org.apache.hadoop.fs.Path(
        ctx.getMRTmpPath(), "dae-extract-" + queryState.getQueryId());
    conf.set(ExtractProcessor.EXTRACT_STAGING_DIR, stagingDir.toString());
    if (ast.getChildCount() > 3) {
      final SessionState ss = SessionState.get();
      if (ss != null) {
        ss.getHiveVariables().put("anon.extract.outFile",
            unquoteIfStringLiteral(ast.getChild(3).getText()));
        ss.getHiveVariables().put("anon.extract.stagingDir", stagingDir.toString());
      }
      ensureCompletionHookRegistered();
    }

    final String inputDir = table.getPath().toString();
    final ExtractWork extractWork = new ExtractWork(indexFound);
    extractWork.setName("Extractor");
    extractWork.setInputDir(inputDir);
    extractWork.setStagingDir(stagingDir.toString());
    extractWork.setNumReducers(1);

    final TezWork tezWork = new TezWork(queryState.getQueryId(), conf);
    tezWork.add(extractWork);
    if (indexFound) {
      final IndexWork indexWork = new IndexWork(indexPath);
      indexWork.setName(AnonConst.ANON_EDGE_INDEX);
      tezWork.add(indexWork);
      final TezEdgeProperty edge =
          new TezEdgeProperty(TezEdgeProperty.EdgeType.SIMPLE_EDGE);
      tezWork.connect(indexWork, extractWork, edge);
    }
    final Task<? extends Serializable> tezTask = TaskFactory.get(tezWork, conf);
    rootTasks.add(tezTask);

    ctx.setResFile(stagingDir);
    setFetchTask(createFetchTask("projected_body#string"));
    this.extractResultSchema = Collections.singletonList(
        new FieldSchema(
            "projected_body", "string",
            "JSON projection of the body restricted to policy-declared paths"));

    LOG.info("EXTRACT FROM TABLE {}.{} -> Tez DAG (indexFound={}) under policy {} "
        + "(version {}); EXTRACTED audit row recorded at ts={}; staging={}",
        qualified(tn), column, indexFound, policyName, versionLabel, now, stagingDir);
  }

  private void analyzeAuditByIdentity(final ASTNode ast) throws SemanticException {
    final ASTNode pidListAst = (ASTNode) ast.getChild(0);
    final List<String> identityValues = new ArrayList<>();
    for (int i = 0; i < pidListAst.getChildCount(); i++) {
      identityValues.add(unquoteIfStringLiteral(pidListAst.getChild(i).getText()));
    }
    if (identityValues.isEmpty()) {
      throw new SemanticException("AUDIT BY IDENTITY VALUES requires at least one identity value");
    }
    requirePrivilege(PRIV_POLICY_VALIDATE, "*");

    final List<ErasureRunAudit> aggregated =
        new ArrayList<>();
    final Set<String> seenRunKeys = new HashSet<>();
    for (final String identity : identityValues) {
      final List<ErasureRunAudit> rows;
      try {
        rows = db.getErasureRunsForTable(0L, Long.MIN_VALUE, Long.MAX_VALUE, null, identity);
      } catch (HiveException he) {
        throw new SemanticException("AUDIT BY IDENTITY VALUES: query failed for '"
            + identity + "': " + he.getMessage());
      }
      if (rows == null) {
        continue;
      }
      for (final ErasureRunAudit r : rows) {
        if (seenRunKeys.add(r.getTblId() + ":" + r.getRunId())) {
          aggregated.add(r);
        }
      }
    }
    aggregated.sort(Comparator.comparingLong(
        ErasureRunAudit::getStartedTs));

    final StringBuilder out = new StringBuilder();
    out.append("AUDIT BY IDENTITY VALUES ").append(identityValues)
       .append(" (Article 33 subject-keyed inverse audit)\n");
    if (aggregated.isEmpty()) {
      out.append("  No erasure events recorded for the requested identity set.\n");
    } else {
      out.append("  ").append(aggregated.size()).append(" event(s) found:\n");
      for (final ErasureRunAudit r : aggregated) {
        out.append("    runId=").append(r.getRunId())
           .append(" tblId=").append(r.getTblId())
           .append(" startedTs=").append(r.getStartedTs())
           .append(" principal=").append(r.getPrincipal())
           .append(" status=").append(r.getStatus());
        if (r.isSetIdentityValues()) {
          out.append(" identityValues='").append(r.getIdentityValues()).append('\'');
        }
        out.append('\n');
      }
    }
    emitAuditResult(out.toString());
    LOG.info("AUDIT BY IDENTITY VALUES {} -> {} event(s)",
        identityValues, aggregated.size());
  }

}
