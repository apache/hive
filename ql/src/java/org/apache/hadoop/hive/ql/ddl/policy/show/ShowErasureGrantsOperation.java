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

package org.apache.hadoop.hive.ql.ddl.policy.show;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ErasurePolicy;
import org.apache.hadoop.hive.metastore.api.PolicyPriv;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.IOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_POLICY_GRANT_ADMIN_USERS;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.PRIV_ERASURE_ADMIN;

/**
 * Resolves the optional policy name to its policy id, queries
 * {@code MPolicyPriv} through the Metastore client, and writes one tab-
 * separated row per grant to the result file. Wildcard grants
 * ({@code policyId == 0}) are surfaced regardless of the policy filter
 * because they apply to any policy; per-policy grants are surfaced only
 * when the filter matches.
 */
public class ShowErasureGrantsOperation extends DDLOperation<ShowErasureGrantsDesc> {

  public ShowErasureGrantsOperation(DDLOperationContext context, ShowErasureGrantsDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    final Hive db = context.getDb();
    final String principalFilter = desc.getPrincipalName();
    final String policyFilter = desc.getPolicyName();

    long policyId = 0L; // wildcard query
    if (policyFilter != null) {
      final ErasurePolicy p = db.getErasurePolicy(policyFilter);
      if (p == null) {
        throw new HiveException(ErrorMsg.GENERIC_ERROR,
            "SHOW ERASURE GRANT: policy not found: " + policyFilter);
      }
      if (p.isSetPolicyId()) {
        policyId = p.getPolicyId();
      }
    }

    final List<PolicyPriv> rows = db.listPolicyPrivs(policyId, principalFilter);

    final DataOutputStream out = ShowUtils.getOutputStream(new Path(desc.getResFile()), context);
    try {
      if (rows != null) {
        for (final PolicyPriv pp : rows) {
          if (policyFilter != null && pp.getPolicyId() != 0L && pp.getPolicyId() != policyId) {
            continue;
          }
          writeRow(out, pp);
        }
      }
      // ERASURE_ADMIN role-holders are named in anon.policy.grant.admin.users
      // and hold no MPolicyPriv grant, so they would be invisible to this view.
      // Surface them as synthetic rows (catalogue-wide, so independent of the
      // policy filter; the principal filter still applies) to make the
      // oversight view complete.
      final String csv = context.getConf().get(ANON_POLICY_GRANT_ADMIN_USERS, "");
      if (csv != null && !csv.isEmpty()) {
        for (final String raw : csv.split(",")) {
          final String admin = raw.trim();
          if (admin.isEmpty() || (principalFilter != null && !principalFilter.equals(admin))) {
            continue;
          }
          // An admin that also holds operational grants is a mutual-exclusion
          // conflict; flag it so the oversight view surfaces the misconfiguration.
          writeAdminRow(out, admin, adminHoldsGrants(db, admin));
        }
      }
    } catch (IOException e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, "SHOW ERASURE GRANT");
    } finally {
      IOUtils.closeStream(out);
    }
    return 0;
  }

  private static void writeRow(final DataOutputStream out, final PolicyPriv p) throws IOException {
    out.writeBytes(safe(p.getPrincipalName()));
    out.write(Utilities.tabCode);
    out.writeBytes(safe(p.getPrincipalType()));
    out.write(Utilities.tabCode);
    out.writeBytes(Long.toString(p.getPolicyId()));
    out.write(Utilities.tabCode);
    // policy_name: empty for wildcard rows (policyId == 0); resolving the
    // id back to a name would require an extra metastore round-trip per
    // row and the id itself is the authoritative key in the audit trail.
    out.writeBytes(p.getPolicyId() == 0L ? "*" : "");
    out.write(Utilities.tabCode);
    out.writeBytes(safe(p.getPrivilege()));
    out.write(Utilities.tabCode);
    out.writeBytes(Boolean.toString(p.isGrantOption()));
    out.write(Utilities.tabCode);
    out.writeBytes(Long.toString(p.getCreateTime()));
    out.write(Utilities.tabCode);
    out.writeBytes("grant");
    out.write(Utilities.newLineCode);
  }

  /**
   * A synthetic row for an {@code ERASURE_ADMIN} role-holder named in
   * {@code anon.policy.grant.admin.users}. The role carries no MPolicyPriv
   * grant, so the row is catalogue-wide ({@code policy_id 0}, {@code policy_name
   * *}), has no grant option or create-time, and is marked {@code config} in
   * the source column to distinguish it from a granted privilege.
   */
  private static void writeAdminRow(final DataOutputStream out, final String admin,
      final boolean conflict) throws IOException {
    out.writeBytes(safe(admin));
    out.write(Utilities.tabCode);
    out.writeBytes("USER");
    out.write(Utilities.tabCode);
    out.writeBytes("0");
    out.write(Utilities.tabCode);
    out.writeBytes("*");
    out.write(Utilities.tabCode);
    out.writeBytes(PRIV_ERASURE_ADMIN);
    out.write(Utilities.tabCode);
    out.writeBytes("false");
    out.write(Utilities.tabCode);
    out.writeBytes("0");
    out.write(Utilities.tabCode);
    // A config admin that also holds operational grants violates the mutual
    // exclusion; mark it so the misconfiguration is visible to the overseer.
    out.writeBytes(conflict ? "config-conflict" : "config");
    out.write(Utilities.newLineCode);
  }

  /** True iff the (config) erasure-admin also holds operational grants. */
  private static boolean adminHoldsGrants(final Hive db, final String admin) {
    try {
      final List<PolicyPriv> held = db.listPolicyPrivs(0L, admin);
      return held != null && !held.isEmpty();
    } catch (HiveException he) {
      return false;                         // cannot determine; do not flag
    }
  }

  private static String safe(final String s) {
    return s == null ? "" : s;
  }
}
