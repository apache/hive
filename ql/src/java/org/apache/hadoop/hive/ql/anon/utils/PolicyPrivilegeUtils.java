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

package org.apache.hadoop.hive.ql.anon.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.ErasurePolicy;
import org.apache.hadoop.hive.metastore.api.PolicyPriv;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.List;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_POLICY_ENFORCE_PRIVILEGES;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_POLICY_GRANT_ADMIN_USERS;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.PRIV_ERASE;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.PRIV_POLICY_VALIDATE;

public final class PolicyPrivilegeUtils {

  private PolicyPrivilegeUtils() {
  }

  public static boolean isErasureAdmin(final Configuration conf, final String principal) {
    if (principal == null || principal.isEmpty()) {
      return false;
    }
    final String csv = conf.get(ANON_POLICY_GRANT_ADMIN_USERS, "");
    if (csv == null || csv.isEmpty()) {
      return false;
    }
    for (final String u : csv.split(",")) {
      if (principal.equals(u.trim())) {
        return true;
      }
    }
    return false;
  }

  public static void requirePrivilege(final Configuration conf, final Hive db,
      final String privilege, final String targetName) throws SemanticException {
    if (!conf.getBoolean(ANON_POLICY_ENFORCE_PRIVILEGES, true)) {
      return;
    }
    final String principal = requirePrincipal(privilege + " on '" + targetName + "'");

    if (isErasureAdmin(conf, principal)) {
      throw new SemanticException("erasure-admin '" + principal + "' may not run erasure commands: "
          + "the ERASURE_ADMIN role administers grants only (it may GRANT/REVOKE and inspect "
          + "grants) and holds no operational privilege.");
    }

    long targetPolicyId = -1L;
    if (!PRIV_ERASE.equals(privilege)) {
      try {
        final ErasurePolicy p = db.getErasurePolicy(targetName);
        if (p != null && p.isSetPolicyId()) {
          targetPolicyId = p.getPolicyId();
        }
      } catch (HiveException he) {
      }
    }

    final List<PolicyPriv> privs;
    try {
      privs = db.listPolicyPrivs(targetPolicyId > 0 ? targetPolicyId : 0L, principal);
    } catch (HiveException he) {
      throw new SemanticException(
          "privilege check failed for principal '" + principal + "' (" + he.getMessage() + ")");
    }
    if (holds(privs, privilege, targetPolicyId)) {
      return;
    }
    throw new SemanticException(
        "principal '" + principal + "' lacks " + privilege + " on '" + targetName + "'");
  }

  public static void requireGrantAuthority(final Configuration conf, final Hive db,
      final String privilege, final String policyName, final String grantee)
      throws SemanticException {
    if (!conf.getBoolean(ANON_POLICY_ENFORCE_PRIVILEGES, true)) {
      return;
    }
    final String grantor = requirePrincipal("GRANT " + privilege + " on '" + policyName + "'");
    if (!isErasureAdmin(conf, grantor)) {
      throw new SemanticException("principal '" + grantor + "' may not grant " + privilege
          + " on '" + policyName + "': only an ERASURE_ADMIN (a principal named in "
          + ANON_POLICY_GRANT_ADMIN_USERS + ") may grant erasure privileges.");
    }
    if (heldGrantConflict(db, grantor)) {
      throw new SemanticException("principal '" + grantor + "' is configured as an ERASURE_ADMIN "
          + "but also holds operational erasure privileges; the two are mutually exclusive. Revoke "
          + "its grants (or remove it from " + ANON_POLICY_GRANT_ADMIN_USERS
          + ") before it may grant.");
    }
    if (grantor.equals(grantee)) {
      throw new SemanticException("erasure-admin '" + grantor + "' may not grant " + privilege
          + " to itself.");
    }
    if (isErasureAdmin(conf, grantee)) {
      throw new SemanticException("may not grant " + privilege + " to '" + grantee
          + "': it is an ERASURE_ADMIN, and the admin role is mutually exclusive with the "
          + "operational privileges (an admin administers grants but runs no erasure command).");
    }
  }

  public static void requireRevokeAuthority(final Configuration conf, final Hive db,
      final String privilege, final String policyName, final String target)
      throws SemanticException {
    if (!conf.getBoolean(ANON_POLICY_ENFORCE_PRIVILEGES, true)) {
      return;
    }
    final String revoker = requirePrincipal("REVOKE " + privilege + " on '" + policyName + "'");
    if (revoker != null && revoker.equals(target)) {
      return;
    }
    if (!isErasureAdmin(conf, revoker)) {
      throw new SemanticException("principal '" + revoker + "' may not revoke " + privilege
          + " on '" + policyName + "' from '" + target + "': only an ERASURE_ADMIN may revoke "
          + "another principal's grant.");
    }
    if (heldGrantConflict(db, revoker)) {
      throw new SemanticException("principal '" + revoker + "' is configured as an ERASURE_ADMIN "
          + "but also holds operational erasure privileges; resolve the conflict (revoke its grants "
          + "or remove it from " + ANON_POLICY_GRANT_ADMIN_USERS + ") before it may revoke.");
    }
  }

  public static void requireGrantView(final Configuration conf, final Hive db,
      final String targetName) throws SemanticException {
    if (!conf.getBoolean(ANON_POLICY_ENFORCE_PRIVILEGES, true)) {
      return;
    }
    final String principal = requirePrincipal("SHOW ERASURE GRANTS on '" + targetName + "'");
    if (isErasureAdmin(conf, principal)) {
      return;
    }
    long targetPolicyId = -1L;
    try {
      final ErasurePolicy p = db.getErasurePolicy(targetName);
      if (p != null && p.isSetPolicyId()) {
        targetPolicyId = p.getPolicyId();
      }
    } catch (HiveException he) {
    }
    final List<PolicyPriv> privs;
    try {
      privs = db.listPolicyPrivs(targetPolicyId > 0 ? targetPolicyId : 0L, principal);
    } catch (HiveException he) {
      throw new SemanticException(
          "grant-view check failed for principal '" + principal + "' (" + he.getMessage() + ")");
    }
    if (holds(privs, PRIV_POLICY_VALIDATE, targetPolicyId)) {
      return;
    }
    throw new SemanticException("principal '" + principal + "' may not inspect erasure grants on '"
        + targetName + "': requires the ERASURE_ADMIN role or " + PRIV_POLICY_VALIDATE + ".");
  }

  private static boolean heldGrantConflict(final Hive db, final String principal)
      throws SemanticException {
    try {
      final List<PolicyPriv> held = db.listPolicyPrivs(0L, principal);
      return held != null && !held.isEmpty();
    } catch (HiveException he) {
      throw new SemanticException("erasure-admin conflict check failed for principal '"
          + principal + "' (" + he.getMessage() + ")");
    }
  }

  static boolean canGrant(final boolean grantorIsAdmin, final boolean granteeIsAdmin,
      final String grantor, final String grantee) {
    if (!grantorIsAdmin) {
      return false;
    }
    if (grantor != null && grantor.equals(grantee)) {
      return false;
    }
    return !granteeIsAdmin;
  }

  static boolean holds(final List<PolicyPriv> privs, final String privilege,
      final long targetPolicyId) {
    if (privs == null) {
      return false;
    }
    for (final PolicyPriv p : privs) {
      if (!privilege.equals(p.getPrivilege())) {
        continue;
      }
      if (p.getPolicyId() == 0L) {
        return true;
      }
      if (targetPolicyId > 0 && p.getPolicyId() == targetPolicyId) {
        return true;
      }
    }
    return false;
  }

  private static String currentPrincipal() {
    try {
      final String u = SessionState.get().getAuthenticator().getUserName();
      if (u != null && !u.isEmpty()) {
        return u;
      }
    } catch (Throwable t) {
    }
    return null;
  }

  private static String requirePrincipal(final String action) throws SemanticException {
    final String p = currentPrincipal();
    if (p == null) {
      throw new SemanticException("ERASURE privilege enforcement is on but the session principal "
          + "could not be determined; refusing " + action + " (fail-closed). Authenticate the "
          + "session, or set " + ANON_POLICY_ENFORCE_PRIVILEGES + "=false to disable enforcement.");
    }
    return p;
  }
}
