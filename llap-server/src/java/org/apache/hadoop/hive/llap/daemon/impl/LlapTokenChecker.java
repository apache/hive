/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.daemon.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.ArrayList;

import java.util.List;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LlapTokenChecker {
  private static final Logger LOG = LoggerFactory.getLogger(LlapTokenChecker.class);

  public static final class LlapTokenInfo {
    public final String userName;
    public final String appId;
    public final boolean isSigningRequired;

    public LlapTokenInfo(String userName, String appId, boolean isSigningRequired) {
      this.userName = userName;
      this.appId = appId;
      this.isSigningRequired = isSigningRequired;
    }
  }

  private static final LlapTokenInfo NO_SECURITY = new LlapTokenInfo(null, null, false);
  public static LlapTokenInfo getTokenInfo(String clusterId) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) return NO_SECURITY;
    UserGroupInformation current = UserGroupInformation.getCurrentUser();
    String kerberosName = current.hasKerberosCredentials() ? current.getShortUserName() : null;
    List<LlapTokenIdentifier> tokens = getLlapTokens(current, clusterId);
    if ((tokens == null || tokens.isEmpty()) && kerberosName == null) {
      throw new SecurityException("No tokens or kerberos for " + current);
    }
    warnMultipleTokens(tokens);
    return getTokenInfoInternal(kerberosName, tokens);
  }

  public static void warnMultipleTokens(List<LlapTokenIdentifier> tokens) {
    if (tokens != null && tokens.size() > 1) {
      StringBuilder sb = new StringBuilder("Found multiple LLAP tokens: [");
      boolean isFirst = true;
      for (LlapTokenIdentifier ti : tokens) {
        if (!isFirst) {
          sb.append(", ");
        }
        isFirst = false;
        sb.append(ti);
      }
      LOG.warn(sb.append("]").toString());
    }
  }

  static List<LlapTokenIdentifier> getLlapTokens(
      UserGroupInformation ugi, String clusterId) {
    List<LlapTokenIdentifier> tokens = null;
    for (TokenIdentifier id : ugi.getTokenIdentifiers()) {
      if (!LlapTokenIdentifier.KIND_NAME.equals(id.getKind())) continue;
      LOG.debug("Token {}", id);
      LlapTokenIdentifier llapId = (LlapTokenIdentifier)id;
      if (clusterId != null && !clusterId.equals(llapId.getClusterId())) continue;
      if (tokens == null) {
        tokens = new ArrayList<>();
      }
      tokens.add((LlapTokenIdentifier)id);
    }
    return tokens;
  }

  @VisibleForTesting
  static LlapTokenInfo getTokenInfoInternal(
      String kerberosName, List<LlapTokenIdentifier> tokens) {
    assert (tokens != null && !tokens.isEmpty()) || kerberosName != null;
    if (tokens == null) {
      return new LlapTokenInfo(kerberosName, null, true);
    }
    String userName = kerberosName, appId = null;
    boolean isSigningRequired = false;
    for (LlapTokenIdentifier llapId : tokens) {
      String newUserName = llapId.getOwner().toString();
      if (userName != null && !userName.equals(newUserName)) {
        throw new SecurityException("Ambiguous user name from credentials - " + userName
            + " and " + newUserName + " from " + llapId
            + ((kerberosName == null) ? ("; has kerberos credentials for " + kerberosName) : ""));
      }
      userName = newUserName;
      String newAppId = llapId.getAppId();
      if (!StringUtils.isEmpty(newAppId)) {
        if (!StringUtils.isEmpty(appId) && !appId.equals(newAppId)) {
          throw new SecurityException("Ambiguous app ID from credentials - " + appId
              + " and " + newAppId + " from " + llapId);
        }
        appId = newAppId;
      }
      isSigningRequired = isSigningRequired || llapId.isSigningRequired();
    }
    assert userName != null;
    return new LlapTokenInfo(userName, appId, isSigningRequired);
  }

  public static void checkPermissions(
      String clusterId, String userName, String appId, Object hint) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) return;
    Preconditions.checkNotNull(userName);
    UserGroupInformation current = UserGroupInformation.getCurrentUser();
    String kerberosName = current.hasKerberosCredentials() ? current.getShortUserName() : null;
    List<LlapTokenIdentifier> tokens = getLlapTokens(current, clusterId);
    checkPermissionsInternal(kerberosName, tokens, userName, appId, hint);
  }

  @VisibleForTesting
  static void checkPermissionsInternal(String kerberosName, List<LlapTokenIdentifier> tokens,
      String userName, String appId, Object hint) {
    if (appId == null) {
      appId = "";
    }
    if (kerberosName != null && StringUtils.isBlank(appId) && kerberosName.equals(userName)) {
      return;
    }
    if (tokens != null) {
      for (LlapTokenIdentifier llapId : tokens) {
        String tokenUser = llapId.getOwner().toString(), tokenAppId = llapId.getAppId();
        if (checkTokenPermissions(userName, appId, tokenUser, tokenAppId)) return;
      }
    }
    throw new SecurityException(
        "Unauthorized to access " + userName + ", " + appId + " (" + hint + ")");
  }

  public static void checkPermissions(
      LlapTokenInfo prm, String userName, String appId, Object hint) {
    if (userName == null) {
      assert StringUtils.isEmpty(appId);
      return;
    }
    if (!checkTokenPermissions(userName, appId, prm.userName, prm.appId)) {
      throw new SecurityException("Unauthorized to access "
          + userName + ", " + appId + " (" + hint + ")");
    }
  }

  private static boolean checkTokenPermissions(
      String userName, String appId, String tokenUser, String tokenAppId) {
    return userName.equals(tokenUser)
        && (StringUtils.isBlank(appId) || appId.equals(tokenAppId));
  }
}
