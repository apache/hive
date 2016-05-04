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

import java.util.ArrayList;

import java.util.List;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LlapTokenChecker {
  private static final Logger LOG = LoggerFactory.getLogger(LlapTokenChecker.class);

  private static final ImmutablePair<String, String> NO_SECURITY = new ImmutablePair<>(null, null);
  public static Pair<String, String> getTokenInfo(String clusterId) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) return NO_SECURITY;
    UserGroupInformation current = UserGroupInformation.getCurrentUser();
    String kerberosName = current.hasKerberosCredentials() ? current.getShortUserName() : null;
    List<LlapTokenIdentifier> tokens = getLlapTokens(current, clusterId);
    if ((tokens == null || tokens.isEmpty()) && kerberosName == null) {
      throw new SecurityException("No tokens or kerberos for " + current);
    }
    return getTokenInfoInternal(kerberosName, tokens);
  }

  private static List<LlapTokenIdentifier> getLlapTokens(
      UserGroupInformation ugi, String clusterId) {
    List<LlapTokenIdentifier> tokens = null;
    for (TokenIdentifier id : ugi.getTokenIdentifiers()) {
      if (!LlapTokenIdentifier.KIND_NAME.equals(id.getKind())) continue;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Token {}", id);
      }
      LlapTokenIdentifier llapId = (LlapTokenIdentifier)id;
      if (!clusterId.equals(llapId.getClusterId())) continue;
      if (tokens == null) {
        tokens = new ArrayList<>();
      }
      tokens.add((LlapTokenIdentifier)id);
    }
    return tokens;
  }

  @VisibleForTesting
  static Pair<String, String> getTokenInfoInternal(
      String kerberosName, List<LlapTokenIdentifier> tokens) {
    assert (tokens != null && !tokens.isEmpty()) || kerberosName != null;
    if (tokens == null) {
      return new ImmutablePair<String, String>(kerberosName, null);
    }
    String userName = kerberosName, appId = null;
    for (LlapTokenIdentifier llapId : tokens) {
      String newUserName = llapId.getRealUser().toString();
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
    }
    assert userName != null;
    return new ImmutablePair<String, String>(userName, appId);
  }

  public static void checkPermissions(
      String clusterId, String userName, String appId, Object hint) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) return;
    UserGroupInformation current = UserGroupInformation.getCurrentUser();
    String kerberosName = current.hasKerberosCredentials() ? current.getShortUserName() : null;
    List<LlapTokenIdentifier> tokens = getLlapTokens(current, clusterId);
    checkPermissionsInternal(kerberosName, tokens, userName, appId, hint);
  }

  @VisibleForTesting
  static void checkPermissionsInternal(String kerberosName, List<LlapTokenIdentifier> tokens,
      String userName, String appId, Object hint) {
    if (kerberosName != null && StringUtils.isEmpty(appId) && kerberosName.equals(userName)) {
      return;
    }
    if (tokens != null) {
      for (LlapTokenIdentifier llapId : tokens) {
        String tokenUser = llapId.getRealUser().toString(), tokenAppId = llapId.getAppId();
        if (checkTokenPermissions(userName, appId, tokenUser, tokenAppId)) return;
      }
    }
    throw new SecurityException("Unauthorized to access "
        + userName + ", " + appId.hashCode() + " (" + hint + ")");
  }

  public static void checkPermissions(
      Pair<String, String> prm, String userName, String appId, Object hint) {
    if (userName == null) {
      assert StringUtils.isEmpty(appId);
      return;
    }
    if (!checkTokenPermissions(userName, appId, prm.getLeft(), prm.getRight())) {
      throw new SecurityException("Unauthorized to access "
          + userName + ", " + appId.hashCode() + " (" + hint + ")");
    }
  }

  private static boolean checkTokenPermissions(
      String userName, String appId, String tokenUser, String tokenAppId) {
    return userName.equals(tokenUser)
        && (StringUtils.isEmpty(appId) || appId.equals(tokenAppId));
  }
}