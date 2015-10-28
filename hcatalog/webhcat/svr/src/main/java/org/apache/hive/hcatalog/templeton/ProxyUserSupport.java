/**
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.Groups;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * When WebHCat is run with doAs query parameter this class ensures that user making the
 * call is allowed to impersonate doAs user and is making a call from authorized host.
 */
final class ProxyUserSupport {
  private static final Logger LOG = LoggerFactory.getLogger(ProxyUserSupport.class);
  private static final String CONF_PROXYUSER_PREFIX = "webhcat.proxyuser.";
  private static final String CONF_GROUPS_SUFFIX = ".groups";
  private static final String CONF_HOSTS_SUFFIX = ".hosts";
  private static final Set<String> WILD_CARD = Collections.unmodifiableSet(new HashSet<String>(0));
  private static final Map<String, Set<String>> proxyUserGroups = new HashMap<String, Set<String>>();
  private static final Map<String, Set<String>> proxyUserHosts = new HashMap<String, Set<String>>();

  static void processProxyuserConfig(AppConfig conf) {
    for(Map.Entry<String, String> confEnt : conf) {
      if(confEnt.getKey().startsWith(CONF_PROXYUSER_PREFIX)
          && confEnt.getKey().endsWith(CONF_GROUPS_SUFFIX)) {
        //process user groups for which doAs is authorized
        String proxyUser = 
            confEnt.getKey().substring(CONF_PROXYUSER_PREFIX.length(), 
            confEnt.getKey().lastIndexOf(CONF_GROUPS_SUFFIX));
        Set<String> groups;
        if("*".equals(confEnt.getValue())) {
          groups = WILD_CARD;
          if(LOG.isDebugEnabled()) {
            LOG.debug("User [" + proxyUser + "] is authorized to do doAs any user.");
          }
        }
        else if(confEnt.getValue() != null && confEnt.getValue().trim().length() > 0) {
          groups = new HashSet<String>(Arrays.asList(confEnt.getValue().trim().split(",")));
          if(LOG.isDebugEnabled()) {
            LOG.debug("User [" + proxyUser + 
                "] is authorized to do doAs for users in the following groups: ["
                + confEnt.getValue().trim() + "]");
          }
        }
        else {
          groups = Collections.emptySet();
          if(LOG.isDebugEnabled()) {
            LOG.debug("User [" + proxyUser + 
                "] is authorized to do doAs for users in the following groups: []");
          }
        }
        proxyUserGroups.put(proxyUser, groups);
      }
      else if(confEnt.getKey().startsWith(CONF_PROXYUSER_PREFIX)
          && confEnt.getKey().endsWith(CONF_HOSTS_SUFFIX)) {
        //process hosts from which doAs requests are authorized
        String proxyUser = confEnt.getKey().substring(CONF_PROXYUSER_PREFIX.length(), 
            confEnt.getKey().lastIndexOf(CONF_HOSTS_SUFFIX));
        Set<String> hosts;
        if("*".equals(confEnt.getValue())) {
          hosts = WILD_CARD;
          if(LOG.isDebugEnabled()) {
            LOG.debug("User [" + proxyUser + "] is authorized to do doAs from any host.");
          }
        }
        else if(confEnt.getValue() != null && confEnt.getValue().trim().length() > 0) {
          String[] hostValues = confEnt.getValue().trim().split(",");
          hosts = new HashSet<String>();
          for(String hostname : hostValues) {
            String nhn = normalizeHostname(hostname);
            if(nhn != null) {
              hosts.add(nhn);
            }
          }
          if(LOG.isDebugEnabled()) {
            LOG.debug("User [" + proxyUser + 
                "] is authorized to do doAs from the following hosts: ["
                + confEnt.getValue().trim() + "]");
          }
        }
        else {
          hosts = Collections.emptySet();
          if(LOG.isDebugEnabled()) {
            LOG.debug("User [" + proxyUser
                + "] is authorized to do doAs from the following hosts: []");
          }
        }
        proxyUserHosts.put(proxyUser, hosts);
      }
    }
  }
  /**
   * Verifies a that proxyUser is making the request from authorized host and that doAs user
   * belongs to one of the groups for which proxyUser is allowed to impersonate users.
   *
   * @param proxyUser user name of the proxy (logged in) user.
   * @param proxyHost host the proxy user is making the request from.
   * @param doAsUser user the proxy user is impersonating.
   * @throws NotAuthorizedException thrown if the user is not allowed to perform the proxyuser request.
   */
  static void validate(String proxyUser, String proxyHost, String doAsUser) throws 
      NotAuthorizedException {
    assertNotEmpty(proxyUser, "proxyUser",
        "If you're attempting to use user-impersonation via a proxy user, please make sure that "
            + CONF_PROXYUSER_PREFIX + "#USER#" + CONF_HOSTS_SUFFIX + " and "
            + CONF_PROXYUSER_PREFIX + "#USER#" + CONF_GROUPS_SUFFIX
            + " are configured correctly");
    assertNotEmpty(proxyHost, "proxyHost",
        "If you're attempting to use user-impersonation via a proxy user, please make sure that "
            + CONF_PROXYUSER_PREFIX + proxyUser + CONF_HOSTS_SUFFIX + " and "
            + CONF_PROXYUSER_PREFIX + proxyUser + CONF_GROUPS_SUFFIX
            + " are configured correctly");
    assertNotEmpty(doAsUser, Server.DO_AS_PARAM);
    LOG.debug(MessageFormat.format("Authorization check proxyuser [{0}] host [{1}] doAs [{2}]",
        proxyUser, proxyHost, doAsUser));
    if (proxyUserHosts.containsKey(proxyUser)) {
      proxyHost = normalizeHostname(proxyHost);
      validateRequestorHost(proxyUser, proxyHost);
      validateGroup(proxyUser, doAsUser);
    }
    else {
      throw new NotAuthorizedException(MessageFormat.format(
          "User [{0}] not defined as proxyuser", proxyUser));
    }
  }

  private static void validateRequestorHost(String proxyUser, String hostname) throws 
      NotAuthorizedException {
    Set<String> validHosts = proxyUserHosts.get(proxyUser);
    if (validHosts == WILD_CARD) {
      return;
    }
    if (validHosts == null || !validHosts.contains(hostname)) {
      throw new NotAuthorizedException(MessageFormat.format(
          "Unauthorized host [{0}] for proxyuser [{1}]", hostname, proxyUser));
    }
  }

  private static void validateGroup(String proxyUser, String doAsUser) throws 
      NotAuthorizedException {
    Set<String> validGroups = proxyUserGroups.get(proxyUser);
    if(validGroups == WILD_CARD) {
      return;
    }
    else if(validGroups == null || validGroups.isEmpty()) {
      throw new NotAuthorizedException(
          MessageFormat.format(
            "Unauthorized proxyuser [{0}] for doAsUser [{1}], not in proxyuser groups",
            proxyUser, doAsUser));
    }
    Groups groupsInfo = new Groups(Main.getAppConfigInstance());
    try {
      List<String> userGroups = groupsInfo.getGroups(doAsUser);
      for (String g : validGroups) {
        if (userGroups.contains(g)) {
          return;
        }
      }
    }
    catch (IOException ex) {//thrown, for example, if there is no such user on the system
      LOG.warn(MessageFormat.format("Unable to get list of groups for doAsUser [{0}].",
          doAsUser), ex);
    }
    throw new NotAuthorizedException(
      MessageFormat.format(
        "Unauthorized proxyuser [{0}] for doAsUser [{1}], not in proxyuser groups",
            proxyUser, doAsUser));
  }

  private static String normalizeHostname(String name) {
    try {
      InetAddress address = InetAddress.getByName( 
          "localhost".equalsIgnoreCase(name) ? null : name);
      return address.getCanonicalHostName();
    }
    catch (UnknownHostException ex) {
      LOG.warn(MessageFormat.format("Unable to normalize hostname [{0}]", name));
      return null;
    }
  }
  /**
   * Check that a string is not null and not empty. If null or empty 
   * throws an IllegalArgumentException.
   *
   * @param str value.
   * @param name parameter name for the exception message.
   * @return the given value.
   */
  private static String assertNotEmpty(String str, String name) {
    return assertNotEmpty(str, name, null);
  }

  /**
   * Check that a string is not null and not empty. If null or empty 
   * throws an IllegalArgumentException.
   *
   * @param str value.
   * @param name parameter name for the exception message.
   * @param info additional information to be printed with the exception message
   * @return the given value.
   */
  private static String assertNotEmpty(String str, String name, String info) {
    if (str == null) {
      throw new IllegalArgumentException(
          name + " cannot be null" + (info == null ? "" : ", " + info));
    }
    if (str.length() == 0) {
      throw new IllegalArgumentException(
          name + " cannot be empty" + (info == null ? "" : ", " + info));
    }
    return str;
  }
}
