/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.common.util;

import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Sets;

/**
 * Parser for extracting ACL information from Configs
 */
@Private
public class ACLConfigurationParser {

  private static final Logger LOG =
      LoggerFactory.getLogger(ACLConfigurationParser.class);

  private static final String WILDCARD_ACL_VALUE = "*";
  private static final Pattern splitPattern = Pattern.compile("\\s+");

  private final Set<String> allowedUsers;
  private final Set<String> allowedGroups;

  public ACLConfigurationParser(Configuration conf, String confPropertyName) {
    allowedUsers = Sets.newLinkedHashSet();
    allowedGroups = Sets.newLinkedHashSet();
    parse(conf, confPropertyName);
  }


  private boolean isWildCard(String aclStr) {
    return aclStr.trim().equals(WILDCARD_ACL_VALUE);
  }

  private void parse(Configuration conf, String configProperty) {
    String aclsStr = conf.get(configProperty);
    if (aclsStr == null || aclsStr.isEmpty()) {
      return;
    }
    if (isWildCard(aclsStr)) {
      allowedUsers.add(WILDCARD_ACL_VALUE);
      return;
    }

    final String[] splits = splitPattern.split(aclsStr);
    int counter = -1;
    String userListStr = null;
    String groupListStr = null;
    for (String s : splits) {
      if (s.isEmpty()) {
        if (userListStr != null) {
          continue;
        }
      }
      ++counter;
      if (counter == 0) {
        userListStr = s;
      } else if (counter == 1) {
        groupListStr = s;
      } else {
        LOG.warn("Invalid configuration specified for " + configProperty
            + ", ignoring configured ACLs, value=" + aclsStr);
        return;
      }
    }

    if (userListStr == null) {
      return;
    }
    if (userListStr.length() >= 1) {
      allowedUsers.addAll(
          org.apache.hadoop.util.StringUtils.getTrimmedStringCollection(userListStr));
    }
    if (groupListStr != null && groupListStr.length() >= 1) {
      allowedGroups.addAll(
          org.apache.hadoop.util.StringUtils.getTrimmedStringCollection(groupListStr));
    }
  }

  public Set<String> getAllowedUsers() {
    return Collections.unmodifiableSet(allowedUsers);
  }

  public Set<String> getAllowedGroups() {
    return Collections.unmodifiableSet(allowedGroups);
  }

  public void addAllowedUser(String user) {
    if (StringUtils.isBlank(user)) {
      return;
    }
    if (allowedUsers.contains(WILDCARD_ACL_VALUE)) {
      return;
    }
    if (user.equals(WILDCARD_ACL_VALUE)) {
      allowedUsers.clear();
      allowedGroups.clear();
    }
    allowedUsers.add(user);
  }

  public void addAllowedGroup(String group) {
    allowedGroups.add(group);
  }

  public String toAclString() {
    return toString();
  }

  @Override
  public String toString() {
    if (getAllowedUsers().contains(WILDCARD_ACL_VALUE)) {
      return WILDCARD_ACL_VALUE;
    } else {
      if (allowedUsers.size() == 0 && allowedGroups.size() == 0) {
        return " ";
      }
      String userString = constructCsv(allowedUsers);
      String groupString = "";
      if (allowedGroups.size() > 0) {
        groupString = " " + constructCsv(allowedGroups);
      }
      return userString + groupString;
    }
  }

  private String constructCsv(Set<String> inSet) {
    StringBuilder sb = new StringBuilder();
    if (inSet != null) {
      boolean isFirst = true;
      for (String s : inSet) {
        if (!isFirst) {
          sb.append(",");
        } else {
          isFirst = false;
        }
        sb.append(s);
      }
    }
    return sb.toString();
  }

}
