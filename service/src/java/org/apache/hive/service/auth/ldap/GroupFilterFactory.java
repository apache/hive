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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.service.auth.ldap;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.naming.NamingException;
import javax.security.sasl.AuthenticationException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory for a {@link Filter} based on a list of allowed groups.
 * <br>
 * The produced filter object filters out all users that are not members of at least one of
 * the groups provided in Hive configuration.
 * @see HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER
 */
public final class GroupFilterFactory implements FilterFactory {

  /**
   * {@inheritDoc}
   */
  @Override
  public Filter getInstance(HiveConf conf) {
    Collection<String> groupFilter = conf.getStringCollection(
        HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER.varname);

    if (groupFilter.isEmpty()) {
      return null;
    }

    return new GroupFilter(groupFilter);
  }

  private static final class GroupFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(GroupFilter.class);

    private final Set<String> groupFilter = new HashSet<>();

    GroupFilter(Collection<String> groupFilter) {
      this.groupFilter.addAll(groupFilter);
    }

    @Override
    public void apply(DirSearch ldap, String user) throws AuthenticationException {
      LOG.info("Authenticating user '{}' using group membership", user);

      List<String> memberOf = null;

      try {
        String userDn = ldap.findUserDn(user);
        memberOf = ldap.findGroupsForUser(userDn);
        LOG.debug("User {} member of : {}", userDn, memberOf);
      } catch (NamingException e) {
        throw new AuthenticationException("LDAP Authentication failed for user", e);
      }

      for (String groupDn : memberOf) {
        String shortName = LdapUtils.getShortName(groupDn);
        if (groupFilter.contains(shortName)) {
          LOG.info("Authentication succeeded based on group membership");
          return;
        }
      }
      LOG.info("Authentication failed based on user membership");
      throw new AuthenticationException("Authentication failed: "
          + "User not a member of specified list");
    }
  }
}
