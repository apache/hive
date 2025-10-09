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
package org.apache.hive.service.auth.ldap;

import com.google.common.base.Strings;

import java.util.List;
import javax.naming.NamingException;
import javax.security.sasl.AuthenticationException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory for a {@link Filter} based on user and group search filters.
 * <br>
 * The produced filter object filters out all users that are not found in the search result
 * of the query provided in Hive configuration.
 * Atleast one search criteria is REQUIRED.
 * Configuration could have Usersearch filter or Groupsearch filter or both.
 * @see HiveConf.ConfVars#HIVE_SERVER2_PLAIN_LDAP_USERSEARCHFILTER
 * @see HiveConf.ConfVars#HIVE_SERVER2_PLAIN_LDAP_BASEDN
 * @see HiveConf.ConfVars#HIVE_SERVER2_PLAIN_LDAP_GROUPSEARCHFILTER
 * @see HiveConf.ConfVars#HIVE_SERVER2_PLAIN_LDAP_GROUPBASEDN
 */
public class UserGroupSearchFilterFactory implements FilterFactory {

  /**
   * {@inheritDoc}
   */
  @Override
  public Filter getInstance(HiveConf conf) {
    String userSearchFilter = conf.get(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERSEARCHFILTER.varname);
    String userSearchBaseDN = conf.get(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BASEDN.varname);
    String groupSearchFilter = conf.get(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPSEARCHFILTER.varname);
    String groupSearchBaseDN = conf.get(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPBASEDN.varname);

    // Both UserSearch and GroupSearch cannot be null or empty.
    if (Strings.isNullOrEmpty(userSearchFilter) &&
        (Strings.isNullOrEmpty(groupSearchFilter) && Strings.isNullOrEmpty(groupSearchBaseDN))) {
      return null;
    }
    return new UserGroupSearchFilter(userSearchFilter, userSearchBaseDN, groupSearchFilter, groupSearchBaseDN);
  }

  private static final class UserGroupSearchFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(UserGroupSearchFilter.class);

    private final String userSearchFilter;
    private final String userBaseDN;
    private final String groupSearchFilter;
    private final String groupBaseDN;

    UserGroupSearchFilter(String userSearchFilter, String userBaseDN, String groupSearchFilter, String groupBaseDN) {
      this.userSearchFilter = userSearchFilter;
      this.userBaseDN = userBaseDN;
      this.groupSearchFilter = groupSearchFilter;
      this.groupBaseDN = groupBaseDN;
    }

    @Override public void apply(DirSearch client, String user) throws AuthenticationException {
      String userDn = null;
      List<String> resultList;
      try {
        if (!Strings.isNullOrEmpty(userSearchFilter) && !Strings.isNullOrEmpty(userBaseDN)) {
          userDn = client.findUserDn(user, userSearchFilter, userBaseDN);

          // This should not be null because we were allowed to bind with this username
          // safe check in case we were able to bind anonymously.
          if (userDn == null) {
            throw new AuthenticationException("Authentication failed: User search found no matching user");
          }
        }

        if (!Strings.isNullOrEmpty(groupSearchFilter) && !Strings.isNullOrEmpty(groupBaseDN)) {
          resultList = client.executeUserAndGroupFilterQuery(user, userDn, groupSearchFilter, groupBaseDN);
            if (resultList != null && resultList.size() > 0) {
              return;
            }
        } else if (userDn != null) { // succeed based on user search filter only
          return;
        }
        throw new AuthenticationException("Authentication failed: User search does not satisfy filter condition");
      } catch (NamingException e) {
        throw new AuthenticationException("LDAP Authentication failed for user", e);
      }
    }
  }
}
