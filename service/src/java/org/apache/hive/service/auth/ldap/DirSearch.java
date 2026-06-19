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

import java.io.Closeable;
import java.util.List;
import javax.naming.NamingException;

/**
 * The object used for executing queries on the Directory Service.
 */
public interface DirSearch extends Closeable {

  /**
   * Finds user's distinguished name.
   * @param user username
   * @return DN for the specified username
   * @throws NamingException
   */
  String findUserDn(String user) throws NamingException;

  /**
   * Finds user's distinguished name.
   * @param user username
   * @param userSearchFilter Generic LDAP Search filter for ex: (&amp;(uid={0})(objectClass=person))
   * @param baseDn LDAP BaseDN for user searches for ex: dc=apache,dc=org
   * @return DN for the specific user if exists, null otherwise
   * @throws NamingException
   */
  String findUserDn(String user, String userSearchFilter, String baseDn) throws NamingException;

  /**
   * Finds group's distinguished name.
   * @param group group name or unique identifier
   * @return DN for the specified group name
   * @throws NamingException
   */
  String findGroupDn(String group) throws NamingException;

  /**
   * Verifies that specified user is a member of specified group.
   * @param user user id or distinguished name
   * @param groupDn group's DN
   * @return {@code true} if the user is a member of the group, {@code false} - otherwise.
   * @throws NamingException
   */
  boolean isUserMemberOfGroup(String user, String groupDn) throws NamingException;

  /**
   * Finds groups that contain the specified user.
   * @param userDn user's distinguished name
   * @return list of groups
   * @throws NamingException
   */
  List<String> findGroupsForUser(String userDn) throws NamingException;

  /**
   * Executes an arbitrary query.
   * @param query any query
   * @return list of names in the namespace
   * @throws NamingException
   */
  List<String> executeCustomQuery(String query) throws NamingException;

  /**
   * Executes an arbitrary query.
   * @param user user RDN or username. This will be substituted for {0} in group search
   * @param userDn userDn DN for the username. This will be substituted for {1} in group search
   * @param filter filter is the group filter query ex: (&amp;(memberUid={0})(&amp;(CN=group1)(objectClass=posixGroup)))
   * @param groupBaseDn BaseDN for group searches. ex: "ou=groups,dc=apache,dc=org"
   * @return list of names that match the group filter aka groups that the user belongs to, if any.
   * @throws NamingException
   */
  List<String> executeUserAndGroupFilterQuery(String user, String userDn, String filter, String groupBaseDn) throws NamingException;
}
