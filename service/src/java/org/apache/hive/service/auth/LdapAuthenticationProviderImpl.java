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
package org.apache.hive.service.auth;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.ServiceUtils;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.security.sasl.AuthenticationException;

public class LdapAuthenticationProviderImpl implements PasswdAuthenticationProvider {

  private static final Log LOG        = LogFactory.getLog(LdapAuthenticationProviderImpl.class);
  private static final String DN_ATTR = "distinguishedName";

  private final String ldapURL;
  private final String baseDN;
  private final String ldapDomain;
  private static List<String> groupBases;
  private static List<String> userBases;
  private static List<String> userFilter;
  private static List<String> groupFilter;
  private final String customQuery;

  LdapAuthenticationProviderImpl() {
    HiveConf conf = new HiveConf();
    ldapURL       = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_URL);
    baseDN        = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BASEDN);
    ldapDomain    = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_DOMAIN);
    customQuery   = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_CUSTOMLDAPQUERY);

    if (customQuery == null) {
      groupBases               = new ArrayList<String>();
      userBases                = new ArrayList<String>();
      String groupDNPatterns   = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPDNPATTERN);
      String groupFilterVal    = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER);
      String userDNPatterns    = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERDNPATTERN);
      String userFilterVal     = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERFILTER);

      // parse COLON delimited root DNs for users/groups that may or may not be under BaseDN.
      // Expect the root DNs be fully qualified including the baseDN
      if (groupDNPatterns != null && groupDNPatterns.trim().length() > 0) {
        String[] groupTokens = groupDNPatterns.split(":");
        for (int i = 0; i < groupTokens.length; i++) {
          if (groupTokens[i].contains(",") && groupTokens[i].contains("=")) {
            groupBases.add(groupTokens[i]);
          } else {
            LOG.warn("Unexpected format for groupDNPattern..ignoring " + groupTokens[i]);
          }
        }
      } else {
        groupBases.add("CN=%s," + baseDN);
      }

      if (groupFilterVal != null && groupFilterVal.trim().length() > 0) {
        groupFilter     = new ArrayList<String>();
        String[] groups = groupFilterVal.split(",");
        for (int i = 0; i < groups.length; i++) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Filtered group: " + groups[i]);
          }
          groupFilter.add(groups[i]);
        }
      }

      if (userDNPatterns != null && userDNPatterns.trim().length() > 0) {
        String[] userTokens = userDNPatterns.split(":");
        for (int i = 0; i < userTokens.length; i++) {
          if (userTokens[i].contains(",") && userTokens[i].contains("=")) {
            userBases.add(userTokens[i]);
          } else {
            LOG.warn("Unexpected format for userDNPattern..ignoring " + userTokens[i]);
          }
        }
      } else {
        userBases.add("CN=%s," + baseDN);
      }

      if (userFilterVal != null && userFilterVal.trim().length() > 0) {
        userFilter     = new ArrayList<String>();
        String[] users = userFilterVal.split(",");
        for (int i = 0; i < users.length; i++) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Filtered user: " + users[i]);
          }
          userFilter.add(users[i]);
        }
      }
    }
  }

  @Override
  public void Authenticate(String user, String password) throws AuthenticationException {

    Hashtable<String, Object> env = new Hashtable<String, Object>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
    env.put(Context.PROVIDER_URL, ldapURL);

    // If the domain is available in the config, then append it unless domain is
    // already part of the username. LDAP providers like Active Directory use a
    // fully qualified user name like foo@bar.com.
    if (!hasDomain(user) && ldapDomain != null) {
      user  = user + "@" + ldapDomain;
    }

    if (password == null || password.isEmpty() || password.getBytes()[0] == 0) {
      throw new AuthenticationException("Error validating LDAP user:" +
          " a null or blank password has been provided");
    }

    // user being authenticated becomes the bindDN and baseDN or userDN is used to search
    env.put(Context.SECURITY_AUTHENTICATION, "simple");
    env.put(Context.SECURITY_PRINCIPAL, user);
    env.put(Context.SECURITY_CREDENTIALS, password);

    LOG.debug("Connecting using principal=" + user + " at url=" + ldapURL);

    DirContext ctx = null;
    String userDN = null;
    try {
      // Create initial context
      ctx = new InitialDirContext(env);

      if (userFilter == null && groupFilter == null && customQuery == null) {
        userDN = findUserDNByPattern(ctx, user);

        if (userDN == null) {
          userDN = findUserDNByName(ctx, baseDN, user);
        }

        // This should not be null because we were allowed to bind with this username
        // safe check in case we were able to bind anonymously.
        if (userDN == null) {
          throw new AuthenticationException("Authentication failed: User search failed");
        }
        return;
      }

      if (customQuery != null) {
        List<String> resultList = executeLDAPQuery(ctx, customQuery, baseDN);
        if (resultList != null) {
          for (String matchedDN : resultList) {
            if (matchedDN.split(",",2)[0].split("=",2)[1].equalsIgnoreCase(user)) {
              LOG.info("Authentication succeeded based on result set from LDAP query");
              return;
            }
          }
        }
        throw new AuthenticationException("Authentication failed: LDAP query " +
            "from property returned no data");
      }

      // This section checks if the user satisfies the specified user filter.
      if (userFilter != null && userFilter.size() > 0) {
        LOG.info("Authenticating user " + user + " using user filter");

        boolean success = false;
        for (String filteredUser : userFilter) {
          if (filteredUser.equalsIgnoreCase(user)) {
            LOG.debug("User filter partially satisfied");
            success = true;
            break;
          }
        }

        if (!success) {
          LOG.info("Authentication failed based on user membership");
          throw new AuthenticationException("Authentication failed: User not a member " +
              "of specified list");
        }

        userDN = findUserDNByPattern(ctx, user);
        if (userDN != null) {
          LOG.info("User filter entirely satisfied");
        } else {
          LOG.info("User " + user + " could not be found in the configured UserBaseDN," +
              "authentication failed");
          throw new AuthenticationException("Authentication failed: UserDN could not be " +
              "found in specified User base(s)");
        }
      }

      if (groupFilter != null && groupFilter.size() > 0) {
        LOG.debug("Authenticating user " + user + " using group membership:");

        // if only groupFilter is configured.
        if (userDN == null) {
          userDN = findUserDNByName(ctx, baseDN, user);
        }

        List<String> userGroups = getGroupsForUser(ctx, userDN);
        if (LOG.isDebugEnabled()) {
          LOG.debug("User member of :");
          prettyPrint(userGroups);
        }

        if (userGroups != null) {
          for (String elem : userGroups) {
            String shortName = ((elem.split(","))[0].split("="))[1];
            String groupDN   = elem.split(",", 2)[1];
            LOG.debug("Checking group:DN=" + elem + ",shortName=" + shortName +
                ",groupDN=" + groupDN);
            if (groupFilter.contains(shortName)) {
              LOG.info("Authentication succeeded based on group membership");
              return;
            }
          }
        }

        throw new AuthenticationException("Authentication failed: User not a member of " +
            "listed groups");
      }

      LOG.info("Simple password authentication succeeded");

    } catch (NamingException e) {
      throw new AuthenticationException("LDAP Authentication failed for user", e);
    } finally {
      try {
        if (ctx != null) {
          ctx.close();
        }
      } catch(Exception e) {
        LOG.warn("Exception when closing LDAP context:" + e.getMessage());
      }
    }
  }

  private boolean hasDomain(String userName) {
    return (ServiceUtils.indexOfDomainMatch(userName) > 0);
  }

  private static void prettyPrint(List<String> list) {
    for (String elem : list) {
      LOG.debug("    " + elem);
    }
  }

  private static void prettyPrint(Attributes attrs) {
    NamingEnumeration<? extends Attribute> set = attrs.getAll();
    try {
      NamingEnumeration<?> list = null;
      while (set.hasMore()) {
        Attribute attr = set.next();
        list = attr.getAll();
        String attrVals = "";
        while (list.hasMore()) {
          attrVals += list.next() + "+";
        }
        LOG.debug(attr.getID() + ":::" + attrVals);
      }
    } catch (Exception e) {
      System.out.println("Error occurred when reading ldap data:" + e.getMessage());
    }
  }

  /**
   * This helper method attempts to find a DN given a unique groupname.
   * Various LDAP implementations have different keys/properties that store this unique ID.
   * So the first attempt is to find an entity with objectClass=group && CN=groupName
   * @param ctx DirContext for the LDAP Connection.
   * @param baseDN BaseDN for this LDAP directory where the search is to be performed.
   * @param groupName A unique groupname that is to be located in the LDAP.
   * @return LDAP DN if the group is found in LDAP, null otherwise.
   */
  public static String findGroupDNByName(DirContext ctx, String baseDN, String groupName)
    throws NamingException {
    String searchFilter  = "(&(objectClass=group)(CN=" + groupName + "))";
    List<String> results = null;

    results = findDNByName(ctx, baseDN, searchFilter, 2);

    if (results == null) {
      return null;
    } else if (results.size() > 1) {
      //make sure there is not another item available, there should be only 1 match
      LOG.info("Matched multiple groups for the group: " + groupName + ",returning null");
      return null;
    }
    return results.get(0);
  }

  /**
   * This helper method attempts to find an LDAP group entity given a unique name using a
   * user-defined pattern for GROUPBASE.The list of group bases is defined by the user via property
   * "hive.server2.authentication.ldap.groupDNPattern" in the hive-site.xml.
   * Users can use %s where the actual groupname is to be substituted in the LDAP Query.
   * @param ctx DirContext for the LDAP Connection.
   * @param groupName A unique groupname that is to be located in the LDAP.
   * @return LDAP DN of given group if found in the directory, null otherwise.
   */
  public static String findGroupDNByPattern(DirContext ctx, String groupName)
      throws NamingException {
    return findDNByPattern(ctx, groupName, groupBases);
  }

  public static String findDNByPattern(DirContext ctx, String name, List<String> nodes)
      throws NamingException {
    String searchFilter;
    String searchBase;
    SearchResult searchResult = null;
    NamingEnumeration<SearchResult> results;

    String[] returnAttributes     = { DN_ATTR };
    SearchControls searchControls = new SearchControls();

    searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    searchControls.setReturningAttributes(returnAttributes);

    for (String node : nodes) {
      searchFilter = "(" + DN_ATTR + "=" + node.replaceAll("%s", name) + ")";
      searchBase   = node.split(",",2)[1];
      results      = ctx.search(searchBase, searchFilter, searchControls);

      if(results.hasMoreElements()) {
        searchResult = results.nextElement();
        //make sure there is not another item available, there should be only 1 match
        if(results.hasMoreElements()) {
          LOG.warn("Matched multiple entities for the name: " + name);
          return null;
        }
        return (String)searchResult.getAttributes().get(DN_ATTR).get();
      }
    }
    return null;
  }

  /**
   * This helper method attempts to find a DN given a unique username.
   * Various LDAP implementations have different keys/properties that store this unique userID.
   * Active Directory has a "sAMAccountName" that appears reliable,openLDAP uses "uid"
   * So the first attempt is to find an entity with objectClass=person||user where
   * (uid||sAMAccountName) matches the given username.
   * The second attempt is to use CN attribute for wild card matching and then match the
   * username in the DN.
   * @param ctx DirContext for the LDAP Connection.
   * @param baseDN BaseDN for this LDAP directory where the search is to be performed.
   * @param userName A unique userid that is to be located in the LDAP.
   * @return LDAP DN if the user is found in LDAP, null otherwise.
   */
  public static String findUserDNByName(DirContext ctx, String baseDN, String userName)
      throws NamingException {
    String baseFilter    = "(&(|(objectClass=person)(objectClass=user))";
    String suffix[]      = new String[] {
                             "(|(uid=" + userName + ")(sAMAccountName=" + userName + ")))",
                             "(|(cn=*" + userName + "*)))"
                           };
    String searchFilter  = null;
    List<String> results = null;

    for (int i = 0; i < suffix.length; i++) {
      searchFilter = baseFilter + suffix[i];
      results      = findDNByName(ctx, baseDN, searchFilter, 2);

      if(results == null) {
        continue;
      }

      if(results != null && results.size() > 1) {
        //make sure there is not another item available, there should be only 1 match
        LOG.info("Matched multiple users for the user: " + userName + ",returning null");
        return null;
      }
      return results.get(0);
    }
    return null;
  }

  public static List<String> findDNByName(DirContext ctx, String baseDN,
      String searchString, int limit) throws NamingException {
    SearchResult searchResult     = null;
    List<String> retValues        = null;
    String matchedDN              = null;
    SearchControls searchControls = new SearchControls();
    String[] returnAttributes     = { DN_ATTR };

    searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    searchControls.setReturningAttributes(returnAttributes);
    if (limit > 0) {
      searchControls.setCountLimit(limit); // limit the result set to limit the size of resultset
    }

    NamingEnumeration<SearchResult> results = ctx.search(baseDN, searchString, searchControls);
    while(results.hasMoreElements()) {
      searchResult = results.nextElement();
      matchedDN    = (String)searchResult.getAttributes().get(DN_ATTR).get();

      if (retValues == null) {
        retValues = new ArrayList<String>();
      }
      retValues.add(matchedDN);
    }
    return retValues;
  }

  /**
   * This helper method attempts to find a UserDN given a unique username from a
   * user-defined pattern for USERBASE. The list of user bases is defined by the user
   * via property "hive.server2.authentication.ldap.userDNPattern" in the hive-site.xml.
   * Users can use %s where the actual username is to be subsituted in the LDAP Query.
   * @param ctx DirContext for the LDAP Connection.
   * @param userName A unique userid that is to be located in the LDAP.
   * @return LDAP DN of given user if found in the directory, null otherwise.
   */
  public static String findUserDNByPattern(DirContext ctx, String userName)
      throws NamingException {
    return findDNByPattern(ctx, userName, userBases);
  }

  /**
   * This helper method finds all the groups a given user belongs to.
   * This method relies on the "memberOf" attribute being set on the user that references
   * the group the group. The returned list ONLY includes direct groups the user belongs to.
   * Parent groups of these direct groups are NOT included.
   * @param ctx DirContext for the LDAP Connection.
   * @param userName A unique userid that is to be located in the LDAP.
   * @return List of Group DNs the user belongs to, emptylist otherwise.
   */
  public static List<String> getGroupsForUser(DirContext ctx, String userDN)
      throws NamingException {
    List<String> groupList        = new ArrayList<String>();
    String searchFilter           = "(" + DN_ATTR + "=" + userDN + ")";
    SearchControls searchControls = new SearchControls();

    LOG.debug("getGroupsForUser:searchFilter=" + searchFilter);
    String[] attrIDs = { "memberOf" };
    searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    searchControls.setReturningAttributes(attrIDs);

    // treat everything after the first COMMA as a baseDN for the search to find this user
    NamingEnumeration<SearchResult> results = ctx.search(userDN.split(",",2)[1], searchFilter,
        searchControls);
    while(results.hasMoreElements()) {
      NamingEnumeration<? extends Attribute> groups = results.next().getAttributes().getAll();
      while (groups.hasMore()) {
        Attribute attr = groups.next();
        NamingEnumeration<?> list = attr.getAll();
        while (list.hasMore()) {
          groupList.add((String)list.next());
        }
      }
    }
    return groupList;
  }

  /**
   * This method helps execute a LDAP query defined by the user via property
   * "hive.server2.authentication.ldap.customLDAPQuery"
   * A full LDAP query that LDAP Atn provider uses to execute against LDAP Server.
   * If this query return a null resultset, the LDAP Provider fails the authentication request.
   * If the LDAP query returns a list of DNs, a check is performed to confirm one
   * of the entries is for the user being authenticated.
   * For example: (&(objectClass=group)(objectClass=top)(instanceType=4)(cn=Domain*))
   * (&(objectClass=person)(|(sAMAccountName=admin)
   *                       (|(memberOf=CN=Domain Admins,CN=Users,DC=domain,DC=com)
   *                         (memberOf=CN=Administrators,CN=Builtin,DC=domain,DC=com))))
   * @param ctx DirContext to execute this query within.
   * @param query User-defined LDAP Query string to be used to authenticate users.
   * @param rootDN BaseDN at which to execute the LDAP query, typically rootDN for the LDAP.
   * @return List of LDAP DNs returned from executing the LDAP Query.
   */
  public static List<String> executeLDAPQuery(DirContext ctx, String query, String rootDN)
      throws NamingException {
    SearchControls searchControls = new SearchControls();
    List<String> list             = new ArrayList<String>();
    String[] returnAttributes     = { DN_ATTR };

    searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    searchControls.setReturningAttributes(returnAttributes);

    LOG.info("Using a user specified LDAP query for adjudication:" + query + ",baseDN=" + rootDN);
    NamingEnumeration<SearchResult> results = ctx.search(rootDN, query, searchControls);
    SearchResult searchResult = null;
    while(results.hasMoreElements()) {
      searchResult = results.nextElement();
      list.add((String)searchResult.getAttributes().get(DN_ATTR).get());
      LOG.debug("LDAPAtn:executeLDAPQuery()::Return set size " + list.get(list.size() - 1));
    }
    return list;
  }
}
