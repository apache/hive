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
import java.util.ListIterator;

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

  private String ldapURL;
  private String baseDN;
  private String ldapDomain;
  private static List<String> groupBases;
  private static List<String> userBases;
  private static List<String> userFilter;
  private static List<String> groupFilter;
  private String customQuery;
  private static String guid_attr;
  private static String groupMembership_attr;
  private static String groupClass_attr;

  LdapAuthenticationProviderImpl() {
    HiveConf conf = new HiveConf();
    init(conf);
  }

  protected void init(HiveConf conf) {
    ldapURL     = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_URL);
    baseDN      = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BASEDN);
    ldapDomain  = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_DOMAIN);
    customQuery = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_CUSTOMLDAPQUERY);
    guid_attr   = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GUIDKEY);
    groupBases  = new ArrayList<String>();
    userBases   = new ArrayList<String>();
    userFilter  = new ArrayList<String>();
    groupFilter = new ArrayList<String>();

    String groupDNPatterns = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPDNPATTERN);
    String groupFilterVal  = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER);
    String userDNPatterns  = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERDNPATTERN);
    String userFilterVal   = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERFILTER);
    groupMembership_attr   = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPMEMBERSHIP_KEY);
    groupClass_attr        = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPCLASS_KEY);

    // parse COLON delimited root DNs for users/groups that may or may not be under BaseDN.
    // Expect the root DNs be fully qualified including the baseDN
    if (groupDNPatterns != null && groupDNPatterns.trim().length() > 0) {
      String[] groupTokens = groupDNPatterns.split(":");
      for (int i = 0; i < groupTokens.length; i++) {
        if (groupTokens[i].contains(",") && groupTokens[i].contains("=")) {
          groupBases.add(groupTokens[i]);
        } else {
          LOG.warn("Unexpected format for " + HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPDNPATTERN
                       + "..ignoring " + groupTokens[i]);
        }
      }
    } else if (baseDN != null) {
      groupBases.add(guid_attr + "=%s," + baseDN);
    }

    if (groupFilterVal != null && groupFilterVal.trim().length() > 0) {
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
          LOG.warn("Unexpected format for " + HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERDNPATTERN
                       + "..ignoring " + userTokens[i]);
        }
      }
    } else if (baseDN != null) {
      userBases.add(guid_attr + "=%s," + baseDN);
    }

    if (userFilterVal != null && userFilterVal.trim().length() > 0) {
      String[] users = userFilterVal.split(",");
      for (int i = 0; i < users.length; i++) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Filtered user: " + users[i]);
        }
        userFilter.add(users[i]);
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

    env.put(Context.SECURITY_AUTHENTICATION, "simple");
    env.put(Context.SECURITY_CREDENTIALS, password);

    // setup the security principal
    String bindDN   = null;
    DirContext ctx  = null;
    String userDN   = null;
    String userName = null;
    Exception ex    = null;

    if (!isDN(user) && !hasDomain(user) && userBases.size() > 0) {
      ListIterator<String> listIter = userBases.listIterator();
      while (listIter.hasNext()) {
        try {
          bindDN = listIter.next().replaceAll("%s", user);
          env.put(Context.SECURITY_PRINCIPAL, bindDN);
          LOG.debug("Connecting using DN " + bindDN + " at url " + ldapURL);
          ctx = new InitialDirContext(env);
          break;
        } catch (NamingException e) {
          ex = e;
        }
      }
    } else {
      env.put(Context.SECURITY_PRINCIPAL, user);
      LOG.debug("Connecting using principal " + user + " at url " + ldapURL);
      try {
        ctx = new InitialDirContext(env);
      } catch (NamingException e) {
        ex = e;
      }
    }

    if (ctx == null) {
      LOG.debug("Could not connect to the LDAP Server:Authentication failed for " + user);
      throw new AuthenticationException("LDAP Authentication failed for user", ex);
    }

    LOG.debug("Connected using principal=" + user + " at url=" + ldapURL);
    try {
      if (isDN(user) || hasDomain(user)) {
        userName = extractName(user);
      } else {
        userName = user;
      }

      // if a custom LDAP query is specified, it takes precedence over other configuration properties.
      // if the user being authenticated is part of the resultset from the custom query, it succeeds.
      if (customQuery != null) {
        List<String> resultList = executeLDAPQuery(ctx, customQuery, baseDN);
        if (resultList != null) {
          for (String matchedDN : resultList) {
            LOG.info("<queried user=" + matchedDN.split(",",2)[0].split("=",2)[1] + ",user=" + user + ">");
            if (matchedDN.split(",",2)[0].split("=",2)[1].equalsIgnoreCase(user) ||
                matchedDN.equalsIgnoreCase(user)) {
              LOG.info("Authentication succeeded based on result set from LDAP query");
              return;
            }
          }
        }
        LOG.info("Authentication failed based on result set from custom LDAP query");
        throw new AuthenticationException("Authentication failed: LDAP query " +
            "from property returned no data");
      } else if (userBases.size() > 0) {
        if (isDN(user)) {
          userDN = findUserDNByDN(ctx, user);
        } else {
          if (userDN == null) {
            userDN = findUserDNByPattern(ctx, userName);
          }

          if (userDN == null) {
            userDN = findUserDNByName(ctx, userName);
          }
        }

        // This should not be null because we were allowed to bind with this username
        // safe check in case we were able to bind anonymously.
        if (userDN == null) {
          throw new AuthenticationException("Authentication failed: User search failed");
        }

        // This section checks if the user satisfies the specified user filter.
        if (userFilter.size() > 0) {
          LOG.info("Authenticating user " + user + " using user filter");

          if (userDN != null) {
            LOG.info("User filter partially satisfied");
          }

          boolean success = false;
          for (String filteredUser : userFilter) {
            if (filteredUser.equalsIgnoreCase(userName)) {
              LOG.debug("User filter entirely satisfied");
              success = true;
              break;
            }
          }

          if (!success) {
            LOG.info("Authentication failed based on user membership");
            throw new AuthenticationException("Authentication failed: User not a member " +
                "of specified list");
          }
        }

        // This section checks if the user satisfies the specified user filter.
        if (groupFilter.size() > 0) {
          LOG.debug("Authenticating user " + user + " using group membership");
          List<String> userGroups = getGroupsForUser(ctx, userDN);
          if (LOG.isDebugEnabled()) {
            LOG.debug("User member of :");
            prettyPrint(userGroups);
          }

          if (userGroups != null) {
            for (String elem : userGroups) {
              String shortName = ((elem.split(","))[0].split("="))[1];
              if (groupFilter.contains(shortName)) {
                LOG.info("Authentication succeeded based on group membership");
                return;
              }
            }
          }

          LOG.debug("Authentication failed: User is not a member of configured groups");
          throw new AuthenticationException("Authentication failed: User not a member of " +
              "listed groups");
        }
        LOG.info("Authentication succeeded using ldap user search");
        return;
      }
      // Ideally we should not be here. Indicates partially configured LDAP Service.
      // We allow it for now for backward compatibility.
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
    String searchFilter  = "(&(objectClass=" + groupClass_attr + ")(" + guid_attr + "=" + groupName + "))";
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

    String[] returnAttributes     = new String[0]; // empty set
    SearchControls searchControls = new SearchControls();

    searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    searchControls.setReturningAttributes(returnAttributes);

    for (String node : nodes) {
      searchFilter = "(" + (node.substring(0,node.indexOf(","))).replaceAll("%s", name) + ")";
      searchBase   = node.split(",",2)[1];
      results      = ctx.search(searchBase, searchFilter, searchControls);

      if(results.hasMoreElements()) {
        searchResult = results.nextElement();
        //make sure there is not another item available, there should be only 1 match
        if(results.hasMoreElements()) {
          LOG.warn("Matched multiple entities for the name: " + name);
          return null;
        }
        return searchResult.getNameInNamespace();
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
  public static String findUserDNByName(DirContext ctx, String userName)
      throws NamingException {
    if (userBases.size() == 0) {
      return null;
    }

    String baseFilter    = "(&(|(objectClass=person)(objectClass=user)(objectClass=inetOrgPerson))";
    String suffix[]      = new String[] {
                             "(|(uid=" + userName + ")(sAMAccountName=" + userName + ")))",
                             "(|(cn=*" + userName + "*)))"
                           };

    String searchFilter           = null;
    List<String> results          = null;
    ListIterator<String> listIter = userBases.listIterator();

    for (int i = 0; i < suffix.length; i++) {
      searchFilter = baseFilter + suffix[i];

      while (listIter.hasNext()) {
        results = findDNByName(ctx, listIter.next().split(",",2)[1], searchFilter, 2);

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
    }
    return null;
  }

  /**
   * This helper method attempts to find a username given a DN.
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
  public static String findUserDNByDN(DirContext ctx, String userDN)
      throws NamingException {
    if (!isDN(userDN)) {
      return null;
    }

    String baseDN        = extractBaseDN(userDN);
    List<String> results = null;
    // we are using the first part of the userDN in the search criteria.
    // We know the DN is legal as we are able to bind with it, this is to confirm that its a user.
    String searchFilter  = "(&(|(objectClass=person)(objectClass=user)(objectClass=inetOrgPerson))("
                             +  userDN.substring(0,userDN.indexOf(",")) + "))";

    results = findDNByName(ctx, baseDN, searchFilter, 2);

    if (results == null) {
      return null;
    }

    if(results.size() > 1) {
      //make sure there is not another item available, there should be only 1 match
      LOG.info("Matched multiple users for the user: " + userDN + ",returning null");
      return null;
    }
    return results.get(0);
  }

  public static List<String> findDNByName(DirContext ctx, String baseDN,
      String searchString, int limit) throws NamingException {
    SearchResult searchResult     = null;
    List<String> retValues        = null;
    String matchedDN              = null;
    SearchControls searchControls = new SearchControls();
    String[] returnAttributes     = new String[0]; //empty set

    searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    searchControls.setReturningAttributes(returnAttributes);
    if (limit > 0) {
      searchControls.setCountLimit(limit); // limit the result set to limit the size of resultset
    }

    NamingEnumeration<SearchResult> results = ctx.search(baseDN, searchString, searchControls);
    while(results.hasMoreElements()) {
      searchResult = results.nextElement();
      matchedDN    = searchResult.getNameInNamespace();

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
   * This method relies on the attribute,configurable via HIVE_SERVER2_PLAIN_LDAP_GROUPMEMBERSHIP_KEY,
   * being set on the user entry that references the group. The returned list ONLY includes direct
   * groups the user belongs to. Parent groups of these direct groups are NOT included.
   * @param ctx DirContext for the LDAP Connection.
   * @param userDN A unique userDN that is to be located in the LDAP.
   * @return List of Group DNs the user belongs to, emptylist otherwise.
   */
  public static List<String> getGroupsForUser(DirContext ctx, String userDN)
      throws NamingException {
    List<String> groupList        = new ArrayList<String>();
    String user                   = extractName(userDN);
    String searchFilter           = "(&(objectClass=" + groupClass_attr + ")(|(" +
                                      groupMembership_attr + "=" + userDN + ")(" +
                                      groupMembership_attr + "=" + user + ")))";
    SearchControls searchControls = new SearchControls();
    NamingEnumeration<SearchResult> results = null;
    SearchResult result = null;
    String groupBase = null;

    LOG.debug("getGroupsForUser:searchFilter=" + searchFilter);
    String[] attrIDs = new String[0];
    searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    searchControls.setReturningAttributes(attrIDs);

    ListIterator<String> listIter = groupBases.listIterator();
    while (listIter.hasNext()) {
      try {
        groupBase = listIter.next().split(",", 2)[1];
        LOG.debug("Searching for groups under " + groupBase);
        results   = ctx.search(groupBase, searchFilter, searchControls);

        while(results.hasMoreElements()) {
          result = results.nextElement();
          LOG.debug("Found Group:" + result.getNameInNamespace());
          groupList.add(result.getNameInNamespace());
        }
      } catch (NamingException e) {
        LOG.warn("Exception searching for user groups", e);
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
    if (rootDN == null) {
      return null;
    }

    SearchControls searchControls = new SearchControls();
    List<String> list             = new ArrayList<String>();
    String[] returnAttributes     = new String[0]; //empty set

    searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    searchControls.setReturningAttributes(returnAttributes);

    LOG.info("Using a user specified LDAP query for adjudication:" + query + ",baseDN=" + rootDN);
    NamingEnumeration<SearchResult> results = ctx.search(rootDN, query, searchControls);
    SearchResult searchResult = null;
    while(results.hasMoreElements()) {
      searchResult = results.nextElement();
      list.add(searchResult.getNameInNamespace());
      LOG.debug("LDAPAtn:executeLDAPQuery()::Return set size " + list.get(list.size() - 1));
    }
    return list;
  }

  public static boolean isDN(String name) {
    return (name.indexOf("=") > -1);
  }

  public static String extractName(String dn) {
    int domainIdx = ServiceUtils.indexOfDomainMatch(dn);
    if (domainIdx > 0) {
      return dn.substring(0, domainIdx);
    }

    if (dn.indexOf("=") > -1) {
      return dn.substring(dn.indexOf("=") + 1, dn.indexOf(","));
    }
    return dn;
  }

  public static String extractBaseDN(String dn) {
    if (dn.indexOf(",") > -1) {
      return dn.substring(dn.indexOf(",") + 1);
    }
    return null;
  }

}
