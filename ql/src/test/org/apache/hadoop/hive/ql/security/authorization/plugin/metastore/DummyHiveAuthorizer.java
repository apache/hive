package org.apache.hadoop.hive.ql.security.authorization.plugin.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.fallback.FallbackHiveAuthorizer;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Arrays;
import java.util.List;

/**
 * Test HiveAuthorizer for invoking checkPrivilege Methods for authorization call
 * Authorizes user sam and rob.
 */
public class DummyHiveAuthorizer extends FallbackHiveAuthorizer {

  static final List<String> allowedUsers = Arrays.asList("sam","rob");

  DummyHiveAuthorizer(HiveConf hiveConf, HiveAuthenticationProvider hiveAuthenticator,
                      HiveAuthzSessionContext ctx) {
    super(hiveConf,hiveAuthenticator, ctx);
  }

  @Override
  public void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputHObjs,
                              List<HivePrivilegeObject> outputHObjs, HiveAuthzContext context) throws
          HiveAuthzPluginException, HiveAccessControlException {

    String user         = null;
    String errorMessage = "";
    try {
      user = UserGroupInformation.getLoginUser().getShortUserName();
    } catch (Exception e) {
      throw  new HiveAuthzPluginException("Unable to get UserGroupInformation");
    }

    if (!isOperationAllowed(user)) {
      errorMessage = "Operation type " + hiveOpType + " not allowed for user:" + user;
      throw  new HiveAuthzPluginException(errorMessage);
    }
  }

  private boolean isOperationAllowed(String user) {
      return allowedUsers.contains(user);
  }

}
