package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.thrift.TException;

public class RevokePrivAuthUtils {

  public static List<HiveObjectPrivilege> authorizeAndGetRevokePrivileges(List<HivePrincipal> principals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject, boolean grantOption,
      IMetaStoreClient mClient, String userName)
          throws HiveAuthzPluginException, HiveAccessControlException {

    List<HiveObjectPrivilege> matchingPrivs = new ArrayList<HiveObjectPrivilege>();

    StringBuilder errMsg = new StringBuilder();
    for (HivePrincipal principal : principals) {

      // get metastore/thrift privilege object for this principal and object, not looking at
      // privileges obtained indirectly via roles
      List<HiveObjectPrivilege> msObjPrivs;
      try {
        msObjPrivs = mClient.list_privileges(principal.getName(),
            AuthorizationUtils.getThriftPrincipalType(principal.getType()),
            SQLAuthorizationUtils.getThriftHiveObjectRef(hivePrivObject));
      } catch (MetaException e) {
        throw new HiveAuthzPluginException(e);
      } catch (TException e) {
        throw new HiveAuthzPluginException(e);
      }

      // the resulting privileges need to be filtered on privilege type and
      // username

      // create a Map to capture object privileges corresponding to privilege
      // type
      Map<String, HiveObjectPrivilege> priv2privObj = new HashMap<String, HiveObjectPrivilege>();

      for (HiveObjectPrivilege msObjPriv : msObjPrivs) {
        PrivilegeGrantInfo grantInfo = msObjPriv.getGrantInfo();
        // check if the grantor matches current user
        if (grantInfo.getGrantor() != null && grantInfo.getGrantor().equals(userName)
            && grantInfo.getGrantorType() == PrincipalType.USER) {
          // add to the map
          priv2privObj.put(grantInfo.getPrivilege(), msObjPriv);
        }
        // else skip this one
      }

      // find the privileges that we are looking for
      for (HivePrivilege hivePrivilege : hivePrivileges) {
        HiveObjectPrivilege matchedPriv = priv2privObj.get(hivePrivilege.getName());
        if (matchedPriv != null) {
          matchingPrivs.add(matchedPriv);
        } else {
          errMsg.append("Cannot find privilege ").append(hivePrivilege).append(" for ")
              .append(principal).append(" on ").append(hivePrivObject).append(" granted by ")
              .append(userName).append(System.getProperty("line.separator"));
        }
      }

    }

    if (errMsg.length() != 0) {
      throw new HiveAccessControlException(errMsg.toString());
    }
    return matchingPrivs;
  }

}
