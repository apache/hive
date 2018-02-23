package org.apache.hadoop.hive.ql.security.authorization.plugin;

/**
 * Interface that can be used to retrieve authorization policy information from
 * authorization plugins
 */
public interface HivePolicyProvider {
  /**
   * @param hiveObject
   * @return representation of user/group to permissions mapping.
   */
  public HiveResourceACLs getResourceACLs(HivePrivilegeObject hiveObject);

  /**
   * @param listener
   */
  public void registerHivePolicyChangeListener(HivePolicyChangeListener listener);

}
