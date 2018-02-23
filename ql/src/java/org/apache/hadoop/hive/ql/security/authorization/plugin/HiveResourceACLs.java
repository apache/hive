package org.apache.hadoop.hive.ql.security.authorization.plugin;
import java.util.Map;

public interface HiveResourceACLs {
  enum Privilege {
    SELECT, UPDATE, CREATE, DROP, ALTER, INDEX, LOCK, READ, WRITE
  };

  enum AccessResult {
    ALLOWED, NOT_ALLOWED, CONDITIONAL_ALLOWED
  };

  
  /**
   * @return Returns mapping of user name to privilege-access result pairs
   */
  public Map<String, Map<Privilege, AccessResult>> getUserPermissions();

  /**
   * @return Returns mapping of group name to privilege-access result pairs
   */
  public Map<String, Map<Privilege, AccessResult>> getGroupPermissions();

}
