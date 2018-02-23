package org.apache.hadoop.hive.ql.security.authorization.plugin;

import java.util.List;

/**
 * This would be implemented by a class that needs to be notified when there is
 * a policy change
 */
public interface HivePolicyChangeListener {
  /**
   * @param hpo
   *          List of Objects whose privileges have changed. If undetermined,
   *          null can be returned (implies that it should be treated as if all object
   *          policies might have changed).
   */
  void notifyPolicyChange(List<HivePrivilegeObject> hpo);

}
