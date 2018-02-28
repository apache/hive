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
package org.apache.hadoop.hive.ql.security.authorization.plugin;

import java.util.List;

/**
 * This would be implemented by a class that needs to be notified when there is
 * a policy change.
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
