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
package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeObjectDesc;

/**
 * This interface has functions that provide the ability to customize the translation
 * from Hive internal representations of Authorization objects to the public API objects
 * This is an interface that is not meant for general use, it is targeted to some
 * specific use cases of Apache Sentry (incubating).
 * The API uses several classes that are considered internal to Hive, and it is
 * subject to change across releases.
 */
@LimitedPrivate(value = { "Apache Sentry (incubating)" })
@Evolving
public interface HiveAuthorizationTranslator {

  public HivePrincipal getHivePrincipal(PrincipalDesc principal)
      throws HiveException;

  public HivePrivilege getHivePrivilege(PrivilegeDesc privilege);

  public HivePrivilegeObject getHivePrivilegeObject(PrivilegeObjectDesc privObject)
      throws HiveException;
}
