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
/*
 * This package provides interfaces and classes that can be used to implement custom authorization for hive.
 *
 * How hive code uses this interface:
 * The interface that hive code invokes is HiveAuthorizer class.
 * The classes HivePrincipal, HivePrivilege, HivePrivilegeObject, HivePrivilegeInfo, HiveOperationType
 * are arguments used in the authorization interface.
 * The methods in the interface throws two types of exceptions - HiveAuthzPluginException (in
 * case of internal errors), and HiveAuthzPluginDeniedException (when action is not permitted
 * because authorization has failed).
 *
 * Hive uses the HiveAuthorizerFactory interface, whose implementing class is configurable through
 * hive configuration, to instantiate an instance of this interface.
 *
 *
 * Guide on implementing the interface:
 * There are two categories of operations to be done by the authorization interface, one is the
 * actions performed by the access control statements, which updates the privileges that have
 * been granted (and stores in some where like metastore database), and also retrieves the current
 * state of privileges. You may choose not to implement this part and juse a no-op implementation
 * if you are going to manage the authorization externally (eg, if you base it on mapping to
 *  file system permissions).
 * The 2nd category of operation is authorizing the hive actions by checking against the privileges
 * the user has on the objects.
 * HiveAccessController has the interface for the first type of operations and
 *  HiveAuthorizationValidator has interface for second type of operations.
 *
 * HiveAuthorizerImpl is a convenience class that you can use by just passing the implementations
 * of these two interfaces (HiveAuthorizerImpl, HiveAuthorizationValidator) in the constructor.
 *
 */
package org.apache.hadoop.hive.ql.security.authorization.plugin;
