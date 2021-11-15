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

package org.apache.hadoop.hive.ql.processors;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.google.common.base.Joiner;

class CommandUtil {
  private static final Logger LOG = LoggerFactory.getLogger(CommandUtil.class);

  /**
   * Authorize command of given type and arguments
   *
   * @param ss
   * @param type
   * @param command
   * @return null if there was no authorization error. Otherwise returns  CommandProcessorResponse
   * capturing the authorization error
   */
  static CommandProcessorResponse authorizeCommand(SessionState ss, HiveOperationType type,
      List<String> command) throws CommandProcessorException {
    if (ss == null) {
      // ss can be null in unit tests
      return null;
    }

    if (ss.isAuthorizationModeV2() &&
        HiveConf.getBoolVar(ss.getConf(), HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)) {
      String errMsg = "Error authorizing command " + command;
      try {
        authorizeCommandThrowEx(ss, type, command);
        // authorized to perform action
        return null;
      } catch (HiveAuthzPluginException | HiveAccessControlException e) {
        LOG.error(errMsg, e);
        throw new CommandProcessorException(e);
      }
    }
    return null;
  }
  /**
   * Authorize command. Throws exception if the check fails
   * @param ss
   * @param type
   * @param command
   * @throws HiveAuthzPluginException
   * @throws HiveAccessControlException
   */
  private static void authorizeCommandThrowEx(SessionState ss, HiveOperationType type,
      List<String> command) throws HiveAuthzPluginException, HiveAccessControlException {
    HivePrivilegeObject commandObj = HivePrivilegeObject.createHivePrivilegeObject(command);
    HiveAuthzContext.Builder ctxBuilder = new HiveAuthzContext.Builder();
    ctxBuilder.setCommandString(Joiner.on(' ').join(command));
    ctxBuilder.setUserIpAddress(ss.getUserIpAddress());
    ctxBuilder.setForwardedAddresses(ss.getForwardedAddresses());
    ss.getAuthorizerV2().checkPrivileges(type, Arrays.asList(commandObj), null, ctxBuilder.build());
  }

  /**
   * Authorize command of given type, arguments and for service hosts (for Service Type authorization)
   *
   * @param ss - session state
   * @param type - operation type
   * @param command - command args
   * @param serviceObject - service object
   * @return null if there was no authorization error. Otherwise returns  CommandProcessorResponse
   * capturing the authorization error
   */
  static CommandProcessorResponse authorizeCommandAndServiceObject(SessionState ss, HiveOperationType type,
      List<String> command, String serviceObject) throws CommandProcessorException {
    if (ss == null) {
      // ss can be null in unit tests
      return null;
    }

    if (ss.isAuthorizationModeV2() &&
      HiveConf.getBoolVar(ss.getConf(), HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)) {
      String errMsg = "Error authorizing command " + command;
      try {
        authorizeCommandThrowEx(ss, type, command, serviceObject);
        // authorized to perform action
        return null;
      } catch (HiveAuthzPluginException | HiveAccessControlException e) {
        LOG.error(errMsg, e);
        throw new CommandProcessorException(e);
      }
    }
    return null;
  }

  private static void authorizeCommandThrowEx(SessionState ss, HiveOperationType type,
    List<String> command, String serviceObject) throws HiveAuthzPluginException, HiveAccessControlException {
    HivePrivilegeObject commandObj = HivePrivilegeObject.createHivePrivilegeObject(command);
    HivePrivilegeObject serviceObj = new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.SERVICE_NAME,
      null, serviceObject, null, null, null);
    HiveAuthzContext.Builder ctxBuilder = new HiveAuthzContext.Builder();
    ctxBuilder.setCommandString(Joiner.on(' ').join(command));
    ctxBuilder.setUserIpAddress(ss.getUserIpAddress());
    ctxBuilder.setForwardedAddresses(ss.getForwardedAddresses());
    ss.getAuthorizerV2().checkPrivileges(type, Collections.singletonList(commandObj),
      Collections.singletonList(serviceObj), ctxBuilder.build());
  }
}
