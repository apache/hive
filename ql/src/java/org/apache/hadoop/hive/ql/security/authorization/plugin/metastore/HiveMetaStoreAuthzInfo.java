/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.security.authorization.plugin.metastore;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
/*
HiveMetaStoreAuthzInfo : Context for HiveMetaStore authorization done by HiveMetaStoreAuthorizer
 */

public class HiveMetaStoreAuthzInfo {
  private final PreEventContext preEventContext;
  private final HiveOperationType operationType;
  private final List<HivePrivilegeObject> inputHObjs;
  private final List<HivePrivilegeObject> outputHObjs;
  private final String                    commandString;
  private final HiveAuthzContext hiveAuthzContext;

  public HiveMetaStoreAuthzInfo(PreEventContext preEventContext, HiveOperationType operationType, List<HivePrivilegeObject> inputHObjs, List<HivePrivilegeObject> outputHObjs, String commandString) {
    this.preEventContext  = preEventContext;
    this.operationType    = operationType;
    this.inputHObjs       = inputHObjs;
    this.outputHObjs      = outputHObjs;
    this.commandString    = commandString;
    this.hiveAuthzContext = createHiveAuthzContext();
  }

    public HiveOperationType getOperationType() {
        return operationType;
    }

    public List<HivePrivilegeObject> getInputHObjs() { return inputHObjs; }

    public List<HivePrivilegeObject> getOutputHObjs() { return outputHObjs; }

    public String getCommandString() {
        return commandString;
    }

    public HiveAuthzContext getHiveAuthzContext() { return hiveAuthzContext; }

    public PreEventContext getPreEventContext(){
      return preEventContext;
    }

    public UserGroupInformation getUGI() {
      try {
        return UserGroupInformation.getCurrentUser();
        } catch (IOException excp) {
      }
      return null;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("HiveMetaStoreAuthzInfo= ").append("{");
      String eventType = (preEventContext != null)? preEventContext.getEventType().name(): StringUtils.EMPTY;
      sb.append("eventType=").append(eventType);
      sb.append(", operationType=").append(operationType.name());
      sb.append(", commandString=" ).append(commandString);
      sb.append(", inputHObjs=").append(inputHObjs);
      sb.append(", outputHObjs=").append(outputHObjs);
      sb.append(" }");
      return sb.toString();
    }

    private HiveAuthzContext createHiveAuthzContext() {
      HiveAuthzContext.Builder builder = new HiveAuthzContext.Builder();
      builder.setCommandString(commandString);

      // TODO: refer to SessionManager/HiveSessionImpl for details on getting ipAddress and forwardedAddresses
      builder.setForwardedAddresses(new ArrayList<>());

      String ipAddress = HiveMetaStore.HMSHandler.getIPAddress();

      builder.setUserIpAddress(ipAddress);

      HiveAuthzContext ret = builder.build();

      return ret;
    }
}
