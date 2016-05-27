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

import java.util.List;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;

/**
 * Provides context information in authorization check call that can be used for
 * auditing and/or authorization.
 * It is an immutable class. Builder inner class is used instantiate it.
 */
@LimitedPrivate(value = { "Apache Argus (incubating)" })
@Evolving
public final class HiveAuthzContext {

  public static class Builder {
    private String commandString;
    private List<String> forwardedAddresses;
    private String userIpAddress;

    /**
     * Get user's ip address. This is set only if the authorization api is
     * invoked from a HiveServer2 instance in standalone mode.
     *
     * @return ip address
     */
    public String getUserIpAddress() {
      return userIpAddress;
    }

    public void setUserIpAddress(String userIpAddress) {
      this.userIpAddress = userIpAddress;
    }

    public String getCommandString() {
      return commandString;
    }
    public void setCommandString(String commandString) {
      this.commandString = commandString;
    }

    public List<String> getForwardedAddresses() {
      return forwardedAddresses;
    }
    public void setForwardedAddresses(List<String> forwardedAddresses) {
      this.forwardedAddresses = forwardedAddresses;
    }

    public HiveAuthzContext build(){
      return new HiveAuthzContext(this);
    }
  }

  private final String userIpAddress;
  private final String commandString;
  private final List<String> forwardedAddresses;

  private HiveAuthzContext(Builder builder) {
    this.userIpAddress = builder.userIpAddress;
    this.commandString = builder.commandString;
    this.forwardedAddresses = builder.forwardedAddresses;
  }

  public String getIpAddress() {
    return userIpAddress;
  }

  public String getCommandString() {
    return commandString;
  }

  public List<String> getForwardedAddresses() {
    return forwardedAddresses;
  }

  @Override
  public String toString() {
    return "QueryContext [commandString=" + commandString + ", forwardedAddresses=" + forwardedAddresses + "]";
  }

}
