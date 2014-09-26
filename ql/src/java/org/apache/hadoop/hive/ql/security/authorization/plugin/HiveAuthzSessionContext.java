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

/**
 * Provides session context information.
 * It is an immutable class. Builder inner class is used instantiate it.
 */
@LimitedPrivate(value = { "Apache Argus (incubating)" })
@Evolving
public final class HiveAuthzSessionContext {

  public enum CLIENT_TYPE {
    HIVESERVER2, HIVECLI
  };

  public static class Builder {
    private String sessionString;
    private CLIENT_TYPE clientType;

    public Builder(){};

    /**
     * Builder that copies values from given instance of HiveAuthzSessionContext
     * @param other
     */
    public Builder(HiveAuthzSessionContext other){
      this.sessionString = other.getSessionString();
      this.clientType = other.getClientType();
    }

    public String getSessionString() {
      return sessionString;
    }
    public void setSessionString(String sessionString) {
      this.sessionString = sessionString;
    }
    public CLIENT_TYPE getClientType() {
      return clientType;
    }
    public void setClientType(CLIENT_TYPE clientType) {
      this.clientType = clientType;
    }
    public HiveAuthzSessionContext build(){
      return new HiveAuthzSessionContext(this);
    }
  }

  private final String sessionString;
  private final CLIENT_TYPE clientType;

  private HiveAuthzSessionContext(Builder builder) {
    this.sessionString = builder.sessionString;
    this.clientType = builder.clientType;
  }

  public String getSessionString() {
    return sessionString;
  }

  public CLIENT_TYPE getClientType() {
    return clientType;
  }

  @Override
  public String toString() {
    return "HiveAuthzSessionContext [sessionString=" + sessionString + ", clientType=" + clientType
        + "]";
  }

}
