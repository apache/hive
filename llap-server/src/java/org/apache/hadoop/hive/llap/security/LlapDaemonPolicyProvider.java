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
package org.apache.hadoop.hive.llap.security;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.protocol.LlapProtocolBlockingPB;
import org.apache.hadoop.hive.llap.protocol.LlapManagementProtocolPB;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;

public class LlapDaemonPolicyProvider extends PolicyProvider {
  private static final Service[] services = new Service[] {
    new Service(HiveConf.ConfVars.LLAP_SECURITY_ACL.varname,
        LlapProtocolBlockingPB.class),
    new Service(HiveConf.ConfVars.LLAP_MANAGEMENT_ACL.varname,
        LlapManagementProtocolPB.class)
  };

  @Override
  public Service[] getServices() {
    return services;
  }
}
