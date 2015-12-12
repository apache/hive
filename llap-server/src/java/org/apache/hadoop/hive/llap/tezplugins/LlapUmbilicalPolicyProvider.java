/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.llap.tezplugins;

import java.util.Arrays;

import org.apache.hadoop.hive.llap.protocol.LlapTaskUmbilicalProtocol;
import org.apache.hadoop.security.authorize.Service;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.app.security.authorize.TezAMPolicyProvider;

public class LlapUmbilicalPolicyProvider extends TezAMPolicyProvider {

  private static Service[] services;
  private static final Object servicesLock = new Object();

  @Override
  public Service[] getServices() {
    if (services != null) return services;
    synchronized (servicesLock) {
      if (services != null) return services;
      Service[] parentSvc = super.getServices();
      Service[] result = Arrays.copyOf(parentSvc, parentSvc.length + 1);
      result[parentSvc.length] =  new Service(
          TezConstants.TEZ_AM_SECURITY_SERVICE_AUTHORIZATION_TASK_UMBILICAL,
          LlapTaskUmbilicalProtocol.class);
      return (services = result);
    }
  }
}
