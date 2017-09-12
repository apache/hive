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
package org.apache.hadoop.hive.registry.impl;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TezAmRegistryImpl extends ZkRegistryBase<TezAmInstance> {
  private static final Logger LOG = LoggerFactory.getLogger(TezAmRegistryImpl.class);

  static final String IPC_TEZCLIENT = "tez-client";
  static final String IPC_PLUGIN = "llap-plugin";
  static final String AM_SESSION_ID = "am.session.id", AM_PLUGIN_TOKEN = "am.plugin.token",
      AM_PLUGIN_JOBID = "am.plugin.jobid";
  private final static String NAMESPACE_PREFIX = "tez-am-";
  private final static String USER_SCOPE_PATH_PREFIX = "user-";
  private static final String WORKER_PREFIX = "worker-";
  private static final String SASL_LOGIN_CONTEXT_NAME = "TezAmZooKeeperClient";

  private final String registryName;

  public static TezAmRegistryImpl create(Configuration conf, boolean b) {
    String amRegistryName = HiveConf.getVar(conf, ConfVars.LLAP_TASK_SCHEDULER_AM_REGISTRY_NAME);
    return StringUtils.isBlank(amRegistryName) ? null
        : new TezAmRegistryImpl(amRegistryName, conf, true);
  }


  private TezAmRegistryImpl(String instanceName, Configuration conf, boolean useSecureZk) {
    super(instanceName, conf, null, NAMESPACE_PREFIX, USER_SCOPE_PATH_PREFIX, WORKER_PREFIX,
        useSecureZk ? SASL_LOGIN_CONTEXT_NAME : null,
        HiveConf.getVar(conf, ConfVars.LLAP_TASK_SCHEDULER_AM_REGISTRY_PRINCIPAL),
        HiveConf.getVar(conf, ConfVars.LLAP_TASK_SCHEDULER_AM_REGISTRY_KEYTAB_FILE),
        null); // Always validate ACLs
    this.registryName = instanceName;
    LOG.info("AM Zookeeper Registry is enabled with registryid: " + instanceName);
  }

  public void initializeWithoutRegistering() throws IOException {
    initializeWithoutRegisteringInternal();
  }

  public void populateCache(boolean doInvokeListeners) throws IOException {
    PathChildrenCache pcc = ensureInstancesCache(0);
    populateCache(pcc, doInvokeListeners);
  }

  public String register(int amPort, int pluginPort, String sessionId,
      String serializedToken, String jobIdForToken) throws IOException {
    ServiceRecord srv = new ServiceRecord();
    Endpoint rpcEndpoint = RegistryTypeUtils.ipcEndpoint(
        IPC_TEZCLIENT, new InetSocketAddress(hostname, amPort));
    srv.addInternalEndpoint(rpcEndpoint);
    Endpoint pluginEndpoint = null;
    if (pluginPort >= 0) {
      pluginEndpoint = RegistryTypeUtils.ipcEndpoint(
          IPC_PLUGIN, new InetSocketAddress(hostname, pluginPort));
      srv.addInternalEndpoint(pluginEndpoint);
    }
    srv.set(AM_SESSION_ID, sessionId);
    boolean hasToken = serializedToken != null;
    srv.set(AM_PLUGIN_TOKEN, hasToken ? serializedToken : "");
    srv.set(AM_PLUGIN_JOBID, jobIdForToken != null ? jobIdForToken : "");
    String uniqueId = registerServiceRecord(srv);
    LOG.info("Registered this AM: rpc: {}, plugin: {}, sessionId: {}, token: {}, znodePath: {}",
        rpcEndpoint, pluginEndpoint, sessionId, hasToken, getRegistrationZnodePath());
    return uniqueId;
  }

  public TezAmInstance getInstance(String name) {
    Collection<TezAmInstance> instances = getAllInternal();
    for(TezAmInstance instance : instances) {
      if (instance.getWorkerIdentity().equals(name)) {
        return instance;
      }
    }
    return null;
  }

  @Override
  protected TezAmInstance createServiceInstance(ServiceRecord srv) throws IOException {
    return new TezAmInstance(srv);
  }

  @Override
  protected String getZkPathUser(Configuration conf) {
    // We assume that AMs and HS2 run under the same user.
    return RegistryUtils.currentUser();
  }

  public String getRegistryName() {
    return registryName;
  }
}
