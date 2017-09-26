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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.codec.binary.Base64;

import com.google.common.io.ByteStreams;

import org.apache.tez.common.security.JobTokenIdentifier;

import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.types.AddressTypes;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;

public class TezAmInstance extends ServiceInstanceBase {
  private static final Logger LOG = LoggerFactory.getLogger(TezAmInstance.class);
  private final int pluginPort;
  private Token<JobTokenIdentifier> token;

  public TezAmInstance(ServiceRecord srv) throws IOException {
    super(srv, TezAmRegistryImpl.IPC_TEZCLIENT);
    final Endpoint plugin = srv.getInternalEndpoint(TezAmRegistryImpl.IPC_PLUGIN);
    if (plugin != null) {
      this.pluginPort = Integer.parseInt(RegistryTypeUtils.getAddressField(
        plugin.addresses.get(0), AddressTypes.ADDRESS_PORT_FIELD));
    } else {
      this.pluginPort = -1;
    }
  }

  public int getPluginPort() {
    return pluginPort;
  }

  public String getSessionId() {
    return getProperties().get(TezAmRegistryImpl.AM_SESSION_ID);
  }

  public String getPluginTokenJobId() {
    return getProperties().get(TezAmRegistryImpl.AM_PLUGIN_JOBID);
  }

  public Token<JobTokenIdentifier> getPluginToken() {
    if (this.token != null) return token;
    String tokenString = getProperties().get(TezAmRegistryImpl.AM_PLUGIN_TOKEN);
    if (tokenString == null || tokenString.isEmpty()) return null;
    byte[] tokenBytes = Base64.decodeBase64(tokenString);
    Token<JobTokenIdentifier> token = new Token<>();
    try {
      token.readFields(ByteStreams.newDataInput(tokenBytes));
    } catch (IOException e) {
      LOG.error("Couldn't read the plugin token from [" + tokenString + "]", e);
      return null;
    }
    this.token = token;
    return token;
  }

  @Override
  public String toString() {
    return "TezAmInstance [" + getSessionId() + ", host=" + host + ", rpcPort=" + rpcPort +
        ", pluginPort=" + pluginPort + ", token=" + token + "]";
  }

}