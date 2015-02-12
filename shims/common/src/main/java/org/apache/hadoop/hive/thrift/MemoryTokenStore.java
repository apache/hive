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

package org.apache.hadoop.hive.thrift;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge.Server.ServerMode;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default in-memory token store implementation.
 */
public class MemoryTokenStore implements DelegationTokenStore {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryTokenStore.class);

  private final Map<Integer, String> masterKeys
      = new ConcurrentHashMap<Integer, String>();

  private final ConcurrentHashMap<DelegationTokenIdentifier, DelegationTokenInformation> tokens
      = new ConcurrentHashMap<DelegationTokenIdentifier, DelegationTokenInformation>();

  private final AtomicInteger masterKeySeq = new AtomicInteger();
  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public int addMasterKey(String s) {
    int keySeq = masterKeySeq.getAndIncrement();
    if (LOG.isTraceEnabled()) {
      LOG.trace("addMasterKey: s = " + s + ", keySeq = " + keySeq);
    }
    masterKeys.put(keySeq, s);
    return keySeq;
  }

  @Override
  public void updateMasterKey(int keySeq, String s) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("updateMasterKey: s = " + s + ", keySeq = " + keySeq);
    }
    masterKeys.put(keySeq, s);
  }

  @Override
  public boolean removeMasterKey(int keySeq) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("removeMasterKey: keySeq = " + keySeq);
    }
    return masterKeys.remove(keySeq) != null;
  }

  @Override
  public String[] getMasterKeys() {
    return masterKeys.values().toArray(new String[0]);
  }

  @Override
  public boolean addToken(DelegationTokenIdentifier tokenIdentifier,
    DelegationTokenInformation token) {
    DelegationTokenInformation tokenInfo = tokens.putIfAbsent(tokenIdentifier, token);
    if (LOG.isTraceEnabled()) {
      LOG.trace("addToken: tokenIdentifier = " + tokenIdentifier + ", added = " + (tokenInfo == null));
    }
    return (tokenInfo == null);
  }

  @Override
  public boolean removeToken(DelegationTokenIdentifier tokenIdentifier) {
    DelegationTokenInformation tokenInfo = tokens.remove(tokenIdentifier);
    if (LOG.isTraceEnabled()) {
      LOG.trace("removeToken: tokenIdentifier = " + tokenIdentifier + ", removed = " + (tokenInfo != null));
    }
    return tokenInfo != null;
  }

  @Override
  public DelegationTokenInformation getToken(DelegationTokenIdentifier tokenIdentifier) {
    DelegationTokenInformation result = tokens.get(tokenIdentifier);
    if (LOG.isTraceEnabled()) {
      LOG.trace("getToken: tokenIdentifier = " + tokenIdentifier + ", result = " + result);
    }
    return result;
  }

  @Override
  public List<DelegationTokenIdentifier> getAllDelegationTokenIdentifiers() {
    List<DelegationTokenIdentifier> result = new ArrayList<DelegationTokenIdentifier>(
        tokens.size());
    for (DelegationTokenIdentifier id : tokens.keySet()) {
        result.add(id);
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    //no-op
  }

  @Override
  public void init(Object hmsHandler, ServerMode smode) throws TokenStoreException {
    // no-op
  }
}
