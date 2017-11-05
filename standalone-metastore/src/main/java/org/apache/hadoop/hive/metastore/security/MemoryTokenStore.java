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

package org.apache.hadoop.hive.metastore.security;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default in-memory token store implementation.
 */
public class MemoryTokenStore implements DelegationTokenStore {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryTokenStore.class);

  private final Map<Integer, String> masterKeys = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<DelegationTokenIdentifier, DelegationTokenInformation> tokens
      = new ConcurrentHashMap<>();

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
    LOG.trace("addMasterKey: s = {}, keySeq = {}", s, keySeq);
    masterKeys.put(keySeq, s);
    return keySeq;
  }

  @Override
  public void updateMasterKey(int keySeq, String s) {
    LOG.trace("updateMasterKey: s = {}, keySeq = {}", s, keySeq);
    masterKeys.put(keySeq, s);
  }

  @Override
  public boolean removeMasterKey(int keySeq) {
    LOG.trace("removeMasterKey: keySeq = {}", keySeq);
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
    LOG.trace("addToken: tokenIdentifier = {}, added = {}", tokenIdentifier, (tokenInfo == null));
    return (tokenInfo == null);
  }

  @Override
  public boolean removeToken(DelegationTokenIdentifier tokenIdentifier) {
    DelegationTokenInformation tokenInfo = tokens.remove(tokenIdentifier);
    LOG.trace("removeToken: tokenIdentifier = {}, removed = ", tokenIdentifier, (tokenInfo != null));
    return tokenInfo != null;
  }

  @Override
  public DelegationTokenInformation getToken(DelegationTokenIdentifier tokenIdentifier) {
    DelegationTokenInformation result = tokens.get(tokenIdentifier);
    LOG.trace("getToken: tokenIdentifier = {}, result = {}", tokenIdentifier, result);
    return result;
  }

  @Override
  public List<DelegationTokenIdentifier> getAllDelegationTokenIdentifiers() {
    return new ArrayList<>(tokens.keySet());
  }

  @Override
  public void close() throws IOException {
    //no-op
  }

  @Override
  public void init(Object hmsHandler, HadoopThriftAuthBridge.Server.ServerMode smode) throws TokenStoreException {
    // no-op
  }
}
