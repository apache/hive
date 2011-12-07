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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;

/**
 * Default in-memory token store implementation.
 */
public class MemoryTokenStore implements TokenStoreDelegationTokenSecretManager.TokenStore {

  private final java.util.concurrent.ConcurrentHashMap<Integer, String> masterKeys
      = new java.util.concurrent.ConcurrentHashMap<Integer, String>();

  private final java.util.concurrent.ConcurrentHashMap<DelegationTokenIdentifier, DelegationTokenInformation> tokens
      = new java.util.concurrent.ConcurrentHashMap<DelegationTokenIdentifier, DelegationTokenInformation>();

  private final AtomicInteger masterKeySeq = new AtomicInteger();

  @Override
  public void setConf(Configuration conf) {
  }

  @Override
  public Configuration getConf() {
    return null;
  }

  @Override
  public int addMasterKey(String s) {
    int keySeq = masterKeySeq.getAndIncrement();
    masterKeys.putIfAbsent(keySeq, s);
    return keySeq;
  }

  @Override
  public void updateMasterKey(int keySeq, String s) {
    masterKeys.put(keySeq, s);
  }

  @Override
  public boolean removeMasterKey(int keySeq) {
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
    return (tokenInfo == null);
  }

  @Override
  public boolean removeToken(DelegationTokenIdentifier tokenIdentifier) {
    DelegationTokenInformation tokenInfo = tokens.remove(tokenIdentifier);
    return tokenInfo != null;
  }

  @Override
  public DelegationTokenInformation getToken(DelegationTokenIdentifier tokenIdentifier) {
    return tokens.get(tokenIdentifier);
  }

  @Override
  public List<DelegationTokenIdentifier> getAllDelegationTokenIdentifiers() {
    List<DelegationTokenIdentifier> result = new java.util.ArrayList<DelegationTokenIdentifier>(
        tokens.size());
    for (DelegationTokenIdentifier id : tokens.keySet()) {
        result.add(id);
    }
    return result;
  }

}
