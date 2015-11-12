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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge.Server.ServerMode;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.apache.hadoop.security.token.delegation.HiveDelegationTokenSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBTokenStore implements DelegationTokenStore {
  private static final Logger LOG = LoggerFactory.getLogger(DBTokenStore.class);

  @Override
  public int addMasterKey(String s) throws TokenStoreException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("addMasterKey: s = " + s);
    }
    return (Integer)invokeOnRawStore("addMasterKey", new Object[]{s},String.class);
  }

  @Override
  public void updateMasterKey(int keySeq, String s) throws TokenStoreException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("updateMasterKey: s = " + s + ", keySeq = " + keySeq);
    }
    invokeOnRawStore("updateMasterKey", new Object[] {Integer.valueOf(keySeq), s},
        Integer.class, String.class);
  }

  @Override
  public boolean removeMasterKey(int keySeq) {
    return (Boolean)invokeOnRawStore("removeMasterKey", new Object[] {Integer.valueOf(keySeq)},
      Integer.class);
  }

  @Override
  public String[] getMasterKeys() throws TokenStoreException {
    return (String[])invokeOnRawStore("getMasterKeys", new Object[0]);
  }

  @Override
  public boolean addToken(DelegationTokenIdentifier tokenIdentifier,
      DelegationTokenInformation token) throws TokenStoreException {

    try {
      String identifier = TokenStoreDelegationTokenSecretManager.encodeWritable(tokenIdentifier);
      String tokenStr = Base64.encodeBase64URLSafeString(
        HiveDelegationTokenSupport.encodeDelegationTokenInformation(token));
      boolean result = (Boolean)invokeOnRawStore("addToken", new Object[] {identifier, tokenStr},
        String.class, String.class);
      if (LOG.isTraceEnabled()) {
        LOG.trace("addToken: tokenIdentifier = " + tokenIdentifier + ", added = " + result);
      }
      return result;
    } catch (IOException e) {
      throw new TokenStoreException(e);
    }
  }

  @Override
  public DelegationTokenInformation getToken(DelegationTokenIdentifier tokenIdentifier)
      throws TokenStoreException {
    try {
      String tokenStr = (String)invokeOnRawStore("getToken", new Object[] {
          TokenStoreDelegationTokenSecretManager.encodeWritable(tokenIdentifier)}, String.class);
      DelegationTokenInformation result = null;
      if (tokenStr != null) {
        result = HiveDelegationTokenSupport.decodeDelegationTokenInformation(Base64.decodeBase64(tokenStr));
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("getToken: tokenIdentifier = " + tokenIdentifier + ", result = " + result);
      }
      return result;
    } catch (IOException e) {
      throw new TokenStoreException(e);
    }
  }

  @Override
  public boolean removeToken(DelegationTokenIdentifier tokenIdentifier) throws TokenStoreException{
    try {
      boolean result = (Boolean)invokeOnRawStore("removeToken", new Object[] {
        TokenStoreDelegationTokenSecretManager.encodeWritable(tokenIdentifier)}, String.class);
      if (LOG.isTraceEnabled()) {
        LOG.trace("removeToken: tokenIdentifier = " + tokenIdentifier + ", removed = " + result);
      }
      return result;
    } catch (IOException e) {
      throw new TokenStoreException(e);
    }
  }

  @Override
  public List<DelegationTokenIdentifier> getAllDelegationTokenIdentifiers() throws TokenStoreException{

    List<String> tokenIdents = (List<String>)invokeOnRawStore("getAllTokenIdentifiers", new Object[0]);
    List<DelegationTokenIdentifier> delTokenIdents = new ArrayList<DelegationTokenIdentifier>(tokenIdents.size());

    for (String tokenIdent : tokenIdents) {
      DelegationTokenIdentifier delToken = new DelegationTokenIdentifier();
      try {
        TokenStoreDelegationTokenSecretManager.decodeWritable(delToken, tokenIdent);
      } catch (IOException e) {
        throw new TokenStoreException(e);
      }
      delTokenIdents.add(delToken);
    }
    return delTokenIdents;
  }

  private Object hmsHandler;

  @Override
  public void init(Object hms, ServerMode smode) throws TokenStoreException {
    this.hmsHandler = hms;
  }

  private Object invokeOnRawStore(String methName, Object[] params, Class<?> ... paramTypes)
      throws TokenStoreException{

    try {
      Object rawStore = hmsHandler.getClass().getMethod("getMS").invoke(hmsHandler);
      return rawStore.getClass().getMethod(methName, paramTypes).invoke(rawStore, params);
    } catch (IllegalArgumentException e) {
        throw new TokenStoreException(e);
    } catch (SecurityException e) {
        throw new TokenStoreException(e);
    } catch (IllegalAccessException e) {
        throw new TokenStoreException(e);
    } catch (InvocationTargetException e) {
        throw new TokenStoreException(e.getCause());
    } catch (NoSuchMethodException e) {
        throw new TokenStoreException(e);
    }
  }

  @Override
  public void setConf(Configuration conf) {
    // No-op
  }

  @Override
  public Configuration getConf() {
    return null;
  }

  @Override
  public void close() throws IOException {
    // No-op.
  }

}
