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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge.Server.ServerMode;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.apache.hadoop.security.token.delegation.MetastoreDelegationTokenSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBTokenStore implements DelegationTokenStore {
  private static final Logger LOG = LoggerFactory.getLogger(DBTokenStore.class);
  private Configuration conf;

  @Override
  public int addMasterKey(String s) throws TokenStoreException {
    LOG.trace("addMasterKey: s = {}", s);
    return (Integer)invokeOnTokenStore("addMasterKey", new Object[]{s},String.class);
  }

  @Override
  public void updateMasterKey(int keySeq, String s) throws TokenStoreException {
    LOG.trace("updateMasterKey: s = {}, keySeq = {}", s, keySeq);
    invokeOnTokenStore("updateMasterKey", new Object[] {Integer.valueOf(keySeq), s},
        Integer.class, String.class);
  }

  @Override
  public boolean removeMasterKey(int keySeq) {
    return (Boolean)invokeOnTokenStore("removeMasterKey", new Object[] {Integer.valueOf(keySeq)},
      Integer.class);
  }

  @Override
  public String[] getMasterKeys() throws TokenStoreException {
    return (String[])invokeOnTokenStore("getMasterKeys", new Object[0]);
  }

  @Override
  public boolean addToken(DelegationTokenIdentifier tokenIdentifier,
      DelegationTokenInformation token) throws TokenStoreException {

    try {
      String identifier = TokenStoreDelegationTokenSecretManager.encodeWritable(tokenIdentifier);
      String tokenStr = Base64.encodeBase64URLSafeString(
        MetastoreDelegationTokenSupport.encodeDelegationTokenInformation(token));
      boolean result = (Boolean)invokeOnTokenStore("addToken", new Object[] {identifier, tokenStr},
        String.class, String.class);
      LOG.trace("addToken: tokenIdentifier = {}, added = {}", tokenIdentifier, result);
      return result;
    } catch (IOException e) {
      throw new TokenStoreException(e);
    }
  }

  @Override
  public DelegationTokenInformation getToken(DelegationTokenIdentifier tokenIdentifier)
      throws TokenStoreException {
    try {
      String tokenStr = (String)invokeOnTokenStore("getToken", new Object[] {
          TokenStoreDelegationTokenSecretManager.encodeWritable(tokenIdentifier)}, String.class);
      DelegationTokenInformation result = null;
      if (StringUtils.isNotEmpty(tokenStr)) {
        result = MetastoreDelegationTokenSupport.decodeDelegationTokenInformation(Base64.decodeBase64(tokenStr));
      }
      LOG.trace("getToken: tokenIdentifier = {}, result = {}", tokenIdentifier, result);
      return result;
    } catch (IOException e) {
      throw new TokenStoreException(e);
    }
  }

  @Override
  public boolean removeToken(DelegationTokenIdentifier tokenIdentifier) throws TokenStoreException{
    try {
      boolean result = (Boolean)invokeOnTokenStore("removeToken", new Object[] {
        TokenStoreDelegationTokenSecretManager.encodeWritable(tokenIdentifier)}, String.class);
      LOG.trace("removeToken: tokenIdentifier = {}, removed = {}", tokenIdentifier, result);
      return result;
    } catch (IOException e) {
      throw new TokenStoreException(e);
    }
  }

  @Override
  public List<DelegationTokenIdentifier> getAllDelegationTokenIdentifiers() throws TokenStoreException{

    List<String> tokenIdents = (List<String>)invokeOnTokenStore("getAllTokenIdentifiers", new Object[0]);
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

  private Object handler;
  private ServerMode serverMode;

  @Override
  public void init(Object handler, HadoopThriftAuthBridge.Server.ServerMode serverMode) throws TokenStoreException {
    this.handler = handler;
    this.serverMode = serverMode;
  }

  private Object invokeOnTokenStore(String methName, Object[] params, Class<?> ... paramTypes)
      throws TokenStoreException{
    Object tokenStore;
    try {
      switch (serverMode) {
      case METASTORE:
        tokenStore = handler.getClass().getMethod("getMS").invoke(handler);
        break;
      case HIVESERVER2:
        Object hiveObject = ((Class<?>) handler).getMethod("get").invoke(handler, null);
        tokenStore = ((Class<?>) handler).getMethod("getMSC").invoke(hiveObject);
        break;
      default:
        throw new IllegalArgumentException("Unexpected value of Server mode " + serverMode);
      }
      return tokenStore.getClass().getMethod(methName, paramTypes).invoke(tokenStore, params);
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
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void close() throws IOException {
    // No-op.
  }

}
