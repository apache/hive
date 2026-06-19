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

package org.apache.hadoop.hive.metastore.metastore.impl;

import javax.jdo.Query;
import javax.jdo.identity.IntIdentity;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.QueryWrapper;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.metastore.RawStoreAware;
import org.apache.hadoop.hive.metastore.metastore.iface.TokenStore;
import org.apache.hadoop.hive.metastore.model.MDelegationToken;
import org.apache.hadoop.hive.metastore.model.MMasterKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenStoreImpl extends RawStoreAware implements TokenStore {
  private static final Logger LOG = LoggerFactory.getLogger(TokenStoreImpl.class);

  private MDelegationToken getTokenFrom(String tokenId) {
    try (QueryWrapper query =
             new QueryWrapper(pm.newQuery(MDelegationToken.class, "tokenIdentifier == tokenId"))) {
      query.declareParameters("java.lang.String tokenId");
      query.setUnique(true);
      MDelegationToken delegationToken = (MDelegationToken) query.execute(tokenId);
      return delegationToken;
    }
  }

  @Override
  public boolean addToken(String tokenId, String delegationToken) {
    LOG.debug("Begin executing addToken");
    MDelegationToken token = getTokenFrom(tokenId);
    if (token == null) {
      // add Token, only if it already doesn't exist
      pm.makePersistent(new MDelegationToken(tokenId, delegationToken));
    }
    LOG.debug("Done executing addToken with status");
    return token == null;
  }

  @Override
  public boolean removeToken(String tokenId) {
    LOG.debug("Begin executing removeToken");
    MDelegationToken token = getTokenFrom(tokenId);
    if (null != token) {
      pm.deletePersistent(token);
    }
    return token != null;
  }

  @Override
  public String getToken(String tokenId) {
    LOG.debug("Begin executing getToken");
    MDelegationToken token = getTokenFrom(tokenId);
    if (null != token) {
      pm.retrieve(token);
    }
    return (null == token) ? null : token.getTokenStr();
  }

  @Override
  public List<String> getAllTokenIdentifiers() {
    LOG.debug("Begin executing getAllTokenIdentifiers");
    List<String> tokenIdents = new ArrayList<>();
    Query query = pm.newQuery(MDelegationToken.class);
    List<MDelegationToken> tokens = (List<MDelegationToken>) query.execute();
    pm.retrieveAll(tokens);

    for (MDelegationToken token : tokens) {
      tokenIdents.add(token.getTokenIdentifier());
    }
    return tokenIdents;
  }

  @Override
  public int addMasterKey(String key) throws MetaException {
    LOG.debug("Begin executing addMasterKey");
    MMasterKey masterKey = new MMasterKey(key);
    pm.makePersistent(masterKey);
    return ((IntIdentity)pm.getObjectId(masterKey)).getKey();
  }

  @Override
  public void updateMasterKey(Integer id, String key) throws NoSuchObjectException, MetaException {
    LOG.debug("Begin executing updateMasterKey");
    MMasterKey masterKey;
    Query query = pm.newQuery(MMasterKey.class, "keyId == id");
    query.declareParameters("java.lang.Integer id");
    query.setUnique(true);
    masterKey = (MMasterKey) query.execute(id);
    if (null != masterKey) {
      masterKey.setMasterKey(key);
    }
    if (null == masterKey) {
      throw new NoSuchObjectException("No key found with keyId: " + id);
    }
  }

  @Override
  public boolean removeMasterKey(Integer id) {
    LOG.debug("Begin executing removeMasterKey");
    MMasterKey masterKey;
    Query query = pm.newQuery(MMasterKey.class, "keyId == id");
    query.declareParameters("java.lang.Integer id");
    query.setUnique(true);
    masterKey = (MMasterKey) query.execute(id);
    if (null != masterKey) {
      pm.deletePersistent(masterKey);
    }
    return (null != masterKey);
  }

  @Override
  public String[] getMasterKeys() {
    LOG.debug("Begin executing getMasterKeys");
    List<MMasterKey> keys;
    Query query = pm.newQuery(MMasterKey.class);
    keys = (List<MMasterKey>) query.execute();
    pm.retrieveAll(keys);

    String[] masterKeys = new String[keys.size()];
    for (int i = 0; i < keys.size(); i++) {
      masterKeys[i] = keys.get(i).getMasterKey();
    }
    return masterKeys;
  }
}
