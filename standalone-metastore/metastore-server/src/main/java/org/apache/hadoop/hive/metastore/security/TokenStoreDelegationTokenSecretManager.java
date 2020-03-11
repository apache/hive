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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.security.token.delegation.MetastoreDelegationTokenSupport;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension of {@link DelegationTokenSecretManager} to support alternative to default in-memory
 * token management for fail-over and clustering through plug-able token store (ZooKeeper etc.).
 * Delegation tokens will be retrieved from the store on-demand and (unlike base class behavior) not
 * cached in memory. This avoids complexities related to token expiration. The security token is
 * needed only at the time the transport is opened (as opposed to per interface operation). The
 * assumption therefore is low cost of interprocess token retrieval (for random read efficient store
 * such as ZooKeeper) compared to overhead of synchronizing per-process in-memory token caches.
 * The wrapper incorporates the token store abstraction within the limitations of current
 * Hive/Hadoop dependency (.20S) with minimum code duplication.
 * Eventually this should be supported by Hadoop security directly.
 */
public class TokenStoreDelegationTokenSecretManager extends DelegationTokenSecretManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TokenStoreDelegationTokenSecretManager.class.getName());

  final private long keyUpdateInterval;
  final private long tokenRemoverScanInterval;
  private Thread tokenRemoverThread;

  final private DelegationTokenStore tokenStore;

  public TokenStoreDelegationTokenSecretManager(long delegationKeyUpdateInterval,
      long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
      long delegationTokenRemoverScanInterval,
      DelegationTokenStore sharedStore) {
    super(delegationKeyUpdateInterval, delegationTokenMaxLifetime, delegationTokenRenewInterval,
        delegationTokenRemoverScanInterval);
    this.keyUpdateInterval = delegationKeyUpdateInterval;
    this.tokenRemoverScanInterval = delegationTokenRemoverScanInterval;

    this.tokenStore = sharedStore;
  }

  protected Map<Integer, DelegationKey> reloadKeys() {
    // read keys from token store
    String[] allKeys = tokenStore.getMasterKeys();
    Map<Integer, DelegationKey> keys = new HashMap<>(allKeys.length);
    for (String keyStr : allKeys) {
      DelegationKey key = new DelegationKey();
      try {
        decodeWritable(key, keyStr);
        keys.put(key.getKeyId(), key);
      } catch (IOException ex) {
        LOGGER.error("Failed to load master key.", ex);
      }
    }
    synchronized (this) {
        super.allKeys.clear();
        super.allKeys.putAll(keys);
    }
    return keys;
  }

  @Override
  public byte[] retrievePassword(DelegationTokenIdentifier identifier) throws InvalidToken {
      DelegationTokenInformation info = this.tokenStore.getToken(identifier);
      if (info == null) {
          throw new InvalidToken("token expired or does not exist: " + identifier);
      }
      // must reuse super as info.getPassword is not accessible
      synchronized (this) {
        try {
          super.currentTokens.put(identifier, info);
          return super.retrievePassword(identifier);
        } finally {
          super.currentTokens.remove(identifier);
        }
      }
  }

  @Override
  public DelegationTokenIdentifier cancelToken(Token<DelegationTokenIdentifier> token,
      String canceller) throws IOException {
    DelegationTokenIdentifier id = getTokenIdentifier(token);
    LOGGER.info("Token cancellation requested for identifier: "+id);
    this.tokenStore.removeToken(id);
    return id;
  }

  /**
   * Create the password and add it to shared store.
   */
  @Override
  protected byte[] createPassword(DelegationTokenIdentifier id) {
    byte[] password;
    DelegationTokenInformation info;
    synchronized (this) {
      password = super.createPassword(id);
      // add new token to shared store
      // need to persist expiration along with password
      info = super.currentTokens.remove(id);
      if (info == null) {
        throw new IllegalStateException("Failed to retrieve token after creation");
      }
    }
    this.tokenStore.addToken(id, info);
    return password;
  }

  @Override
  public long renewToken(Token<DelegationTokenIdentifier> token, String renewer) throws IOException {
    // since renewal is KERBEROS authenticated token may not be cached
    final DelegationTokenIdentifier id = getTokenIdentifier(token);
    DelegationTokenInformation tokenInfo = this.tokenStore.getToken(id);
    if (tokenInfo == null) {
        throw new InvalidToken("token does not exist: " + id); // no token found
    }
    // ensure associated master key is available
    if (!super.allKeys.containsKey(id.getMasterKeyId())) {
      LOGGER.info("Unknown master key (id={}), (re)loading keys from token store.",
        id.getMasterKeyId());
      reloadKeys();
    }
    // reuse super renewal logic
    synchronized (this) {
      super.currentTokens.put(id,  tokenInfo);
      try {
        long res = super.renewToken(token, renewer);
        this.tokenStore.removeToken(id);
        this.tokenStore.addToken(id, super.currentTokens.get(id));
        return res;
      } finally {
        super.currentTokens.remove(id);
      }
    }
  }

  public static String encodeWritable(Writable key) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    key.write(dos);
    dos.flush();
    return Base64.encodeBase64URLSafeString(bos.toByteArray());
  }

  public static void decodeWritable(Writable w, String idStr) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(Base64.decodeBase64(idStr)));
    w.readFields(in);
  }

  /**
   * Synchronize master key updates / sequence generation for multiple nodes.
   * NOTE: {@link AbstractDelegationTokenSecretManager} keeps currentKey private, so we need
   * to utilize this "hook" to manipulate the key through the object reference.
   * This .20S workaround should cease to exist when Hadoop supports token store.
   */
  @Override
  protected void logUpdateMasterKey(DelegationKey key) throws IOException {
    int keySeq = this.tokenStore.addMasterKey(encodeWritable(key));
    // update key with assigned identifier
    DelegationKey keyWithSeq = new DelegationKey(keySeq, key.getExpiryDate(), key.getKey());
    String keyStr = encodeWritable(keyWithSeq);
    this.tokenStore.updateMasterKey(keySeq, keyStr);
    decodeWritable(key, keyStr);
    LOGGER.info("New master key with key id={}", key.getKeyId());
    super.logUpdateMasterKey(key);
  }

  @Override
  public synchronized void startThreads() throws IOException {
    try {
      // updateCurrentKey needs to be called to initialize the master key
      // (there should be a null check added in the future in rollMasterKey)
      // updateCurrentKey();
      Method m = AbstractDelegationTokenSecretManager.class.getDeclaredMethod("updateCurrentKey");
      m.setAccessible(true);
      m.invoke(this);
    } catch (Exception e) {
      throw new IOException("Failed to initialize master key", e);
    }
    running = true;
    tokenRemoverThread = new Daemon(new ExpiredTokenRemover());
    tokenRemoverThread.start();
  }

  @Override
  public synchronized void stopThreads() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Stopping expired delegation token remover thread");
    }
    running = false;
    if (tokenRemoverThread != null) {
      tokenRemoverThread.interrupt();
    }
  }

  /**
   * Remove expired tokens. Replaces logic in {@link AbstractDelegationTokenSecretManager}
   * that cannot be reused due to private method access. Logic here can more efficiently
   * deal with external token store by only loading into memory the minimum data needed.
   */
  protected void removeExpiredTokens() {
    long now = System.currentTimeMillis();
    Iterator<DelegationTokenIdentifier> i = tokenStore.getAllDelegationTokenIdentifiers()
        .iterator();
    while (i.hasNext()) {
      DelegationTokenIdentifier id = i.next();
      if (now > id.getMaxDate()) {
        this.tokenStore.removeToken(id); // no need to look at token info
      } else {
        // get token info to check renew date
        DelegationTokenInformation tokenInfo = tokenStore.getToken(id);
        if (tokenInfo != null) {
          if (now > tokenInfo.getRenewDate()) {
            this.tokenStore.removeToken(id);
          }
        }
      }
    }
  }

  /**
   * Extension of rollMasterKey to remove expired keys from store.
   *
   * @throws IOException
   */
  protected void rollMasterKeyExt() throws IOException {
    Map<Integer, DelegationKey> keys = reloadKeys();
    int currentKeyId = super.currentId;
    MetastoreDelegationTokenSupport.rollMasterKey(TokenStoreDelegationTokenSecretManager.this);
    List<DelegationKey> keysAfterRoll = Arrays.asList(getAllKeys());
    for (DelegationKey key : keysAfterRoll) {
      keys.remove(key.getKeyId());
      if (key.getKeyId() == currentKeyId) {
        tokenStore.updateMasterKey(currentKeyId, encodeWritable(key));
      }
    }
    for (DelegationKey expiredKey : keys.values()) {
      LOGGER.info("Removing expired key id={}", expiredKey.getKeyId());
      try {
        tokenStore.removeMasterKey(expiredKey.getKeyId());
      } catch (Exception e) {
        LOGGER.error("Error removing expired key id={}", expiredKey.getKeyId(), e);
      }
    }
  }

  /**
   * Cloned from {@link AbstractDelegationTokenSecretManager} to deal with private access
   * restriction (there would not be an need to clone the remove thread if the remove logic was
   * protected/extensible).
   */
  protected class ExpiredTokenRemover extends Thread {
    private long lastMasterKeyUpdate;
    private long lastTokenCacheCleanup;

    @Override
    public void run() {
      LOGGER.info("Starting expired delegation token remover thread, "
          + "tokenRemoverScanInterval=" + tokenRemoverScanInterval
          / (60 * 1000) + " min(s)");
      while (running) {
        try {
          long now = System.currentTimeMillis();
          if (lastMasterKeyUpdate + keyUpdateInterval < now) {
            try {
              rollMasterKeyExt();
              lastMasterKeyUpdate = now;
            } catch (IOException e) {
              LOGGER.error("Master key updating failed. "
                  + StringUtils.stringifyException(e));
            }
          }
          if (lastTokenCacheCleanup + tokenRemoverScanInterval < now) {
            removeExpiredTokens();
            lastTokenCacheCleanup = now;
          }
          try {
            Thread.sleep(5000); // 5 seconds
          } catch (InterruptedException ie) {
            LOGGER
            .error("InterruptedException received for ExpiredTokenRemover thread "
                + ie);
          }
        } catch (Throwable t) {
          LOGGER.error("ExpiredTokenRemover thread received unexpected exception. "
                           + t, t);
          // Wait 5 seconds too in case of an exception, so we do not end up in busy waiting for
          // the solution for this exception
          try {
            Thread.sleep(5000); // 5 seconds
          } catch (InterruptedException ie) {
            LOGGER.error("InterruptedException received for ExpiredTokenRemover thread during " +
                "wait in exception sleep " + ie);
          }
        }
      }
    }
  }

}
