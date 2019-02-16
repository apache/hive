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

import java.io.Closeable;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;

/**
 * Interface for pluggable token store that can be implemented with shared external
 * storage for load balancing and high availability (for example using ZooKeeper).
 * Internal, store specific errors are translated into {@link TokenStoreException}.
 */
public interface DelegationTokenStore extends Configurable, Closeable {
  /**
   * Exception for internal token store errors that typically cannot be handled by the caller.
   */
  class TokenStoreException extends RuntimeException {
    private static final long serialVersionUID = -8693819817623074083L;

    public TokenStoreException(Throwable cause) {
      super(cause);
    }

    public TokenStoreException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Add new master key. The token store assigns and returns the sequence number.
   * Caller needs to use the identifier to update the key (since it is embedded in the key).
   *
   * @param s
   * @return sequence number for new key
   */
  int addMasterKey(String s) throws TokenStoreException;

  /**
   * Update master key (for expiration and setting store assigned sequence within key)
   * @param keySeq
   * @param s
   * @throws TokenStoreException
   */
  void updateMasterKey(int keySeq, String s) throws TokenStoreException;

  /**
   * Remove key for given id.
   * @param keySeq
   * @return false if key no longer present, true otherwise.
   */
  boolean removeMasterKey(int keySeq);

  /**
   * Return all master keys.
   * @return
   * @throws TokenStoreException
   */
  String[] getMasterKeys() throws TokenStoreException;

  /**
   * Add token. If identifier is already present, token won't be added.
   * @param tokenIdentifier
   * @param token
   * @return true if token was added, false for existing identifier
   */
  boolean addToken(DelegationTokenIdentifier tokenIdentifier,
      DelegationTokenInformation token) throws TokenStoreException;

  /**
   * Get token. Returns null if the token does not exist.
   * @param tokenIdentifier
   * @return
   */
  DelegationTokenInformation getToken(DelegationTokenIdentifier tokenIdentifier)
      throws TokenStoreException;

  /**
   * Remove token. Return value can be used by caller to detect concurrency.
   * @param tokenIdentifier
   * @return true if token was removed, false if it was already removed.
   * @throws TokenStoreException
   */
  boolean removeToken(DelegationTokenIdentifier tokenIdentifier) throws TokenStoreException;

  /**
   * List of all token identifiers in the store. This is used to remove expired tokens
   * and a potential scalability improvement would be to partition by master key id
   * @return
   */
  List<DelegationTokenIdentifier> getAllDelegationTokenIdentifiers() throws TokenStoreException;

  /**
   * @param hmsHandler ObjectStore used by DBTokenStore
   * @param serverMode indicate if this tokenstore is for Metastore and HiveServer2
   */
  void init(Object hmsHandler, HadoopThriftAuthBridge.Server.ServerMode serverMode);

}
