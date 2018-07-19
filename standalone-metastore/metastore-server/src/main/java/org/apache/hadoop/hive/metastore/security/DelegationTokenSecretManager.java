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
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;

/**
 * A Hive specific delegation token secret manager.
 * The secret manager is responsible for generating and accepting the password
 * for each token.
 */
public class DelegationTokenSecretManager
    extends AbstractDelegationTokenSecretManager<DelegationTokenIdentifier> {

  /**
   * Create a secret manager
   * @param delegationKeyUpdateInterval the number of seconds for rolling new
   *        secret keys.
   * @param delegationTokenMaxLifetime the maximum lifetime of the delegation
   *        tokens
   * @param delegationTokenRenewInterval how often the tokens must be renewed
   * @param delegationTokenRemoverScanInterval how often the tokens are scanned
   *        for expired tokens
   */
  public DelegationTokenSecretManager(long delegationKeyUpdateInterval,
                                      long delegationTokenMaxLifetime,
                                      long delegationTokenRenewInterval,
                                      long delegationTokenRemoverScanInterval) {
    super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
          delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
  }

  @Override
  public DelegationTokenIdentifier createIdentifier() {
    return new DelegationTokenIdentifier();
  }

  /**
   * Verify token string
   * @param tokenStrForm
   * @return user name
   * @throws IOException
   */
  public synchronized String verifyDelegationToken(String tokenStrForm) throws IOException {
    Token<DelegationTokenIdentifier> t = new Token<>();
    t.decodeFromUrlString(tokenStrForm);

    DelegationTokenIdentifier id = getTokenIdentifier(t);
    verifyToken(id, t.getPassword());
    return id.getUser().getShortUserName();
  }

  protected DelegationTokenIdentifier getTokenIdentifier(Token<DelegationTokenIdentifier> token)
      throws IOException {
    // turn bytes back into identifier for cache lookup
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    DelegationTokenIdentifier id = createIdentifier();
    id.readFields(in);
    return id;
  }

  public synchronized void cancelDelegationToken(String tokenStrForm) throws IOException {
    Token<DelegationTokenIdentifier> t= new Token<>();
    t.decodeFromUrlString(tokenStrForm);
    String user = UserGroupInformation.getCurrentUser().getUserName();
    cancelToken(t, user);
  }

  public synchronized long renewDelegationToken(String tokenStrForm) throws IOException {
    Token<DelegationTokenIdentifier> t= new Token<>();
    t.decodeFromUrlString(tokenStrForm);
    //when a token is created the renewer of the token is stored
    //as shortName in AbstractDelegationTokenIdentifier.setRenewer()
    //this seems like an inconsistency because while cancelling the token
    //it uses the shortname to compare the renewer while it does not use
    //shortname during token renewal. Use getShortUserName() until its fixed
    //in HADOOP-15068
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    return renewToken(t, user);
  }

  public synchronized String getDelegationToken(final String ownerStr, final String renewer) throws IOException {
    if (ownerStr == null) {
      throw new RuntimeException("Delegation token owner is null");
    }
    Text owner = new Text(ownerStr);
    Text realUser = null;
    UserGroupInformation currentUgi = UserGroupInformation.getCurrentUser();
    if (currentUgi.getUserName() != null) {
      realUser = new Text(currentUgi.getUserName());
    }
    DelegationTokenIdentifier ident =
      new DelegationTokenIdentifier(owner, new Text(renewer), realUser);
    Token<DelegationTokenIdentifier> t = new Token<>(
        ident, this);
    return t.encodeToUrlString();
  }

  public String getUserFromToken(String tokenStr) throws IOException {
    Token<DelegationTokenIdentifier> delegationToken = new Token<>();
    delegationToken.decodeFromUrlString(tokenStr);

    ByteArrayInputStream buf = new ByteArrayInputStream(delegationToken.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    DelegationTokenIdentifier id = createIdentifier();
    id.readFields(in);
    return id.getUser().getShortUserName();
  }
}

