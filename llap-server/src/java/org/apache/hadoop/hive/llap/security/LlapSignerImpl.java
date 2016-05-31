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
package org.apache.hadoop.hive.llap.security;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.DaemonId;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.DelegationKey;

import com.google.common.annotations.VisibleForTesting;

public class LlapSignerImpl implements LlapSigner {
  private final SigningSecretManager secretManager;

  public LlapSignerImpl(Configuration conf, DaemonId daemonId) {
    // TODO: create this centrally in HS2 case
    secretManager = SecretManager.createSecretManager(conf, daemonId.getClusterString());
  }

  @VisibleForTesting
  public LlapSignerImpl(SigningSecretManager sm) {
    secretManager = sm;
  }

  @Override
  public SignedMessage serializeAndSign(Signable message) throws IOException {
    SignedMessage result = new SignedMessage();
    DelegationKey key = secretManager.getCurrentKey();
    message.setSignInfo(key.getKeyId(), UserGroupInformation.getCurrentUser().getUserName());
    result.message = message.serialize();
    result.signature = secretManager.signWithKey(result.message, key);
    return result;
  }

  @Override
  public void checkSignature(byte[] message, byte[] signature, int keyId)
      throws SecurityException {
    byte[] expectedSignature = secretManager.signWithKey(message, keyId);
    if (Arrays.equals(signature, expectedSignature)) return;
    throw new SecurityException("Message signature does not match");
  }
}
