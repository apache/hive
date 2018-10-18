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

package org.apache.hadoop.hive.llap.security;

import java.io.IOException;

public interface LlapSigner {
  /** An object signable by a signer. */
  public interface Signable {
    /** Called by the signer to record key information as part of the message to be signed. */
    void setSignInfo(int masterKeyId);
    /** Called by the signer to get the serialized representation of the message to be signed. */
    byte[] serialize() throws IOException;
  }

  /** Message with the signature. */
  public static final class SignedMessage {
    public byte[] message, signature;
  }

  /** Serializes and signs the message. */
  SignedMessage serializeAndSign(Signable message) throws IOException;

  void checkSignature(byte[] message, byte[] signature, int keyId);

  void close();
}