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

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.hive.llap.security.LlapSigner.Signable;
import org.apache.hadoop.hive.llap.security.LlapSigner.SignedMessage;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.security.token.delegation.HiveDelegationTokenSupport;
import org.junit.Test;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLlapSignerImpl {
  private static final Logger LOG = LoggerFactory.getLogger(TestLlapSignerImpl.class);

  @Test(timeout = 10000)
  @Ignore("HIVE-22621: test case is unstable")
  public void testSigning() throws Exception {
    FakeSecretManager fsm = new FakeSecretManager();
    fsm.startThreads();

    // Make sure the signature works.
    LlapSignerImpl signer = new LlapSignerImpl(fsm);
    byte theByte = 1;
    TestSignable in = new TestSignable(theByte);
    TestSignable in2 = new TestSignable(++theByte);
    SignedMessage sm = signer.serializeAndSign(in);
    SignedMessage sm2 = signer.serializeAndSign(in2);
    TestSignable out = TestSignable.deserialize(sm.message);
    TestSignable out2 = TestSignable.deserialize(sm2.message);
    assertEquals(in, out);
    assertEquals(in2, out2);
    signer.checkSignature(sm.message, sm.signature, out.masterKeyId);
    signer.checkSignature(sm2.message, sm2.signature, out2.masterKeyId);

    // Make sure the broken signature doesn't work.
    try {
      signer.checkSignature(sm.message, sm2.signature, out.masterKeyId);
      fail("Didn't throw");
    } catch (SecurityException ex) {
      // Expected.
    }

    int index = sm.signature.length / 2;
    sm.signature[index] = (byte)(sm.signature[index] + 1);
    try {
      signer.checkSignature(sm.message, sm.signature, out.masterKeyId);
      fail("Didn't throw");
    } catch (SecurityException ex) {
      // Expected.
    }
    sm.signature[index] = (byte)(sm.signature[index] - 1);

    fsm = rollKey(fsm, out.masterKeyId);
    signer = new LlapSignerImpl(fsm);
    // Sign in2 with a different key.
    sm2 = signer.serializeAndSign(in2);
    out2 = TestSignable.deserialize(sm2.message);
    assertNotEquals(out.masterKeyId, out2.masterKeyId);
    assertEquals(in2, out2);
    signer.checkSignature(sm2.message, sm2.signature, out2.masterKeyId);
    signer.checkSignature(sm.message, sm.signature, out.masterKeyId);
    // Make sure the key ID mismatch causes error.
    try {
      signer.checkSignature(sm2.message, sm2.signature, out.masterKeyId);
      fail("Didn't throw");
    } catch (SecurityException ex) {
      // Expected.
    }

    // The same for rolling the key; re-create the fsm with only the key #2.
    fsm = rollKey(fsm, out2.masterKeyId);
    signer = new LlapSignerImpl(fsm);
    signer.checkSignature(sm2.message, sm2.signature, out2.masterKeyId);
    // The key is missing - shouldn't be able to verify.
    try {
      signer.checkSignature(sm.message, sm.signature, out.masterKeyId);
      fail("Didn't throw");
    } catch (SecurityException ex) {
      // Expected.
    }
    fsm.stopThreads();
  }

  private FakeSecretManager rollKey(FakeSecretManager fsm, int idToPreserve) throws IOException {
    // Adding keys is PITA - there's no way to plug into timed rolling; just create a new fsm.
    DelegationKey dk = fsm.getDelegationKey(idToPreserve), curDk = fsm.getCurrentKey();
    if (curDk == null || curDk.getKeyId() != idToPreserve) {
      LOG.warn("The current key is not the one we expect; key rolled in background? Signed with "
          + idToPreserve + " but got " + (curDk == null ? "null" : curDk.getKeyId()));
    }
    // Regardless of the above, we should have the key we've signed with.
    assertNotNull(dk);
    assertEquals(idToPreserve, dk.getKeyId());
    fsm.stopThreads();
    fsm = new FakeSecretManager();
    fsm.addKey(dk);
    assertNotNull("Couldn't add key", fsm.getDelegationKey(dk.getKeyId()));
    fsm.startThreads();
    return fsm;
  }

  private static class TestSignable implements Signable {
    public int masterKeyId;
    public byte index;

    public TestSignable(byte i) {
      index = i;
    }

    public TestSignable(int keyId, byte b) {
      masterKeyId = keyId;
      index = b;
    }

    @Override
    public void setSignInfo(int masterKeyId) {
      this.masterKeyId = masterKeyId;
    }

    @Override
    public byte[] serialize() throws IOException {
      DataOutputBuffer dob = new DataOutputBuffer(5);
      dob.writeInt(masterKeyId);
      dob.write(index);
      byte[] b = dob.getData();
      dob.close();
      return b;
    }

    public static TestSignable deserialize(byte[] bytes) throws IOException {
      DataInputBuffer db = new DataInputBuffer();
      db.reset(bytes, bytes.length);
      int keyId = db.readInt();
      byte b = db.readByte();
      db.close();
      return new TestSignable(keyId, b);
    }

    @Override
    public int hashCode() {
      return 31 * index + masterKeyId;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof TestSignable)) return false;
      TestSignable other = (TestSignable) obj;
      return (index == other.index) && (masterKeyId == other.masterKeyId);
    }
  }

  private static class FakeSecretManager
    extends AbstractDelegationTokenSecretManager<AbstractDelegationTokenIdentifier>
    implements SigningSecretManager {

    public FakeSecretManager() {
      super(10000000, 10000000, 10000000, 10000000);
    }

    @Override
    public DelegationKey getCurrentKey() {
      // We cannot synchronize properly with the internal thread (or something is weird about
      // the parent class internal state) and occasionally get null keys; loop until we can get
      // the key.
      long endTimeMs = System.nanoTime() + 3 * 1000000000L; // Wait for at most 3 sec.
      int keyId = -1;
      do {
        keyId = getCurrentKeyId();
        DelegationKey key = getDelegationKey(keyId);
        if (key != null) return key;
        if (keyId > 0 && keyId == getCurrentKeyId()) {
          throw new AssertionError("The ID didn't change but we couldn't get the key " + keyId);
        }
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      } while ((endTimeMs - System.nanoTime()) > 0);
      throw new AssertionError("Cannot get a key from the base class; the last ID was " + keyId);
    }

    @Override
    public DelegationKey getDelegationKey(int keyId) {
      return super.getDelegationKey(keyId);
    }

    @Override
    public byte[] signWithKey(byte[] message, DelegationKey key) {
      return createPassword(message, key.getKey());
    }

    @Override
    public byte[] signWithKey(byte[] message, int keyId) throws SecurityException {
      DelegationKey key = getDelegationKey(keyId);
      if (key == null) {
        throw new SecurityException("The key ID " + keyId + " was not found");
      }
      return createPassword(message, key.getKey());
    }

    @Override
    public AbstractDelegationTokenIdentifier createIdentifier() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      stopThreads();
    }
  }
}
