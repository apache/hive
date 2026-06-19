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

package org.apache.hadoop.hive.ql.anon.format;

import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.anon.TestUtils;
import org.apache.hadoop.hive.ql.anon.anonymize.Anonymizer;
import org.apache.hadoop.hive.ql.anon.anonymize.AnonymizerFactory;
import org.apache.hadoop.hive.ql.anon.model.BankDetails;
import org.apache.hadoop.hive.ql.anon.model.Msg3;
import org.apache.hadoop.hive.ql.anon.policy.DataErasurePolicy;
import org.apache.hadoop.hive.ql.anon.policy.DataErasureRule;
import org.apache.hadoop.hive.ql.anon.policy.DataErasureStatement;
import org.apache.hadoop.hive.ql.anon.proto.TestMessages;
import org.apache.hadoop.hive.ql.anon.utils.ProtoUtils;
import org.apache.hadoop.io.IntWritable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.hive.ql.anon.consts.MsgConst.MSG_MSG_3;
import static org.apache.hadoop.hive.ql.anon.utils.MessageUtils.createMsg3;

public class TestMessageAnonymizer {

  private final DataErasurePolicy policy = TestUtils.getTestPolicy();
  private final IntWritable schemaId = new IntWritable(MSG_MSG_3);

  @Test
  public void testBaseMsgAnonymizer() {
    if (!policy.hasId(schemaId)) {
      throw new AssertionError("Schema ID not set");
    }
    final DataErasureStatement statement = policy.getStmt(schemaId);
    final Msg3 msg3 = createMsg3(1, 20);

    final Anonymizer anonymizer = AnonymizerFactory.getAnonymizer(ColumnInternalFormat.JSON);
    final Msg3 outMsg = (Msg3) anonymizer.anonymize(msg3, statement);
    Assertions.assertEquals("", outMsg.getCountry());
    Assertions.assertEquals("", outMsg.getTelephone());
    Assertions.assertEquals("", outMsg.getCountry());
    for (final String ip : outMsg.getIpList()) {
      Assertions.assertEquals("", ip);
    }
    Assertions.assertNotEquals("", outMsg.getValue());
  }

  @Test
  public void testEraseSubObjectWildcard() {
    final DataErasureStatement statement = new DataErasureStatement();
    statement.schemaId = String.valueOf(MSG_MSG_3);
    final DataErasureRule rule = new DataErasureRule();
    rule.path = "bankDetails:*";
    rule.action = "ERASE";
    statement.rules.add(rule);

    final Msg3 msg3 = createMsg3(1, 20);
    final BankDetails before = msg3.getBankDetails();
    Assertions.assertNotNull(before);
    Assertions.assertNotEquals("", before.getBankName());
    Assertions.assertNotEquals("", before.getCardNum());
    Assertions.assertNotEquals("", before.getPinCode());

    final Anonymizer anonymizer = AnonymizerFactory.getAnonymizer(ColumnInternalFormat.JSON);
    final Msg3 outMsg = (Msg3) anonymizer.anonymize(msg3, statement);

    final BankDetails after = outMsg.getBankDetails();
    Assertions.assertNotNull(after, "sub-object reference must be preserved (schema-preservation property)");
    Assertions.assertEquals("", after.getBankName());
    Assertions.assertEquals("", after.getCardNum());
    Assertions.assertEquals("", after.getPinCode());
  }

  @Test
  public void testEraseSubObjectWithoutWildcardIsRejected() {
    final DataErasureStatement statement = new DataErasureStatement();
    statement.schemaId = String.valueOf(MSG_MSG_3);
    final DataErasureRule rule = new DataErasureRule();
    rule.path = "bankDetails";
    rule.action = "ERASE";
    statement.rules.add(rule);

    final Msg3 msg3 = createMsg3(1, 20);
    final Anonymizer anonymizer = AnonymizerFactory.getAnonymizer(ColumnInternalFormat.JSON);
    final RuntimeException thrown = Assertions.assertThrows(RuntimeException.class,
        () -> anonymizer.anonymize(msg3, statement));
    Assertions.assertTrue(thrown.getMessage() != null
            && thrown.getMessage().contains("sub-object")
            && thrown.getMessage().contains(":*"),
        "expected a clear error mentioning the missing ':*' wildcard, got: " + thrown.getMessage());
  }

  @Test
  public void testEraseSubObjectSingleLeaf() {
    final DataErasureStatement statement = new DataErasureStatement();
    statement.schemaId = String.valueOf(MSG_MSG_3);
    final DataErasureRule rule = new DataErasureRule();
    rule.path = "bankDetails:bankName";
    rule.action = "ERASE";
    statement.rules.add(rule);

    final Msg3 msg3 = createMsg3(1, 20);
    final BankDetails before = msg3.getBankDetails();
    final String originalCardNum = before.getCardNum();
    final String originalPinCode = before.getPinCode();
    Assertions.assertNotEquals("", originalCardNum);
    Assertions.assertNotEquals("", originalPinCode);

    final Anonymizer anonymizer = AnonymizerFactory.getAnonymizer(ColumnInternalFormat.JSON);
    final Msg3 outMsg = (Msg3) anonymizer.anonymize(msg3, statement);

    final BankDetails after = outMsg.getBankDetails();
    Assertions.assertNotNull(after);
    Assertions.assertEquals("", after.getBankName(), "targeted leaf must be erased");
    Assertions.assertEquals(originalCardNum, after.getCardNum(), "sibling leaf must be unchanged");
    Assertions.assertEquals(originalPinCode, after.getPinCode(), "sibling leaf must be unchanged");
  }

  @Test
  public void testEraseSubObjectWildcardThenReplaceLeaf() {
    final DataErasureStatement statement = new DataErasureStatement();
    statement.schemaId = String.valueOf(MSG_MSG_3);
    final DataErasureRule eraseSubObject = new DataErasureRule();
    eraseSubObject.path = "bankDetails:*";
    eraseSubObject.action = "ERASE";
    statement.rules.add(eraseSubObject);
    final DataErasureRule replaceLeaf = new DataErasureRule();
    replaceLeaf.path = "bankDetails:cardNum";
    replaceLeaf.value = "REDACTED";
    replaceLeaf.action = "REPLACE";
    statement.rules.add(replaceLeaf);

    final Msg3 msg3 = createMsg3(1, 20);
    final Anonymizer anonymizer = AnonymizerFactory.getAnonymizer(ColumnInternalFormat.JSON);
    final Msg3 outMsg = (Msg3) anonymizer.anonymize(msg3, statement);

    final BankDetails after = outMsg.getBankDetails();
    Assertions.assertNotNull(after);
    Assertions.assertEquals("", after.getBankName(),
        "bankName erased by the wildcard rule, no second rule restores it");
    Assertions.assertEquals("REDACTED", after.getCardNum(),
        "cardNum erased by the wildcard rule then overwritten by the inner REPLACE; "
            + "the STRICTEST resolver folds these into a single ERASE at ATTACH time");
    Assertions.assertEquals("", after.getPinCode(),
        "pinCode erased by the wildcard rule, no second rule restores it");
  }

  @Test
  public void testProtoAnonymizer() {
    if (!policy.hasId(schemaId)) {
      throw new AssertionError("Schema ID not set");
    }
    final DataErasureStatement statement = policy.getStmt(schemaId);
    final TestMessages.Msg3 msg3 = ProtoUtils.createMsg3(1, 20);

    final Anonymizer anonymizer = AnonymizerFactory.getAnonymizer(ColumnInternalFormat.PROTOBUF);
    final TestMessages.Msg3 outMsg = (TestMessages.Msg3) anonymizer.anonymize(msg3, statement);
    Assertions.assertEquals("", outMsg.getCountry());
    Assertions.assertEquals("", outMsg.getTelephone());
    Assertions.assertEquals("", outMsg.getCountry());
    for (final String ip : outMsg.getIpList()) {
      Assertions.assertEquals("", ip);
    }
    Assertions.assertNotEquals("", outMsg.getValue());
  }

}
