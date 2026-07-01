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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.anon.ConstCode;
import org.apache.hadoop.hive.ql.anon.TestUtils;
import org.apache.hadoop.hive.ql.anon.anonymize.AnonymizerFactory;
import org.apache.hadoop.hive.ql.anon.anonymize.ProjectorFactory;
import org.apache.hadoop.hive.ql.anon.extract.AvroMessageExtractor;
import org.apache.hadoop.hive.ql.anon.extract.MessageExtractor;
import org.apache.hadoop.hive.ql.anon.extract.ProtobufMessageExtractor;
import org.apache.hadoop.hive.ql.anon.model.Address;
import org.apache.hadoop.hive.ql.anon.model.Msg3;
import org.apache.hadoop.hive.ql.anon.model.Msg4;
import org.apache.hadoop.hive.ql.anon.policy.DataErasurePolicy;
import org.apache.hadoop.hive.ql.anon.policy.DataErasureRule;
import org.apache.hadoop.hive.ql.anon.policy.DataErasureStatement;
import org.apache.hadoop.hive.ql.anon.proto.TestMessages;
import org.apache.hadoop.hive.ql.anon.utils.AvroUtils;
import org.apache.hadoop.hive.ql.anon.utils.ProtoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import static org.apache.hadoop.hive.ql.anon.consts.MsgConst.MSG_MSG_3;
import static org.apache.hadoop.hive.ql.anon.utils.MessageUtils.createMsg3;
public class TestFormatConformance {

  private final DataErasurePolicy policy = TestUtils.getTestPolicy();
  private final IntWritable schemaId = new IntWritable(MSG_MSG_3);

  private DataErasureStatement statement() {
    if (!policy.hasId(schemaId)) {
      throw new AssertionError("test policy has no statement for schema " + MSG_MSG_3);
    }
    return policy.getStmt(schemaId);
  }

  @Test
  public void testAnonymisePojoEncodings() {
    final DataErasureStatement statement = statement();
    for (final ColumnInternalFormat fmt : new ColumnInternalFormat[] {
        ColumnInternalFormat.JSON, ColumnInternalFormat.XML, ColumnInternalFormat.MSGPACK}) {
      final Msg3 out = (Msg3) AnonymizerFactory.getAnonymizer(fmt)
          .anonymize(createMsg3(1, 20), statement);
      Assertions.assertEquals("", out.getCountry(), fmt + ": country must be erased");
      Assertions.assertEquals("", out.getTelephone(), fmt + ": telephone must be erased");
      Assertions.assertEquals(1, out.getUserId(), fmt + ": identity field must be retained");
      Assertions.assertNotEquals("", out.getValue(), fmt + ": non-PII field must be retained");
    }
  }

  @Test
  public void testAnonymiseProtobuf() {
    final TestMessages.Msg3 out = (TestMessages.Msg3) AnonymizerFactory
        .getAnonymizer(ColumnInternalFormat.PROTOBUF)
        .anonymize(ProtoUtils.createMsg3(1, 20), statement());
    Assertions.assertEquals("", out.getCountry(), "protobuf: country must be erased");
    Assertions.assertEquals("", out.getTelephone(), "protobuf: telephone must be erased");
    Assertions.assertEquals(1, out.getUserId(), "protobuf: identity field must be retained");
    Assertions.assertNotEquals("", out.getValue(), "protobuf: non-PII field must be retained");
  }

  @Test
  public void testAnonymiseAvro() {
    final org.apache.hadoop.hive.ql.anon.avro.Msg3 out =
        (org.apache.hadoop.hive.ql.anon.avro.Msg3) AnonymizerFactory
            .getAnonymizer(ColumnInternalFormat.AVRO)
            .anonymize(AvroUtils.createMsg3(1, 20), statement());
    Assertions.assertEquals("", out.getCountry(), "avro: country must be erased");
    Assertions.assertEquals("", out.getTelephone(), "avro: telephone must be erased");
    Assertions.assertEquals(1, out.getUserId(), "avro: identity field must be retained");
    Assertions.assertNotEquals("", out.getValue(), "avro: non-PII field must be retained");
  }

  @Test
  public void testReplaceValueAllEncodings() {
    final DataErasureStatement repl = replaceStmtFor("country", "UNKNOWN");

    for (final ColumnInternalFormat fmt : new ColumnInternalFormat[] {
        ColumnInternalFormat.JSON, ColumnInternalFormat.XML, ColumnInternalFormat.MSGPACK}) {
      final Msg3 out = (Msg3) AnonymizerFactory.getAnonymizer(fmt).anonymize(createMsg3(1, 20), repl);
      Assertions.assertEquals("UNKNOWN", out.getCountry(), fmt + ": country must be replaced with the literal");
      Assertions.assertEquals(1, out.getUserId(), fmt + ": identity field must be retained");
      Assertions.assertNotEquals("", out.getValue(), fmt + ": non-targeted field must be retained");
    }

    final TestMessages.Msg3 proto = (TestMessages.Msg3) AnonymizerFactory
        .getAnonymizer(ColumnInternalFormat.PROTOBUF).anonymize(ProtoUtils.createMsg3(1, 20), repl);
    Assertions.assertEquals("UNKNOWN", proto.getCountry(), "protobuf: country must be replaced with the literal");
    Assertions.assertEquals(1, proto.getUserId(), "protobuf: identity field must be retained");

    final org.apache.hadoop.hive.ql.anon.avro.Msg3 avro =
        (org.apache.hadoop.hive.ql.anon.avro.Msg3) AnonymizerFactory
            .getAnonymizer(ColumnInternalFormat.AVRO).anonymize(AvroUtils.createMsg3(1, 20), repl);
    Assertions.assertEquals("UNKNOWN", avro.getCountry(), "avro: country must be replaced with the literal");
    Assertions.assertEquals(1, avro.getUserId(), "avro: identity field must be retained");
  }

  @Test
  public void testExtractIdentityAllEncodings() {
    for (final ConstCode code : new ConstCode[] {ConstCode.j, ConstCode.x, ConstCode.m}) {
      final Set<WritableComparable> ids = new HashSet<>();
      new MessageExtractor(code).extract(new Text("userId"), createMsg3(7, 20), ids);
      Assertions.assertTrue(ids.contains(new IntWritable(7)), code + ": identity must be extracted");
    }
    final Set<WritableComparable> avroIds = new HashSet<>();
    new AvroMessageExtractor(ConstCode.a)
        .extract(new Text("userId"), AvroUtils.createMsg3(7, 20), avroIds);
    Assertions.assertTrue(avroIds.contains(new IntWritable(7)), "avro: identity must be extracted");

    final Set<WritableComparable> protoIds = new HashSet<>();
    new ProtobufMessageExtractor(ConstCode.p)
        .extract(new Text("userId"), ProtoUtils.createMsg3(7, 20), protoIds);
    Assertions.assertTrue(protoIds.contains(new IntWritable(7)), "protobuf: identity must be extracted");
  }

  @Test
  public void testExtractWithNullNestedSubObjectDoesNotNpe() {
    for (final ConstCode code : new ConstCode[] {ConstCode.j, ConstCode.x, ConstCode.m}) {
      final Msg3 msg = createMsg3(42, 20);
      msg.setBankDetails(null);
      msg.setIpList(null);
      final Set<WritableComparable> ids = new HashSet<>();
      Assertions.assertDoesNotThrow(
          () -> new MessageExtractor(code).extract(new Text("userId"), msg, ids),
          code + ": extractor must null-guard an unset nested sub-object");
      Assertions.assertTrue(ids.contains(new IntWritable(42)),
          code + ": identity must still be extracted from a message with unset optional fields");
    }
  }

  @Test
  public void testNestedEraseAllEncodings() {
    final DataErasureStatement nested = stmtFor("bankDetails:*");

    for (final ColumnInternalFormat fmt : new ColumnInternalFormat[] {
        ColumnInternalFormat.JSON, ColumnInternalFormat.XML, ColumnInternalFormat.MSGPACK}) {
      final Msg3 out = (Msg3) AnonymizerFactory.getAnonymizer(fmt).anonymize(createMsg3(1, 20), nested);
      Assertions.assertNotNull(out.getBankDetails(), fmt + ": sub-object reference preserved");
      Assertions.assertEquals("", out.getBankDetails().getBankName(), fmt + ": nested leaf erased");
    }

    final org.apache.hadoop.hive.ql.anon.avro.Msg3 avroOut =
        (org.apache.hadoop.hive.ql.anon.avro.Msg3) AnonymizerFactory.getAnonymizer(ColumnInternalFormat.AVRO)
            .anonymize(AvroUtils.createMsg3(1, 20), nested);
    Assertions.assertEquals("", String.valueOf(avroOut.getBankDetails().getBankName()),
        "avro: nested leaf erased");

    final TestMessages.Msg3 protoOut = (TestMessages.Msg3) AnonymizerFactory
        .getAnonymizer(ColumnInternalFormat.PROTOBUF).anonymize(ProtoUtils.createMsg3(1, 20), nested);
    Assertions.assertEquals("", protoOut.getBankDetails().getBankName(),
        "protobuf: nested leaf erased");
  }

  @Test
  public void testNestedFieldEraseAvroProtobuf() {
    final DataErasureStatement nested = stmtFor("bankDetails:bankName");

    final org.apache.hadoop.hive.ql.anon.avro.Msg3 avroOut =
        (org.apache.hadoop.hive.ql.anon.avro.Msg3) AnonymizerFactory.getAnonymizer(ColumnInternalFormat.AVRO)
            .anonymize(AvroUtils.createMsg3(2, 20), nested);
    Assertions.assertEquals("", String.valueOf(avroOut.getBankDetails().getBankName()),
        "avro: nested bankName erased");

    final TestMessages.Msg3 protoOut = (TestMessages.Msg3) AnonymizerFactory
        .getAnonymizer(ColumnInternalFormat.PROTOBUF).anonymize(ProtoUtils.createMsg3(2, 20), nested);
    Assertions.assertEquals("", protoOut.getBankDetails().getBankName(),
        "protobuf: nested bankName erased");
  }

  @Test
  public void testProjectAllEncodings() {
    final DataErasureStatement statement = statement();

    for (final ColumnInternalFormat fmt : new ColumnInternalFormat[] {
        ColumnInternalFormat.JSON, ColumnInternalFormat.XML, ColumnInternalFormat.MSGPACK}) {
      final Msg3 src = createMsg3(1, 20);
      final Map<String, Object> p = ProjectorFactory.getProjector(fmt).project(src, statement);
      assertProjection(fmt.name(), p, src.getCountry(), src.getTelephone());
    }

    final TestMessages.Msg3 proto = ProtoUtils.createMsg3(1, 20);
    assertProjection("protobuf",
        ProjectorFactory.getProjector(ColumnInternalFormat.PROTOBUF).project(proto, statement),
        proto.getCountry(), proto.getTelephone());

    final org.apache.hadoop.hive.ql.anon.avro.Msg3 avro = AvroUtils.createMsg3(1, 20);
    assertProjection("avro",
        ProjectorFactory.getProjector(ColumnInternalFormat.AVRO).project(avro, statement),
        avro.getCountry(), avro.getTelephone());
  }

  private void assertProjection(final String fmt, final Map<String, Object> p,
                                final Object expCountry, final Object expTelephone) {
    Assertions.assertTrue(p.containsKey("country"), fmt + ": country must be projected");
    Assertions.assertTrue(p.containsKey("telephone"), fmt + ": telephone must be projected");
    Assertions.assertEquals(String.valueOf(expCountry), String.valueOf(p.get("country")),
        fmt + ": projected country value must match the source");
    Assertions.assertEquals(String.valueOf(expTelephone), String.valueOf(p.get("telephone")),
        fmt + ": projected telephone value must match the source");
    Assertions.assertFalse(p.containsKey("value"), fmt + ": non-targeted field must not leak into the projection");
    Assertions.assertFalse(p.containsKey("userId"), fmt + ": the identity field is not a projected rule");
  }

  @Test
  public void testProjectNestedFieldAllEncodings() {
    final DataErasureStatement nested = stmtFor("bankDetails:bankName");

    for (final ColumnInternalFormat fmt : new ColumnInternalFormat[] {
        ColumnInternalFormat.JSON, ColumnInternalFormat.XML, ColumnInternalFormat.MSGPACK}) {
      final Msg3 src = createMsg3(1, 20);
      final Map<String, Object> p = ProjectorFactory.getProjector(fmt).project(src, nested);
      Assertions.assertEquals(src.getBankDetails().getBankName(),
          String.valueOf(p.get("bankDetails:bankName")), fmt + ": nested bankName projected");
    }

    final org.apache.hadoop.hive.ql.anon.avro.Msg3 avro = AvroUtils.createMsg3(1, 20);
    Assertions.assertEquals(String.valueOf(avro.getBankDetails().getBankName()),
        String.valueOf(ProjectorFactory.getProjector(ColumnInternalFormat.AVRO)
            .project(avro, nested).get("bankDetails:bankName")),
        "avro: nested bankName projected");

    final TestMessages.Msg3 proto = ProtoUtils.createMsg3(1, 20);
    Assertions.assertEquals(proto.getBankDetails().getBankName(),
        String.valueOf(ProjectorFactory.getProjector(ColumnInternalFormat.PROTOBUF)
            .project(proto, nested).get("bankDetails:bankName")),
        "protobuf: nested bankName projected");
  }

  @Test
  public void testProjectWildcardSubObjectAvroProtobuf() {
    final DataErasureStatement star = stmtFor("bankDetails:*");

    final org.apache.hadoop.hive.ql.anon.avro.Msg3 avro = AvroUtils.createMsg3(1, 20);
    final Object av = ProjectorFactory.getProjector(ColumnInternalFormat.AVRO).project(avro, star).get("bankDetails:*");
    Assertions.assertTrue(av instanceof Map && !((Map<?, ?>) av).isEmpty(), "avro ':*' projects a sub-record map");
    Assertions.assertTrue(((Map<?, ?>) av).containsValue(String.valueOf(avro.getBankDetails().getBankName())),
        "avro ':*' includes the bankName value");

    final TestMessages.Msg3 proto = ProtoUtils.createMsg3(1, 20);
    final Object pv = ProjectorFactory.getProjector(ColumnInternalFormat.PROTOBUF).project(proto, star).get("bankDetails:*");
    Assertions.assertTrue(pv instanceof Map && !((Map<?, ?>) pv).isEmpty(), "protobuf ':*' projects a sub-message map");
    Assertions.assertTrue(((Map<?, ?>) pv).containsValue(proto.getBankDetails().getBankName()),
        "protobuf ':*' includes the bankName value");
  }

  @Test
  public void testRepeatedMessageEraseAllEncodings() {
    final DataErasureStatement all = stmtFor("addressBookList[*]:street");

    final Msg4 pojo = (Msg4) AnonymizerFactory.getAnonymizer(ColumnInternalFormat.JSON)
        .anonymize(pojoMsg4(), all);
    Assertions.assertEquals("", pojo.getAddressBookList().get(0).getStreet(), "pojo: [*] street[0] erased");
    Assertions.assertEquals("", pojo.getAddressBookList().get(1).getStreet(), "pojo: [*] street[1] erased");
    Assertions.assertEquals("US", pojo.getAddressBookList().get(0).getCountry(), "pojo: country retained");

    final org.apache.hadoop.hive.ql.anon.avro.Msg4 avro =
        (org.apache.hadoop.hive.ql.anon.avro.Msg4) AnonymizerFactory.getAnonymizer(ColumnInternalFormat.AVRO)
            .anonymize(avroMsg4(), all);
    Assertions.assertEquals("", String.valueOf(avro.getAddressBookList().get(0).getStreet()), "avro: [*] street[0] erased");
    Assertions.assertEquals("", String.valueOf(avro.getAddressBookList().get(1).getStreet()), "avro: [*] street[1] erased");

    final TestMessages.Msg4 proto = (TestMessages.Msg4) AnonymizerFactory.getAnonymizer(ColumnInternalFormat.PROTOBUF)
        .anonymize(protoMsg4(), all);
    Assertions.assertEquals("", proto.getAddressBook(0).getStreet(), "protobuf: [*] street[0] erased");
    Assertions.assertEquals("", proto.getAddressBook(1).getStreet(), "protobuf: [*] street[1] erased");
    Assertions.assertEquals("US", proto.getAddressBook(0).getCountry(), "protobuf: country retained");

    final DataErasureStatement de = stmtFor("addressBookList[country='DE']:street");

    final Msg4 pojoF = (Msg4) AnonymizerFactory.getAnonymizer(ColumnInternalFormat.JSON).anonymize(pojoMsg4(), de);
    Assertions.assertEquals("1 Main", pojoF.getAddressBookList().get(0).getStreet(), "pojo: US street retained under filter");
    Assertions.assertEquals("", pojoF.getAddressBookList().get(1).getStreet(), "pojo: DE street erased under filter");

    final org.apache.hadoop.hive.ql.anon.avro.Msg4 avroF =
        (org.apache.hadoop.hive.ql.anon.avro.Msg4) AnonymizerFactory.getAnonymizer(ColumnInternalFormat.AVRO)
            .anonymize(avroMsg4(), de);
    Assertions.assertEquals("1 Main", String.valueOf(avroF.getAddressBookList().get(0).getStreet()), "avro: US street retained under filter");
    Assertions.assertEquals("", String.valueOf(avroF.getAddressBookList().get(1).getStreet()), "avro: DE street erased under filter");

    final TestMessages.Msg4 protoF = (TestMessages.Msg4) AnonymizerFactory.getAnonymizer(ColumnInternalFormat.PROTOBUF)
        .anonymize(protoMsg4(), de);
    Assertions.assertEquals("1 Main", protoF.getAddressBook(0).getStreet(), "protobuf: US street retained under filter");
    Assertions.assertEquals("", protoF.getAddressBook(1).getStreet(), "protobuf: DE street erased under filter");
  }

  @Test
  public void testRepeatedMessageProjectAvroProtobuf() {
    final DataErasureStatement all = stmtFor("addressBookList[*]:street");

    final String avro = String.valueOf(
        ProjectorFactory.getProjector(ColumnInternalFormat.AVRO).project(avroMsg4(), all));
    Assertions.assertTrue(avro.contains("1 Main") && avro.contains("2 Haupt"),
        "avro: both street values projected from the list, got " + avro);

    final String proto = String.valueOf(
        ProjectorFactory.getProjector(ColumnInternalFormat.PROTOBUF).project(protoMsg4(), all));
    Assertions.assertTrue(proto.contains("1 Main") && proto.contains("2 Haupt"),
        "protobuf: both street values projected from the list, got " + proto);
  }

  private static Msg4 pojoMsg4() {
    final Msg4 m = new Msg4();
    m.setUserId(1);
    m.setAddressBookList(Arrays.asList(new Address("US", "1 Main"), new Address("DE", "2 Haupt")));
    return m;
  }

  private static org.apache.hadoop.hive.ql.anon.avro.Msg4 avroMsg4() {
    return org.apache.hadoop.hive.ql.anon.avro.Msg4.newBuilder().setUserId(1)
        .setAddressBookList(Arrays.asList(
            org.apache.hadoop.hive.ql.anon.avro.Address.newBuilder().setCountry("US").setStreet("1 Main").build(),
            org.apache.hadoop.hive.ql.anon.avro.Address.newBuilder().setCountry("DE").setStreet("2 Haupt").build()))
        .build();
  }

  private static TestMessages.Msg4 protoMsg4() {
    return TestMessages.Msg4.newBuilder().setUserId(1)
        .addAddressBook(TestMessages.Address.newBuilder().setCountry("US").setStreet("1 Main").build())
        .addAddressBook(TestMessages.Address.newBuilder().setCountry("DE").setStreet("2 Haupt").build())
        .build();
  }

  private static DataErasureStatement stmtFor(final String path) {
    final DataErasureStatement s = new DataErasureStatement();
    s.schemaId = String.valueOf(MSG_MSG_3);
    final DataErasureRule r = new DataErasureRule();
    r.path = path;
    r.action = "ERASE";
    s.rules.add(r);
    return s;
  }

  private static DataErasureStatement replaceStmtFor(final String field, final String value) {
    final DataErasureStatement s = new DataErasureStatement();
    s.schemaId = String.valueOf(MSG_MSG_3);
    final DataErasureRule r = new DataErasureRule();
    r.path = field;
    r.action = "REPLACE";
    r.value = value;
    s.rules.add(r);
    return s;
  }
}
