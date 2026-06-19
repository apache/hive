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

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.hive.ql.anon.TestUtils;
import org.apache.hadoop.hive.ql.anon.convert.AvroBodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.BodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.JsonBodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.MsgpackBodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.ProtobufBodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.XmlBodyConverter;
import org.apache.hadoop.hive.ql.anon.model.Msg3;
import org.apache.hadoop.hive.ql.anon.utils.AvroUtils;
import org.apache.hadoop.hive.ql.anon.utils.MessageUtils;
import org.apache.hadoop.hive.ql.anon.utils.ProtoUtils;
import org.apache.hadoop.hive.ql.anon.proto.TestMessages;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TestMessageGenerator {

  @Test
  public void testProto() throws IOException {
    final TestMessages.Msg3 msg3 = ProtoUtils.createMsg3(1, 20);
    final byte[] bytes = msg3.toByteArray();

    final TestMessages.Msg3 m = TestMessages.Msg3.parseFrom(bytes);
    final String hex = ProtobufBodyConverter.serializeMsgToHex(msg3);
    System.out.println(hex);

    TestMessages.Msg3 out = ProtobufBodyConverter.convert("08011214464E2D544B736959785756715944506152522D311A144C4E2D544B736959785756715944506152522D31221441442D544B736959785756715944506152522D312A0032003A14454D2D544B736959785756715944506152522D3142004A1442442D544B736959785756715944506152522D315213562D544B736959785756715944506152522D315A005A005A005A0062420A14424E2D544B736959785756715944506152522D311214434E2D544B736959785756715944506152522D311A1450432D544B736959785756715944506152522D31", TestMessages.Msg3.class);
    int d = 1;
  }

  @Test
  public void testAvro() throws IOException {
    final org.apache.hadoop.hive.ql.anon.avro.Msg3 msg3 = AvroUtils.createMsg3(1, 20);

    final String hex = AvroBodyConverter.serializeMsgToHex(msg3);
    System.out.println(hex);

    SpecificRecordBase out = AvroBodyConverter.convert("0228464E2D6F694A56456A76746E7A41526E4D672D31284C4E2D6F694A56456A76746E7A41526E4D672D312841442D6F694A56456A76746E7A41526E4D672D31000028454D2D6F694A56456A76746E7A41526E4D672D31002842442D6F694A56456A76746E7A41526E4D672D3126562D6F694A56456A76746E7A41526E4D672D3108000000000028424E2D6F694A56456A76746E7A41526E4D672D3128434E2D6F694A56456A76746E7A41526E4D672D312850432D6F694A56456A76746E7A41526E4D672D31",
      org.apache.hadoop.hive.ql.anon.avro.Msg3.class);
    int d = 1;
  }

  @Test
  public void testJson() {
    final Msg3 msg3 = MessageUtils.createMsg3(1, 20);
    final BodyConverter converter = new JsonBodyConverter();
    final Writable writable = converter.convertMessage(msg3);
    System.out.println(writable.toString());
  }

 @Test
  public void testXml() {
    final Msg3 msg3 = MessageUtils.createMsg3(1, 20);
    final BodyConverter converter = new XmlBodyConverter();
    final Writable writable = converter.convertMessage(msg3);
    System.out.println(writable.toString());
  }


 @Test
  public void testMsgPack() {
    final Msg3 msg3 = MessageUtils.createMsg3(1, 20);
    final String hex = MsgpackBodyConverter.serializeMsgToHex(msg3);
    System.out.println(hex);

    Msg3 out = MsgpackBodyConverter.convert("8CA675736572496401A966697273744E616D65B4464E2D6E6E664A716D55565073544B73744E2D31A86C6173744E616D65B44C4E2D6E6E664A716D55565073544B73744E2D31A761646472657373B441442D6E6E664A716D55565073544B73744E2D31A463697479A0A7636F756E747279A0A5656D61696CB4454D2D6E6E664A716D55565073544B73744E2D31A974656C6570686F6E65A0A9626972746844617465B442442D6E6E664A716D55565073544B73744E2D31A576616C7565B3562D6E6E664A716D55565073544B73744E2D31A669704C69737494A0A0A0A0AB62616E6B44657461696C7383A862616E6B4E616D65B4424E2D6E6E664A716D55565073544B73744E2D31A7636172644E756DB4434E2D6E6E664A716D55565073544B73744E2D31A770696E436F6465B450432D6E6E664A716D55565073544B73744E2D31", Msg3.class);
    int d = 1;
  }


}

