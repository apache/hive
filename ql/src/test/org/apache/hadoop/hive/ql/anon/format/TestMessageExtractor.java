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

import com.google.protobuf.GeneratedMessageV3;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.hive.ql.anon.extract.AvroMessageExtractor;
import org.apache.hadoop.hive.ql.anon.extract.MessageExtractor;
import org.apache.hadoop.hive.ql.anon.extract.ProtobufMessageExtractor;
import org.apache.hadoop.hive.ql.anon.model.BaseMsg;
import org.apache.hadoop.hive.ql.anon.model.Msg1;
import org.apache.hadoop.hive.ql.anon.model.Msg2;
import org.apache.hadoop.hive.ql.anon.model.Msg3;
import org.apache.hadoop.hive.ql.anon.proto.TestMessages;
import org.apache.hadoop.hive.ql.anon.utils.AvroUtils;
import org.apache.hadoop.hive.ql.anon.utils.ProtoUtils;
import org.apache.hadoop.io.WritableComparable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.IDENTITY_FIELD_NAME;
import static org.apache.hadoop.hive.ql.anon.utils.MessageUtils.*;

public class TestMessageExtractor {

  @Test
  public void testContains() {
    Assertions.assertFalse(MessageExtractor.contains(Msg1.class, IDENTITY_FIELD_NAME));
    Assertions.assertTrue(MessageExtractor.contains(Msg2.class, IDENTITY_FIELD_NAME));
    Assertions.assertTrue(MessageExtractor.contains(Msg3.class, IDENTITY_FIELD_NAME));
  }

  @Test
  public void testContainsProto() {
    Assertions.assertFalse(ProtobufMessageExtractor.contains2(TestMessages.Msg1.class, IDENTITY_FIELD_NAME));
    Assertions.assertTrue(ProtobufMessageExtractor.contains2(TestMessages.Msg2.class, IDENTITY_FIELD_NAME));
    Assertions.assertTrue(ProtobufMessageExtractor.contains2(TestMessages.Msg3.class, IDENTITY_FIELD_NAME));
  }

  @Test
  public void testContainsAvro() {
    Assertions.assertFalse(AvroMessageExtractor.contains2(org.apache.hadoop.hive.ql.anon.avro.Msg1.class, IDENTITY_FIELD_NAME));
    Assertions.assertTrue(AvroMessageExtractor.contains2(org.apache.hadoop.hive.ql.anon.avro.Msg2.class, IDENTITY_FIELD_NAME));
    Assertions.assertTrue(AvroMessageExtractor.contains2(org.apache.hadoop.hive.ql.anon.avro.Msg3.class, IDENTITY_FIELD_NAME));
  }

  @Test
  public void testExtract1() {
    BaseMsg msg = createMsg1(111L);
    Set<WritableComparable> set = new HashSet<>();
    MessageExtractor.extract(IDENTITY_FIELD_NAME, msg, set);
    Assertions.assertEquals(0, set.size());
  }

  @Test
  public void testExtract2() {
    BaseMsg msg = createMsg2(1);
    Set<WritableComparable> set = new HashSet<>();
    MessageExtractor.extract(IDENTITY_FIELD_NAME, msg, set);
    Assertions.assertEquals(1, set.size());
  }

  @Test
  public void testExtract3() {
    BaseMsg msg = createMsg3(1, 20);
    Set<WritableComparable> set = new HashSet<>();
    MessageExtractor.extract(IDENTITY_FIELD_NAME, msg, set);
    Assertions.assertEquals(1, set.size());
  }

  @Test
  public void testExtractProto3() {
    GeneratedMessageV3 msg = ProtoUtils.createMsg3(1, 20);
    Set<WritableComparable> set = new HashSet<>();
    ProtobufMessageExtractor.extract2(IDENTITY_FIELD_NAME, msg, set);
    Assertions.assertEquals(1, set.size());
  }

  @Test
  public void testExtractAvro3() {
    SpecificRecordBase msg = AvroUtils.createMsg3(1, 20);
    Set<WritableComparable> set = new HashSet<>();
    AvroMessageExtractor.extract2(IDENTITY_FIELD_NAME, msg, set);
    Assertions.assertEquals(1, set.size());
  }

}
