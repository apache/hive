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

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hive.ql.anon.convert.JsonBodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.MsgpackBodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.XmlBodyConverter;
import org.apache.hadoop.hive.ql.anon.model.Msg3;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.apache.hadoop.hive.ql.anon.utils.MessageUtils.createMsg3;

public class TestUnmodelledFieldRoundTrip {

  private static final String EXTRA_KEY = "loyaltyTier";
  private static final String EXTRA_VAL = "GOLD";

  private static Msg3 seedWithExtra() {
    final Msg3 m = createMsg3(7, 20);
    m.setUnmodelledField(EXTRA_KEY, EXTRA_VAL);
    return m;
  }

  @Test
  public void jsonCapturesAndReemitsExternalUnmodelledField() {
    final String upstream =
        "{\"userId\":7,\"value\":\"keep\",\"" + EXTRA_KEY + "\":\"" + EXTRA_VAL + "\"}";

    final Msg3 back = (Msg3) JsonBodyConverter.convert(upstream, Msg3.class);
    Assertions.assertEquals(7, back.getUserId(), "json: modelled field must decode");
    Assertions.assertEquals(EXTRA_VAL, String.valueOf(back.getUnmodelledFields().get(EXTRA_KEY)),
        "json: decode must capture the unmodelled field, not drop or reject it");

    final String reencoded = JsonBodyConverter.serializeMsg(back);
    Assertions.assertTrue(reencoded.contains(EXTRA_KEY) && reencoded.contains(EXTRA_VAL),
        "json: encode must re-emit the captured unmodelled field, got " + reencoded);
  }

  @Test
  public void jsonEncodeDecodeKeepsUnmodelledField() {
    final String enc = JsonBodyConverter.serializeMsg(seedWithExtra());
    assertEmittedThenCaptured("json", enc, JsonBodyConverter.convert(enc, Msg3.class));
  }

  @Test
  public void xmlEncodeDecodeKeepsUnmodelledField() {
    final String enc = XmlBodyConverter.serializeMsg(seedWithExtra());
    assertEmittedThenCaptured("xml", enc, XmlBodyConverter.convert(enc, Msg3.class));
  }

  @Test
  public void msgpackEncodeDecodeKeepsUnmodelledField() throws Exception {
    final String hex = MsgpackBodyConverter.serializeMsgToHex(seedWithExtra());
    final String wire = new String(Hex.decodeHex(hex.toCharArray()), StandardCharsets.UTF_8);
    assertEmittedThenCaptured("msgpack", wire, MsgpackBodyConverter.convert(hex, Msg3.class));
  }

  private static void assertEmittedThenCaptured(final String fmt, final String wire, final Msg3 back) {
    Assertions.assertTrue(wire.contains(EXTRA_KEY) && wire.contains(EXTRA_VAL),
        fmt + ": encode (@JsonAnyGetter) must re-emit the unmodelled field, got " + wire);
    Assertions.assertEquals(7, back.getUserId(), fmt + ": modelled field must round-trip");
    Assertions.assertEquals(EXTRA_VAL, String.valueOf(back.getUnmodelledFields().get(EXTRA_KEY)),
        fmt + ": decode (@JsonAnySetter) must capture the unmodelled field");
  }
}
