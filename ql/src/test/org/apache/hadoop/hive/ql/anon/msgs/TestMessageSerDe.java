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

package org.apache.hadoop.hive.ql.anon.msgs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hive.ql.anon.convert.JsonBodyConverter;
import org.apache.hadoop.hive.ql.anon.model.BaseMsg;
import org.apache.hadoop.hive.ql.anon.utils.MessageUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.msgpack.jackson.dataformat.MessagePackMapper;

import java.io.IOException;

public class TestMessageSerDe {

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final MessagePackMapper mapper2 = new MessagePackMapper();
  private static final Hex hex = new Hex();

  @Test
  public void testMsg3() throws IOException {
    BaseMsg msg = MessageUtils.createMsg3(1, 20);
    String json = mapper.writeValueAsString(msg);
    Text body = new Text(json);
    IntWritable msgId = new IntWritable(2);
    JsonBodyConverter jsonBodyConverter = new JsonBodyConverter();
    BaseMsg tmp = jsonBodyConverter.convertBody(msgId, body);
    Assertions.assertNotNull(tmp);
  }

  @Test
  public void test() throws IOException {
    String s = "01234";
    s = s.substring(1, s.length() - 1);
    Assertions.assertEquals("123", s);
  }

  @Test
  public void testBase64() throws JsonProcessingException {
    BaseMsg msg = MessageUtils.createMsg3(1, 20);
    byte[] bytes = mapper2.writeValueAsBytes(msg);


    Text encodedText = new Text(Base64.encodeBase64(bytes));
    Text hexText = new Text(hex.encode(bytes));

    System.out.println("bytes length: " + bytes.length);
    System.out.println(encodedText);
    System.out.println(hexText);
  }

}
