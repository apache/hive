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

package org.apache.hadoop.hive.ql.anon.convert;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hive.ql.anon.utils.Utils;
import org.apache.hadoop.hive.ql.anon.model.BaseMsg;
import org.apache.hadoop.hive.ql.anon.model.Msg1;
import org.apache.hadoop.hive.ql.anon.model.Msg2;
import org.apache.hadoop.hive.ql.anon.model.Msg3;
import org.apache.hadoop.hive.ql.anon.model.Msg4;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.msgpack.jackson.dataformat.MessagePackMapper;

import java.io.IOException;

import static org.apache.hadoop.hive.ql.anon.consts.MsgConst.*;

public class MsgpackBodyConverter implements BodyConverter {

  private static final Hex hex = new Hex();
  private static final MessagePackMapper mapper = new MessagePackMapper();

  @Override
  public BaseMsg convertBody(final WritableComparable msgId, final Writable body) {
    final int id = Utils.convertNumberWritable(msgId);
    final BytesWritable bw = (BytesWritable) body;
    final byte[] bytes = bw.getBytes();
    final int len = bw.getLength();
    try {
      switch (id) {
        case MSG_MSG_1:
          return mapper.readValue(bytes, 0, len, Msg1.class);
        case MSG_MSG_2:
          return mapper.readValue(bytes, 0, len, Msg2.class);
        case MSG_MSG_3:
          return mapper.readValue(bytes, 0, len, Msg3.class);
        case MSG_MSG_4:
          return mapper.readValue(bytes, 0, len, Msg4.class);
        default: {
          throw new RuntimeException("Unknown msg id: " + id);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public BytesWritable convertMessage(final Object msg) {
    try {
      final byte[] bytes = mapper.writeValueAsBytes(msg);
      return new BytesWritable(bytes);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static String serializeMsgToHex(final BaseMsg msg) {
    try {
      final byte[] bytes = mapper.writeValueAsBytes(msg);
      final byte[] hexBytes = hex.encode(bytes);
      return new String(hexBytes);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T extends BaseMsg> T convert(final String hexString, final Class<T> clazz) {
    try {
      final byte[] bytes = hex.decode(hexString.getBytes());
      return mapper.readValue(bytes, clazz);
    } catch (DecoderException | IOException e) {
      throw new RuntimeException(e);
    }
  }
}
