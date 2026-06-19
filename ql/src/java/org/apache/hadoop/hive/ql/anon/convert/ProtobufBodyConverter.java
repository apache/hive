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

import com.google.protobuf.GeneratedMessageV3;
import java.lang.reflect.Method;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hive.ql.anon.proto.TestMessages;
import org.apache.hadoop.hive.ql.anon.utils.Utils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import static org.apache.hadoop.hive.ql.anon.consts.MsgConst.*;
public class ProtobufBodyConverter implements BodyConverter {

  private static final Hex hex = new Hex();

  @Override
  public GeneratedMessageV3 convertBody(final WritableComparable msgId, final Writable body) {
    final int id = Utils.convertNumberWritable(msgId);
    final BytesWritable bytesWritable = (BytesWritable) body;
    final byte[] data1 = bytesWritable.getBytes();
    final int len = bytesWritable.getLength();
    final byte[] data = new byte[len];
    System.arraycopy(data1, 0, data, 0, len);

    try {
      switch (id) {
        case MSG_MSG_1:
          return TestMessages.Msg1.parseFrom(data);
        case MSG_MSG_2:
          return TestMessages.Msg2.parseFrom(data);
        case MSG_MSG_3:
          return TestMessages.Msg3.parseFrom(data);
        case MSG_MSG_4:
          return TestMessages.Msg4.parseFrom(data);
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
    final byte[] data = serializeMsg((GeneratedMessageV3) msg);
    return new BytesWritable(data);
  }

  public static byte[] serializeMsg(final GeneratedMessageV3 msg) {
    return msg.toByteArray();
  }

  public static String serializeMsgToHex(final GeneratedMessageV3 msg) {
    final byte[] data = serializeMsg(msg);
    final byte[] hexBytes = hex.encode(data);
    return new String(hexBytes);
  }

  public static <T extends GeneratedMessageV3> T convert(final String hexData, final Class<T> clazz) {
    try {
      final byte[] bytes = hex.decode(hexData.getBytes());
      final Method parseFrom = clazz.getMethod("parseFrom", byte[].class);
      return clazz.cast(parseFrom.invoke(null, (Object) bytes));
    } catch (DecoderException | ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

}
