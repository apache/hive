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
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.hadoop.hive.ql.anon.utils.Utils;
import org.apache.hadoop.hive.ql.anon.model.BaseMsg;
import org.apache.hadoop.hive.ql.anon.model.Msg1;
import org.apache.hadoop.hive.ql.anon.model.Msg2;
import org.apache.hadoop.hive.ql.anon.model.Msg3;
import org.apache.hadoop.hive.ql.anon.model.Msg4;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import static org.apache.hadoop.hive.ql.anon.consts.MsgConst.*;

public class XmlBodyConverter implements BodyConverter {

  private static final XmlMapper mapper = new XmlMapper();

  @Override
  public BaseMsg convertBody(final WritableComparable msgId, final Writable body) {
    final int id = Utils.convertNumberWritable(msgId);
    final Text text = (Text) body;
    final String json = text.toString();
    try {
      switch (id) {
        case MSG_MSG_1:
          return mapper.readValue(json, Msg1.class);
        case MSG_MSG_2:
          return mapper.readValue(json, Msg2.class);
        case MSG_MSG_3:
          return mapper.readValue(json, Msg3.class);
        case MSG_MSG_4:
          return mapper.readValue(json, Msg4.class);
        default: {
          throw new RuntimeException("Unknown msg id: " + id);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Text convertMessage(final Object msg) {
    final String json = serializeMsg(msg);
    return new Text(json);
  }

  public static String serializeMsg(final Object msg) {
    try {
      return mapper.writeValueAsString(msg);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T extends BaseMsg> T convert(final String xml, final Class<T> clazz) {
    try {
      return mapper.readValue(xml, clazz);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
