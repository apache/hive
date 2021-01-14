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

package org.apache.hadoop.hive.ql.io.protobuf;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

/**
 * Class to convert bytes writable containing a protobuf message to hive formats.
 * @see ProtobufSerDe
 */
public class ProtobufBytesWritableSerDe extends ProtobufSerDe {
  private Parser<? extends Message> parser;

  @Override
  public void initialize(Configuration configuration, Properties tableProperties, Properties partitionProperties)
      throws SerDeException {
    super.initialize(configuration, tableProperties, partitionProperties);

    try {
      @SuppressWarnings("unchecked")
      Parser<? extends Message> tmpParser = (Parser<? extends Message>)protoMessageClass
          .getField("PARSER").get(null);
      this.parser = tmpParser;
    } catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException
        | SecurityException e) {
      throw new SerDeException("Unable get PARSER from class: " + protoMessageClass.getName(), e);
    }
  }

  @Override
  protected Message toMessage(Writable writable) throws SerDeException {
    try {
      BytesWritable bytes = (BytesWritable)writable;
      return parser.parseFrom(bytes.getBytes(), 0, bytes.getLength());
    } catch (InvalidProtocolBufferException e) {
      throw new SerDeException("Unable to parse proto message", e);
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BytesWritable.class;
  }
}
