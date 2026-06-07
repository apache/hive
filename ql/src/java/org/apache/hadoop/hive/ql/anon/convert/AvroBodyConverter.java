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

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hive.ql.anon.utils.Utils;
import org.apache.hadoop.hive.ql.anon.avro.Msg1;
import org.apache.hadoop.hive.ql.anon.avro.Msg2;
import org.apache.hadoop.hive.ql.anon.avro.Msg3;
import org.apache.hadoop.hive.ql.anon.avro.Msg4;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.apache.hadoop.hive.ql.anon.consts.MsgConst.*;

public class AvroBodyConverter implements BodyConverter {

  private static final Hex hex = new Hex();

  @Override
  public SpecificRecordBase convertBody(final WritableComparable msgId, final Writable body) {
    final int id = Utils.convertNumberWritable(msgId);
    final BytesWritable bytesWritable = (BytesWritable) body;
    final byte[] data1 = bytesWritable.getBytes();
    final int len = bytesWritable.getLength();
    final byte[] data = new byte[len];
    System.arraycopy(data1, 0, data, 0, len);

    try {
      switch (id) {
        case MSG_MSG_1: {
          DatumReader<Msg1> reader = new SpecificDatumReader<>(Msg1.getClassSchema());
          BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
          Msg1 out = reader.read(null, decoder);
          return out;
        }
        case MSG_MSG_2: {
          DatumReader<Msg2> reader = new SpecificDatumReader<>(Msg2.getClassSchema());
          BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
          Msg2 out = reader.read(null, decoder);
          return out;
        }
        case MSG_MSG_3: {
          DatumReader<Msg3> reader = new SpecificDatumReader<>(Msg3.getClassSchema());
          BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
          Msg3 out = reader.read(null, decoder);
          return out;
        }
        case MSG_MSG_4: {
          DatumReader<Msg4> reader = new SpecificDatumReader<>(Msg4.getClassSchema());
          BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
          Msg4 out = reader.read(null, decoder);
          return out;
        }
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
    final byte[] data = serializeMsg((SpecificRecordBase) msg);
    return new BytesWritable(data);
  }

  public static byte[] serializeMsg(final SpecificRecordBase msg) {
    DatumWriter<SpecificRecordBase> writer = null;
    if (msg instanceof Msg3) {
      writer = new SpecificDatumWriter<>(Msg3.getClassSchema());
    } else if (msg instanceof Msg1) {
      writer = new SpecificDatumWriter<>(Msg1.getClassSchema());
    } else if (msg instanceof Msg2) {
      writer = new SpecificDatumWriter<>(Msg2.getClassSchema());
    } else if (msg instanceof Msg4) {
      writer = new SpecificDatumWriter<>(Msg4.getClassSchema());
    } else {
      throw new RuntimeException("Unknown msg type: " + msg.getClass());
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);

    try {
      writer.write(msg, encoder);
      encoder.flush();
      baos.flush();
      byte[] bytes = baos.toByteArray();
      return bytes;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static String serializeMsgToHex(final SpecificRecordBase msg) {
    final byte[] data = serializeMsg(msg);
    final byte[] hexBytes = hex.encode(data);
    return new String(hexBytes);
  }

  public static SpecificRecordBase convert(final String hexData, final Class<? extends SpecificRecordBase> clazz) {
    try {
      final byte[] bytes = hex.decode(hexData.getBytes());
      final DatumReader<SpecificRecordBase> reader;
      if (clazz == Msg1.class) {
        reader = new SpecificDatumReader<>(Msg1.getClassSchema());
      } else if (clazz == Msg2.class) {
        reader = new SpecificDatumReader<>(Msg2.getClassSchema());
      } else if (clazz == Msg3.class) {
        reader = new SpecificDatumReader<>(Msg3.getClassSchema());
      } else if (clazz == Msg4.class) {
        reader = new SpecificDatumReader<>(Msg4.getClassSchema());
      } else {
        throw new RuntimeException("Unknown msg type: " + clazz);
      }

      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
      SpecificRecordBase out = reader.read(null, decoder);
      return out;
    } catch (DecoderException | IOException e) {
      throw new RuntimeException(e);
    }
  }

}
