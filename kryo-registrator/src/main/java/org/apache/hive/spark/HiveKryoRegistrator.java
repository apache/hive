/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.spark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.serializer.KryoRegistrator;

/**
 * Kryo registrator for shuffle data, i.e. HiveKey and BytesWritable.
 *
 * Active use (e.g. reflection to get a class instance) of this class on hive side can cause
 * problems because kryo is relocated in hive-exec.
 */
public class HiveKryoRegistrator implements KryoRegistrator {
  @Override
  public void registerClasses(Kryo kryo) {
    kryo.register(HiveKey.class, new HiveKeySerializer());
    kryo.register(BytesWritable.class, new BytesWritableSerializer());
  }

  private static class HiveKeySerializer extends Serializer<HiveKey> {

    public void write(Kryo kryo, Output output, HiveKey object) {
      output.writeVarInt(object.getLength(), true);
      output.write(object.getBytes(), 0, object.getLength());
      output.writeVarInt(object.hashCode(), false);
    }

    public HiveKey read(Kryo kryo, Input input, Class<HiveKey> type) {
      int len = input.readVarInt(true);
      byte[] bytes = new byte[len];
      input.readBytes(bytes);
      return new HiveKey(bytes, input.readVarInt(false));
    }
  }

  private static class BytesWritableSerializer extends Serializer<BytesWritable> {

    public void write(Kryo kryo, Output output, BytesWritable object) {
      output.writeVarInt(object.getLength(), true);
      output.write(object.getBytes(), 0, object.getLength());
    }

    public BytesWritable read(Kryo kryo, Input input, Class<BytesWritable> type) {
      int len = input.readVarInt(true);
      byte[] bytes = new byte[len];
      input.readBytes(bytes);
      return new BytesWritable(bytes);
    }

  }
}
