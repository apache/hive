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
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;


/**
 * A {@link KryoSerializer} that does not serialize hash codes while serializing a
 * {@link HiveKey}. This decreases the amount of data to be shuffled during a Spark shuffle.
 */
public class NoHashCodeKryoSerializer extends KryoSerializer {

  private static final long serialVersionUID = 3350910170041648022L;

  public NoHashCodeKryoSerializer(SparkConf conf) {
    super(conf);
  }

  @Override
  public Kryo newKryo() {
    Kryo kryo = super.newKryo();
    kryo.register(HiveKey.class, new HiveKeySerializer());
    kryo.register(BytesWritable.class, new HiveKryoRegistrator.BytesWritableSerializer());
    return kryo;
  }

  private static class HiveKeySerializer extends Serializer<HiveKey> {

    public void write(Kryo kryo, Output output, HiveKey object) {
      output.writeVarInt(object.getLength(), true);
      output.write(object.getBytes(), 0, object.getLength());
    }

    public HiveKey read(Kryo kryo, Input input, Class<? extends HiveKey> type) {
      int len = input.readVarInt(true);
      byte[] bytes = new byte[len];
      input.readBytes(bytes);
      return new HiveKey(bytes);
    }
  }
}
