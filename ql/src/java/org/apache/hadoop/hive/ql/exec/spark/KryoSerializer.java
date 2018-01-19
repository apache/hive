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

package org.apache.hadoop.hive.ql.exec.spark;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoSerializer {
  private static final Logger LOG = LoggerFactory.getLogger(KryoSerializer.class);

  public static byte[] serialize(Object object) {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    Output output = new Output(stream);

    Kryo kryo = SerializationUtilities.borrowKryo();
    kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
    try {
      kryo.writeObject(output, object);
    } finally {
      SerializationUtilities.releaseKryo(kryo);
    }

    output.close(); // close() also calls flush()
    return stream.toByteArray();
  }

  public static <T> T deserialize(byte[] buffer, Class<T> clazz) {
    Kryo kryo = SerializationUtilities.borrowKryo();
    kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
    T result = null;
    try {
      result = kryo.readObject(new Input(new ByteArrayInputStream(buffer)), clazz);
    } finally {
      SerializationUtilities.releaseKryo(kryo);
    }
    return result;
  }

  public static byte[] serializeJobConf(JobConf jobConf) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      jobConf.write(new DataOutputStream(out));
    } catch (IOException e) {
      LOG.error("Error serializing job configuration: " + e, e);
      return null;
    } finally {
      try {
        out.close();
      } catch (IOException e) {
        LOG.error("Error closing output stream: " + e, e);
      }
    }

    return out.toByteArray();

  }

  public static JobConf deserializeJobConf(byte[] buffer) {
    JobConf conf = new JobConf();
    try {
      conf.readFields(new DataInputStream(new ByteArrayInputStream(buffer)));
    } catch (IOException e) {
      String msg = "Error de-serializing job configuration: " + e;
      throw new IllegalStateException(msg, e);
    }
    return conf;
  }

}
