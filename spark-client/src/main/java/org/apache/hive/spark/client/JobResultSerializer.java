/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.spark.client;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Objects;


/**
 * A custom {@link Serializer} for serializing / deserializing {@link BaseProtocol.JobResult}
 * objects. This class uses Java serialization to write / read the JobResult objects. This has
 * the nice property that it is able to successfully serialize Java {@link Throwable}s. Whereas
 * the Kryo serializer cannot (because certain objects in a Throwable don't have public zero-arg
 * constructors.
 *
 * <p>
 *   Given that any developer can write a custom exception that contains non-serializable objects
 *   (e.g. objects that don't implement {@link java.io.Serializable}), this class needs to handle
 *   the case where the given Throwable cannot be serialized by Java. In this case, the
 *   serializer will recursively go through the {@link Throwable} and wrap all objects with a
 *   {@link RuntimeException} which is guaranteed to be serializable.
 * </p>
 */
public class JobResultSerializer extends Serializer<BaseProtocol.JobResult<?>> {

  private static final Logger LOG = LoggerFactory.getLogger(JobResultSerializer.class);

  @Override
  public BaseProtocol.JobResult<?> read(Kryo kryo, Input input, Class type) {
    try {
      return (BaseProtocol.JobResult<?>) new ObjectInputStream(input).readObject();
    } catch (Exception e) {
      throw new KryoException("Error during Java deserialization.", e);
    }
  }

  @Override
  public void write(Kryo kryo, Output output, BaseProtocol.JobResult<?> object) {
    try {
      safeWriteToOutput(output, object);
    } catch (Exception e) {
      LOG.warn("Unable to serialize JobResult object " + object, e);

      BaseProtocol.JobResult<?> serializableJobResult = new BaseProtocol.JobResult<>(object.id,
              object.result, convertToSerializableSparkException(object.error),
              object.sparkCounters);
      try {
        safeWriteToOutput(output, serializableJobResult);
      } catch (Exception ex) {
        throw new KryoException("Error during Java serialization.", ex);
      }
    }
  }

  private void safeWriteToOutput(Output output,
                                 BaseProtocol.JobResult<?> jobResult) throws IOException {
    ByteArrayOutputStream boas = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(boas);

    oos.writeObject(jobResult);
    oos.flush();

    output.write(boas.toByteArray());
    output.flush();
  }

  @VisibleForTesting
  static RuntimeException convertToSerializableSparkException(Throwable error) {
    RuntimeException serializableThrowable = new RuntimeException(
            error.getClass().getName() + ": " + Objects.toString(error.getMessage(), ""),
            error.getCause() == null ? null : convertToSerializableSparkException(
                    error.getCause()));

    serializableThrowable.setStackTrace(error.getStackTrace());

    Arrays.stream(error.getSuppressed())
            .map(JobResultSerializer::convertToSerializableSparkException)
            .forEach(serializableThrowable::addSuppressed);

    return serializableThrowable;
  }
}
