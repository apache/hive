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
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.google.common.base.Strings;
import org.apache.hive.spark.counter.SparkCounters;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;


public class TestJobResultSerializer {

  @Test
  public void testSerializablableExceptionSingleBlankException() {
    RuntimeException blankRuntimeException = new RuntimeException();

    RuntimeException serializableException = JobResultSerializer.convertToSerializableSparkException(
            blankRuntimeException);

    assertException(serializableException, blankRuntimeException);
  }

  @Test
  public void testSerializablableExceptionSingleException() {
    RuntimeException blankRuntimeException = new RuntimeException("hello");

    RuntimeException serializableException = JobResultSerializer.convertToSerializableSparkException(
            blankRuntimeException);

    assertException(serializableException, blankRuntimeException);
  }

  @Test
  public void testSerializablableExceptionNestedBlankException() {
    RuntimeException nestedBlankRuntimeException = new RuntimeException();
    RuntimeException blankRuntimeException = new RuntimeException(nestedBlankRuntimeException);

    RuntimeException serializableException = JobResultSerializer.convertToSerializableSparkException(
            blankRuntimeException);

    assertNestedException(serializableException, blankRuntimeException);
  }

  @Test
  public void testSerializablableExceptionNestedException() {
    RuntimeException nestedRuntimeException = new RuntimeException("hello");
    RuntimeException blankRuntimeException = new RuntimeException(nestedRuntimeException);

    RuntimeException serializableException = JobResultSerializer.convertToSerializableSparkException(
            blankRuntimeException);

    assertNestedException(serializableException, blankRuntimeException);

    nestedRuntimeException = new RuntimeException();
    blankRuntimeException = new RuntimeException("hello", nestedRuntimeException);

    serializableException = JobResultSerializer.convertToSerializableSparkException(
            blankRuntimeException);

    assertNestedException(serializableException, blankRuntimeException);

    nestedRuntimeException = new RuntimeException("hello");
    blankRuntimeException = new RuntimeException("hello", nestedRuntimeException);

    serializableException = JobResultSerializer.convertToSerializableSparkException(
            blankRuntimeException);

    assertNestedException(serializableException, blankRuntimeException);
  }

  private void assertException(Throwable serializedException, Throwable originalException) {
    Assert.assertEquals(originalException.getClass().getName() + ": " + Strings.nullToEmpty(
            originalException.getMessage()), serializedException.getMessage());
    Assert.assertArrayEquals(originalException.getStackTrace(),
            serializedException.getStackTrace());
  }

  private void assertNestedException(Throwable serializedException, Throwable originalException) {
    assertException(serializedException, originalException);
    assertException(serializedException.getCause(), originalException.getCause());
  }

  @Test
  public void testSerializeNonSerializableObject() {
    Kryo kryo = new Kryo();
    kryo.addDefaultSerializer(BaseProtocol.JobResult.class, new JobResultSerializer());

    ByteArrayOutputStream boas = new ByteArrayOutputStream();
    Output output = new Output(boas);

    String id = "1";
    String result = "result";
    SparkCounters counters = new SparkCounters(null);

    BaseProtocol.JobResult<String> jobResult = new BaseProtocol.JobResult<>(id, result, new
            NonSerializableException("content"), counters);

    kryo.writeClassAndObject(output, jobResult);
    output.flush();

    Input kryoIn = new Input(new ByteArrayInputStream(boas.toByteArray()));
    Object deserializedObject = kryo.readClassAndObject(kryoIn);

    Assert.assertTrue(deserializedObject instanceof BaseProtocol.JobResult);

    BaseProtocol.JobResult<String> deserializedJobResult = (BaseProtocol.JobResult<String>) deserializedObject;

    Assert.assertEquals(id, deserializedJobResult.id);
    Assert.assertEquals(result, deserializedJobResult.result);
    Assert.assertEquals(counters.toString(), deserializedJobResult.sparkCounters.toString());
    Assert.assertTrue(deserializedJobResult.error instanceof RuntimeException);
  }

  private static final class NonSerializableException extends Exception {

    private static final long serialVersionUID = 2548414562750016219L;

    private final NonSerializableObject nonSerializableObject;

    private NonSerializableException(String content) {
      this.nonSerializableObject = new NonSerializableObject(content);
    }
  }

  private static final class NonSerializableObject {

    private String content;

    private NonSerializableObject(String content) {
      this.content = content;
    }
  }
}
