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

package org.apache.hadoop.hive.common.histogram.kll;

import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.hadoop.hive.ql.util.JavaDataModel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * KLL serialization utilities.
 */
public class KllUtils {

  private KllUtils() {
    throw new AssertionError("Suppress default constructor for non instantiation");
  }

  /**
   * KLL is serialized according to what provided by data-sketches library
   * @param out output stream to write to
   * @param kll KLL sketch that needs to be serialized
   * @throws IOException if an error occurs during serialization
   */
  public static void serializeKll(OutputStream out, KllFloatsSketch kll) throws IOException {
    out.write(kll.toByteArray());
  }

  /**
   * This function deserializes the serialized KLL sketch from a stream.
   * @param in input stream to be deserialized
   * @return KLL sketch
   * @throws IOException if errors occur while reading the stream
   */
  public static KllFloatsSketch deserializeKll(InputStream in) throws IOException {
    final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    final byte[] data = new byte[4];
    int nRead;

    while ((nRead = in.read(data, 0, data.length)) != -1) {
      buffer.write(data, 0, nRead);
    }

    buffer.flush();
    return KllFloatsSketch.heapify(Memory.wrap(buffer.toByteArray()));
  }

  /**
   * This function deserializes the serialized KLL sketch from a byte array.
   * @param buf to deserialize
   * @param start start index for deserialization
   * @param len start+len is deserialized
   * @return KLL sketch
   */
  public static KllFloatsSketch deserializeKll(byte[] buf, int start, int len) {
    InputStream is = new ByteArrayInputStream(buf, start, len);
    try {
      KllFloatsSketch result = deserializeKll(is);
      is.close();
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This function deserializes the serialized KLL sketch from a byte array.
   * @param buf to deserialize
   * @return KLL sketch
   */
  public static KllFloatsSketch deserializeKll(final byte[] buf) {
    return deserializeKll(buf, 0, buf.length);
  }

  /**
   * Returns the length of the given KLL sketch according to the given java data model.
   * @param model the java data model to compute the length
   * @param kll the KLL sketch to compute the length for
   * @return the length of the given KLL sketch according to the given java data model
   */
  public static int lengthFor(JavaDataModel model, KllFloatsSketch kll) {
    return model == null ? KllFloatsSketch.getMaxSerializedSizeBytes(kll.getK(), kll.getN())
        : (int) model.lengthForByteArrayOfSize(kll.getSerializedSizeBytes());
  }
}
