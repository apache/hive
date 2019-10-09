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

package org.apache.hadoop.hive.serde2.text;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * AbstractEncodingAwareSerDe aware the encoding from table properties,
 * transform data from specified charset to UTF-8 during serialize, and
 * transform data from UTF-8 to specified charset during deserialize.
 */
public abstract class AbstractEncodingAwareSerDe extends AbstractTextSerDe {

  protected Charset charset;

  @Override
  public void initialize(final Configuration configuration,
      final Properties tableProperties) throws SerDeException {
    super.initialize(configuration, tableProperties);
    this.charset = Charset.forName(tableProperties.getProperty(
        serdeConstants.SERIALIZATION_ENCODING, StandardCharsets.UTF_8.name()));
    if (this.charset.equals(StandardCharsets.ISO_8859_1)
        || this.charset.equals(StandardCharsets.US_ASCII)) {
      LOG.warn("The data may not be properly converted to target charset "
          + charset);
    }
  }

  /**
   * Deserialize an object out of a Writable blob. This class assumes that the
   * data is stored in a text format, so the blob must be of type {@link Text}.
   * In most cases, the return value of this function will be constant since the
   * function will reuse the returned object. If the client wants to keep a copy
   * of the object, the client needs to clone the returned value by calling
   * ObjectInspectorUtils.getStandardObject().
   *
   * This class can be initialized with the character matching the source data.
   * This method will convert the provided {@link Text} to UTF-8 from the
   * specified character set before being processed.
   *
   * @param blob The Writable object containing a serialized object
   * @return A Java object representing the contents in the blob.
   * @throws SerDeException If the object cannot be deserialized
   */
  @Override
  public Object deserialize(final Writable blob) throws SerDeException {
    final Text text = (Text) blob;
    final Writable clob = (StandardCharsets.UTF_8.equals(this.charset)) ? text
        : transformToUTF8(text);
    return super.deserialize(clob);
  }

  /**
   * Serialize an object by navigating inside the Object with the
   * ObjectInspector. In most cases, the return value of this function will be
   * constant since the function will reuse the Writable object. If the client
   * wants to keep a copy of the Writable, the client needs to clone the
   * returned value.
   *
   * This class can be initialized with the character matching the source data.
   * This method will convert the {@link Text} provided by subclasses from UTF-8
   * to the specified character set of the data.
   *
   * @param obj The object to serialize
   * @param objInspector The ObjectInspector to reference to navigate the object
   *          to serialize
   * @return The object serialized into a single {@link Text} object
   * @throws SerDeException If the object cannot be serialized
   */
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {
    final Text text = (Text) super.serialize(obj, objInspector);
    final Writable result = (StandardCharsets.UTF_8.equals(this.charset)) ? text
        : transformFromUTF8(text, this.charset);
    return result;
  }

  /**
   * Transform Text data from UTF-8 to another charset.
   *
   * @param text The unicode text to encode
   * @param cs The target character set of the text
   * @return The text encoded in the specified character set
   */
  private Text transformFromUTF8(final Text text, final Charset cs) {
    return new Text(new String(text.getBytes(), 0, text.getLength(), cs));
  }

  /**
   * Transform Writable data from its native charset to UTF-8.
   *
   * @param text The text to encode
   * @return The text encoded as UTF-8
   */
  private Text transformToUTF8(final Text text) {
    return new Text(new String(text.getBytes(), 0, text.getLength(),
        StandardCharsets.UTF_8));
  }
}
