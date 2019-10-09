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

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base class for any Hive serializer or deserializer (SerDe) which supports a
 * text-based format.
 */
public abstract class AbstractTextSerDe extends AbstractSerDe {

  public static final String IGNORE_EMPTY_LINES = "text.ignore.empty.line";

  protected final Logger LOG = LoggerFactory.getLogger(getClass());

  private List<String> columnNames = Collections.emptyList();

  private ObjectInspector objectInspector;

  private final Text cachedWritable = new Text();

  private final EnumSet<Feature> features = EnumSet.noneOf(Feature.class);

  /**
   * Enumeration that defines all on/off features for this SerDe.
   * <ul>
   * <li>{@link #ERROR_ON_BLANK_LINE}</li>
   * </ul>
   */
  public enum Feature {
    /**
     * If this feature is enabled, an exception is raised if the serializer or
     * deserializer encounters an empty line in the text file.
     */
    ERROR_ON_BLANK_LINE
  }

  @Override
  public void initialize(final Configuration configuration,
      final Properties tableProperties) throws SerDeException {

    LOG.debug("Table Configuration: {}", configuration);
    LOG.debug("Table Properties: {}", tableProperties);

    this.columnNames = Collections.unmodifiableList(Arrays.asList(
        tableProperties.getProperty(serdeConstants.LIST_COLUMNS).split(",")));

    final List<ObjectInspector> objectInspectors = Collections
        .unmodifiableList(Collections.nCopies(this.columnNames.size(),
            PrimitiveObjectInspectorFactory.javaStringObjectInspector));

    this.objectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(this.columnNames, objectInspectors);

    if (!Boolean
        .valueOf(tableProperties.getProperty(IGNORE_EMPTY_LINES, "true"))) {
      features.add(Feature.ERROR_ON_BLANK_LINE);
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
   * @param blob The Writable object containing a serialized object
   * @return A Java object representing the contents in the blob.
   * @throws SerDeException If the object cannot be deserialized
   */
  @Override
  public Object deserialize(final Writable blob) throws SerDeException {
    Objects.requireNonNull(blob, "Cannot deserialize null BLOB");

    final Text rowText = (Text) blob;

    LOG.trace("Row data: [{}]", rowText);

    if (features.contains(Feature.ERROR_ON_BLANK_LINE)
        && rowText.getLength() == 0) {
      throw new SerDeException("Text data contains blank line");
    }

    return doDeserialize(rowText.toString());
  }

  /**
   * Serialize an object by navigating inside the Object with the
   * ObjectInspector. In most cases, the return value of this function will be
   * constant since the function will reuse the Writable object. If the client
   * wants to keep a copy of the Writable, the client needs to clone the
   * returned value.
   *
   * @param obj The object to serialize
   * @param objInspector The ObjectInspector to reference to navigate the object
   *          to serialize
   * @return The object serialized into a single {@link Text} object
   * @throws SerDeException If the object cannot be serialized
   */
  @Override
  public Writable serialize(final Object obj,
      final ObjectInspector objInspector) throws SerDeException {
    Objects.requireNonNull(obj, "Cannot serialize null object");
    Objects.requireNonNull(objInspector, "objInspector cannot be null");
    cachedWritable.set(doSerialize(obj, objInspector));
    return cachedWritable;
  }

  /**
   * Subclasses can override this method to allow for special serialization. The
   * returned String is always expected to be UTF-8 encoded.
   *
   * @param obj The object to serialize
   * @param objInspector The ObjectInspector to reference to navigate the object
   *          to serialize
   * @return The object serialized into a unicode String
   * @throws SerDeException If the object cannot be serialized
   */
  protected abstract String doSerialize(Object obj,
      ObjectInspector objInspector) throws SerDeException;

  /**
   * Subclasses can override this method to allow for special deserialization.
   * The String argument is always expected to be UTF-8 encoded.
   *
   * @param clob The unicode string to deserialize
   * @return The object which was parsed from the String
   * @throws SerDeException If the object cannot be serialized
   */
  protected abstract Object doDeserialize(String clob) throws SerDeException;

  /**
   * Get the names of the columns defined in this table.
   *
   * @return A list of column names
   */
  public List<String> getColumnNames() {
    return this.columnNames;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return this.objectInspector;
  }

  /**
   * The serialized type is always {@link Text} for any sub-classes.
   */
  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  /**
   * There are no stats for most text-base data, but sub-classes may wish to
   * override this method with their own implementation.
   */
  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

}
