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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

/**
 * A base class for any Hive serializer or deserializer (SerDe) which supports a
 * text-based format that stores tabular data as a unicode string and separates
 * values in each row with one or more delimiter characters.
 */
public abstract class AbstractDelimitedTextSerDe
    extends AbstractEncodingAwareSerDe {

  public static final String STRICT_FIELDS_COUNT = "text.strict.fields.count";

  private String delim;
  private Splitter stringSplitter;
  private Joiner stringJoiner;

  private final EnumSet<Feature> features = EnumSet.noneOf(Feature.class);

  /**
   * Enumeration that defines all on/off features for this SerDe.
   * <ul>
   * <li>{@link #CHECK_FIELD_COUNT}</li>
   * </ul>
   */
  public enum Feature {
    /**
     * If this feature is enabled, and more fields are parsed from a row than is
     * defined in the schema, an exception is raised. Otherwise, the data is
     * silently ignored,
     */
    CHECK_FIELD_COUNT
  }

  @Override
  public void initialize(final Configuration configuration,
      final Properties tableProperties) throws SerDeException {
    Objects.requireNonNull(this.delim,
        "Delimiter must be set before initializing");

    LOG.debug("Table delimiter: {}", this.delim);

    super.initialize(configuration, tableProperties);

    this.stringSplitter =
        Splitter.on(delim).trimResults().limit(getColumnNames().size());
    this.stringJoiner = Joiner.on(delim);

    if (Boolean
        .valueOf(tableProperties.getProperty(STRICT_FIELDS_COUNT, "false"))) {
      features.add(Feature.CHECK_FIELD_COUNT);
    }
  }

  /**
   * Serialize a string into a list of one or more fields. The fields are
   * separated by an arbitrary character sequence.
   *
   * @param clob The unicode string to deserialize
   * @return The object which was parsed from the String
   * @throws SerDeException If the object cannot be serialized
   */
  @Override
  protected List<String> doDeserialize(final String clob)
      throws SerDeException {
    Objects.requireNonNull(this.stringSplitter,
        "Deserializer is not initialized");

    LOG.trace("Splitting row on delimiter [{}]:[{}]", this.delim, clob);

    final List<String> fields = new ArrayList<>(getColumnNames().size());

    if (!clob.isEmpty()) {
      Iterables.addAll(fields, this.stringSplitter.split(clob));
    }

    final int fieldCountDelta = getColumnNames().size() - fields.size();

    if (this.features.contains(Feature.CHECK_FIELD_COUNT)) {
      if (fieldCountDelta > 0) {
        throw new SerDeException(
            "Number of fields parsed from text data is less than number of "
                + "columns defined in the table schema");
      }
      if (Iterables.getLast(fields).contains(this.delim)) {
        throw new SerDeException(
            "Number of fields parsed from text data is more than number of "
                + "columns defined in the table schema");
      }
    }

    if (fieldCountDelta > 0) {
      fields.addAll(Collections.nCopies(fieldCountDelta, null));
    }

    return Collections.unmodifiableList(fields);
  }

  /**
   * Given a row of data, generate a UTF-8 encoded String which contains all of
   * the fields in the row separated by a delimiter.
   *
   * @param obj The object to serialize
   * @param objInspector The ObjectInspector to reference to navigate the object
   *          to serialize
   * @return The object serialized into a unicode String
   * @throws SerDeException If the object cannot be serialized
   */
  @Override
  protected String doSerialize(final Object obj,
      final ObjectInspector objInspector) throws SerDeException {
    Objects.requireNonNull(this.stringJoiner, "Serializer is not initialized");

    final StructObjectInspector outputRowOI =
        (StructObjectInspector) objInspector;

    final List<? extends StructField> outputFieldRefs =
        outputRowOI.getAllStructFieldRefs();

    if (outputFieldRefs.size() != getColumnNames().size()) {
      throw new SerDeException("Cannot serialize the object because there are "
          + outputFieldRefs.size() + " fields but the table has "
          + getColumnNames().size() + " columns.");
    }

    final List<String> row = new ArrayList<>(outputFieldRefs.size());

    for (int c = 0; c < outputFieldRefs.size(); c++) {
      final Object field =
          outputRowOI.getStructFieldData(obj, outputFieldRefs.get(c));
      final ObjectInspector fieldOI =
          outputFieldRefs.get(c).getFieldObjectInspector();

      // The data must be of type String
      final StringObjectInspector fieldStringOI =
          (StringObjectInspector) fieldOI;

      // Convert the field to Java class String, because objects of String type
      // can be stored in String, Text, or some other classes.
      row.add(fieldStringOI.getPrimitiveJavaObject(field));
    }

    return this.stringJoiner.join(row);
  }

  /**
   * Set the delimiter to be used to separate the fields in each row. The
   * delimiter must be set before calling the
   * {@link #initialize(Configuration, Properties)} method.
   *
   * @param delim Delimiter used to separate each field
   */
  public void setDelim(final String delim) {
    this.delim = delim;
  }

  /**
   * Get the currently configured delimiter used to separate fields in each row.
   *
   * @return The currently configured delimiter
   */
  public String getDelim() {
    return delim;
  }

}
