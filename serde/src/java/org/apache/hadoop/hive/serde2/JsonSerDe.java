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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.serde2;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.json.BinaryEncoding;
import org.apache.hadoop.hive.serde2.json.HiveJsonReader;
import org.apache.hadoop.hive.serde2.json.HiveJsonWriter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.TimestampParser;

/**
 * Hive SerDe for processing JSON formatted data. This is typically paired with
 * the TextInputFormat and therefore each line provided to this SerDe must be a
 * single, and complete JSON object.<br>
 * <h2>Example</h2>
 * <p>
 * {"name="john","age"=30}<br>
 * {"name="sue","age"=32}
 * </p>
 */
@SerDeSpec(schemaProps = { serdeConstants.LIST_COLUMNS,
    serdeConstants.LIST_COLUMN_TYPES, serdeConstants.TIMESTAMP_FORMATS,
    JsonSerDe.BINARY_FORMAT, JsonSerDe.IGNORE_EXTRA, JsonSerDe.STRINGIFY_COMPLEX })
public class JsonSerDe extends AbstractSerDe {

  public static final String BINARY_FORMAT = "json.binary.format";
  public static final String STRINGIFY_COMPLEX = "json.stringify.complex.fields";
  public static final String IGNORE_EXTRA = "text.ignore.extra.fields";
  public static final String NULL_EMPTY_LINES = "text.null.empty.line";

  private BinaryEncoding binaryEncoding;
  private boolean nullEmptyLines;

  private HiveJsonReader jsonReader;
  private HiveJsonWriter jsonWriter;
  private StructTypeInfo rowTypeInfo;
  private StructObjectInspector soi;

  /**
   * Initialize the SerDe. By default, items being deserialized are expected to
   * be wrapped in Hadoop Writable objects and objects being serialized are
   * expected to be Java primitive objects.
   *
   * @param configuration Hadoop configuration
   * @param tableProperties Table properties
   * @param partitionProperties Partition properties (may be {@code null} if
   *          table has no partitions)
   * @throws NullPointerException if tableProperties is {@code null}
   * @throws SerDeException if SerDe fails to initialize
   */
  @Override
  public void initialize(Configuration configuration, Properties tableProperties, Properties partitionProperties)
      throws SerDeException {
    initialize(configuration, tableProperties, partitionProperties, true);
  }

  /**
   * Initialize the SerDe.
   *
   * @param configuration Hadoop configuration
   * @param tableProperties Table properties
   * @param partitionProperties Partition properties (may be {@code null} if
   *          table has no partitions)
   * @param writeablePrimitivesDeserialize true if outputs are Hadoop Writable
   * @throws NullPointerException if tableProperties is {@code null}
   * @throws SerDeException if SerDe fails to initialize
   */
  public void initialize(Configuration configuration, Properties tableProperties, Properties partitionProperties,
      boolean writeablePrimitivesDeserialize) throws SerDeException {
    super.initialize(configuration, tableProperties, partitionProperties);
    initialize(configuration, this.properties, writeablePrimitivesDeserialize);
  }

  /**
   * Initialize the SerDe.
   *
   * @param conf System properties; can be null in compile time
   * @param tbl table properties
   * @param writeablePrimitivesDeserialize true if outputs are Hadoop Writable
   */
  private void initialize(final Configuration conf, final Properties tbl,
      final boolean writeablePrimitivesDeserialize) {

    log.debug("Initializing JsonSerDe: {}", tbl.entrySet());

    final String nullEmpty = tbl.getProperty(NULL_EMPTY_LINES, "false");
    this.nullEmptyLines = Boolean.parseBoolean(nullEmpty);

    this.rowTypeInfo = (StructTypeInfo) TypeInfoFactory
        .getStructTypeInfo(getColumnNames(), getColumnTypes());

    this.soi = (StructObjectInspector) TypeInfoUtils
        .getStandardWritableObjectInspectorFromTypeInfo(this.rowTypeInfo);

    final TimestampParser tsParser;
    final String parserFormats =
        tbl.getProperty(serdeConstants.TIMESTAMP_FORMATS);
    if (parserFormats != null) {
      tsParser =
          new TimestampParser(HiveStringUtils.splitAndUnEscape(parserFormats));
    } else {
      tsParser = new TimestampParser();
    }

    final String binaryEncodingStr = tbl.getProperty(BINARY_FORMAT, "base64");
    this.binaryEncoding =
        BinaryEncoding.valueOf(binaryEncodingStr.toUpperCase());

    this.jsonReader = new HiveJsonReader(this.soi, tsParser);
    this.jsonWriter = new HiveJsonWriter(this.binaryEncoding, getColumnNames());

    this.jsonReader.setBinaryEncoding(binaryEncoding);
    this.jsonReader.enable(HiveJsonReader.Feature.COL_INDEX_PARSING);

    if (writeablePrimitivesDeserialize) {
      this.jsonReader.enable(HiveJsonReader.Feature.PRIMITIVE_TO_WRITABLE);
    }

    final String ignoreExtras = tbl.getProperty(IGNORE_EXTRA, "true");
    if (Boolean.parseBoolean(ignoreExtras)) {
      this.jsonReader.enable(HiveJsonReader.Feature.IGNORE_UNKNOWN_FIELDS);
    }

    final String stringifyComplex = tbl.getProperty(STRINGIFY_COMPLEX, "true");
    if (Boolean.parseBoolean(stringifyComplex)) {
      this.jsonReader.enable(HiveJsonReader.Feature.STRINGIFY_COMPLEX_FIELDS);
    }

    log.debug("Initialized SerDe {}", this);
    log.debug("JSON Struct Reader: {}", jsonReader);
    log.debug("JSON Struct Writer: {}", jsonWriter);
  }

  /**
   * Deserialize an object out of a Writable blob containing JSON text. The
   * return value of this function will be constant since the function will
   * reuse the returned object. If the client wants to keep a copy of the
   * object, the client needs to clone the returned value by calling
   * ObjectInspectorUtils.getStandardObject().
   *
   * @param blob The Writable (Text) object containing a serialized object
   * @return A List containing all the values of the row
   */
  @Override
  public Object deserialize(final Writable blob) throws SerDeException {
    final Text t = (Text) blob;

    if (t.getLength() == 0) {
      if (!this.nullEmptyLines) {
        throw new SerDeException("Encountered an empty row in the text file");
      }
      final int fieldCount = soi.getAllStructFieldRefs().size();
      return Collections.nCopies(fieldCount, null);
    }

    try {
      return jsonReader.parseStruct(
          new ByteArrayInputStream((t.getBytes()), 0, t.getLength()));
    } catch (Exception e) {
      log.debug("Problem parsing JSON text [{}]", t, e);
      throw new SerDeException(e);
    }
  }

  /**
   * Given an object and object inspector pair, traverse the object
   * and generate a Text representation of the object.
   */
  @Override
  public Writable serialize(final Object obj,
      final ObjectInspector objInspector) throws SerDeException {

    final String jsonText = this.jsonWriter.write(obj, objInspector);

    return new Text(jsonText);
  }

  /**
   * Returns an object inspector for the specified schema that is capable of
   * reading in the object representation of the JSON string.
   */
  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return jsonReader.getObjectInspector();
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  public StructTypeInfo getTypeInfo() {
    return rowTypeInfo;
  }

  public BinaryEncoding getBinaryEncoding() {
    return binaryEncoding;
  }

  public boolean isNullEmptyLines() {
    return nullEmptyLines;
  }
}
