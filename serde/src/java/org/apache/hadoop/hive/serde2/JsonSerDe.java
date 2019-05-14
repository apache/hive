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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.json.BinaryEncoding;
import org.apache.hadoop.hive.serde2.json.HiveJsonReader;
import org.apache.hadoop.hive.serde2.json.HiveJsonWriter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.TimestampParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hive SerDe for processing JSON formatted data. This is typically paired with
 * the TextInputFormat and therefore each line provided to this SerDe must be a
 * single, and complete JSON object.<br/>
 * <h2>Example</h2>
 * <p>
 * {"name="john","age"=30}<br/>
 * {"name="sue","age"=32}
 * </p>
 */
@SerDeSpec(schemaProps = { serdeConstants.LIST_COLUMNS,
    serdeConstants.LIST_COLUMN_TYPES, serdeConstants.TIMESTAMP_FORMATS,
    JsonSerDe.BINARY_FORMAT, JsonSerDe.IGNORE_EXTRA })
public class JsonSerDe extends AbstractSerDe {

  private static final Logger LOG = LoggerFactory.getLogger(JsonSerDe.class);

  public static final String BINARY_FORMAT = "json.binary.format";
  public static final String IGNORE_EXTRA = "text.ignore.extra.fields";
  public static final String NULL_EMPTY_LINES = "text.null.empty.line";

  private List<String> columnNames;

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
   */
  @Override
  public void initialize(final Configuration conf, final Properties tbl)
      throws SerDeException {
    initialize(conf, tbl, true);
  }

  /**
   * Initialize the SerDe.
   *
   * @param conf System properties; can be null in compile time
   * @param tbl table properties
   * @param writeablePrimitivesDeserialize true if outputs are Hadoop Writable
   */
  public void initialize(final Configuration conf, final Properties tbl,
      final boolean writeablePrimitivesDeserialize) {

    LOG.debug("Initializing JsonSerDe: {}", tbl.entrySet());

    // Get column names
    final String columnNameProperty =
        tbl.getProperty(serdeConstants.LIST_COLUMNS);
    final String columnNameDelimiter = tbl.getProperty(
        serdeConstants.COLUMN_NAME_DELIMITER, String.valueOf(SerDeUtils.COMMA));

    this.columnNames = columnNameProperty.isEmpty() ? Collections.emptyList()
        : Arrays.asList(columnNameProperty.split(columnNameDelimiter));

    // all column types
    final String columnTypeProperty =
        tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

    final List<TypeInfo> columnTypes =
        columnTypeProperty.isEmpty() ? Collections.emptyList()
            : TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

    LOG.debug("columns: {}, {}", columnNameProperty, columnNames);
    LOG.debug("types: {}, {} ", columnTypeProperty, columnTypes);

    assert (columnNames.size() == columnTypes.size());

    final String nullEmpty = tbl.getProperty(NULL_EMPTY_LINES, "false");
    this.nullEmptyLines = Boolean.parseBoolean(nullEmpty);

    this.rowTypeInfo = (StructTypeInfo) TypeInfoFactory
        .getStructTypeInfo(columnNames, columnTypes);

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
    this.jsonWriter = new HiveJsonWriter(this.binaryEncoding, columnNames);

    this.jsonReader.setBinaryEncoding(binaryEncoding);
    this.jsonReader.enable(HiveJsonReader.Feature.COL_INDEX_PARSING);

    if (writeablePrimitivesDeserialize) {
      this.jsonReader.enable(HiveJsonReader.Feature.PRIMITIVE_TO_WRITABLE);
    }

    final String ignoreExtras = tbl.getProperty(IGNORE_EXTRA, "true");
    if (Boolean.parseBoolean(ignoreExtras)) {
      this.jsonReader.enable(HiveJsonReader.Feature.IGNORE_UKNOWN_FIELDS);
    }

    LOG.debug("JSON Struct Reader: {}", jsonReader);
    LOG.debug("JSON Struct Writer: {}", jsonWriter);
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
      LOG.debug("Problem parsing JSON text [{}].", t, e);
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

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics yet
    return null;
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
