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

package org.apache.hadoop.hive.serde2.json;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.apache.hive.common.util.TimestampParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Preconditions;

/**
 * This class converts JSON strings into Java or Hive Primitive objects.
 *
 * Support types are:<br/>
 * <br/>
 * <table border="1">
 * <tr>
 * <th>JSON Type</th>
 * <th>Java Type</th>
 * <th>Notes</th>
 * </tr>
 * <tr>
 * <td>Object</td>
 * <td>java.util.List</td>
 * <td>Each element may be different type
 * </tr>
 * <tr>
 * <td>Array</td>
 * <td>java.util.List</td>
 * <td>Each element is same type</td>
 * </tr>
 * <tr>
 * <td>Map</td>
 * <td>java.util.Map</td>
 * <td>Keys must be same primitive type; every value is the same type</td>
 * </tr>
 * </table>
 */
public class HiveJsonReader {

  private static final Logger LOG =
      LoggerFactory.getLogger(HiveJsonReader.class);

  private final Map<Pair<StructObjectInspector, String>, StructField> discoveredFields =
      new HashMap<>();

  private final Set<Pair<StructObjectInspector, String>> discoveredUnknownFields =
      new HashSet<>();

  private final EnumSet<Feature> features = EnumSet.noneOf(Feature.class);

  private final ObjectMapper objectMapper;

  private final TimestampParser tsParser;
  private BinaryEncoding binaryEncoding;
  private final ObjectInspector oi;

  /**
   * Enumeration that defines all on/off features for this reader.
   * <ul>
   * <li>{@link #COL_INDEX_PARSING}</li>
   * <li>{@link #PRIMITIVE_TO_WRITABLE}</li>
   * <li>{@link #IGNORE_UKNOWN_FIELDS}</li>
   * </ul>
   */
  public enum Feature {
    /**
     * Enables an optimization to look up each JSON field based on its index in
     * the Hive schema.
     */
    COL_INDEX_PARSING,

    /**
     * If this feature is enabled, when a JSON node is parsed, its value will be
     * returned as a Hadoop Writable object. Otherwise, the Java native value is
     * returned.
     */
    PRIMITIVE_TO_WRITABLE,

    /**
     * If the JSON object being parsed includes a field that is not included in
     * the Hive schema, enabling this feature will cause the JSON reader to
     * produce a log warnings. If this feature is disabled, an Exception will be
     * thrown and parsing will stop.
     */
    IGNORE_UKNOWN_FIELDS
  }

  /**
   * Constructor with default the Hive default timestamp parser.
   *
   * @param oi ObjectInspector for all the fields in the JSON object
   */
  public HiveJsonReader(ObjectInspector oi) {
    this(oi, new TimestampParser());
  }

  /**
   * Constructor with default the Hive default timestamp parser.
   *
   * @param oi ObjectInspector info for all the fields in the JSON object
   * @param tsParser Custom timestamp parser
   */
  public HiveJsonReader(ObjectInspector oi, TimestampParser tsParser) {
    this.binaryEncoding = BinaryEncoding.BASE64;
    this.tsParser = tsParser;
    this.oi = oi;
    this.objectMapper = new ObjectMapper();
  }

  /**
   * Parse text containing a complete JSON object.
   *
   * @param text The text to parse
   * @return A List of Objects, one for each field in the JSON object
   * @throws IOException Unable to parse the JSON text
   * @throws SerDeException The SerDe is not configured correctly
   */
  public Object parseStruct(final String text)
      throws IOException, SerDeException {
    Preconditions.checkNotNull(text);
    Preconditions.checkState(this.oi != null);
    final JsonNode rootNode = this.objectMapper.reader().readTree(text);
    return visitNode(rootNode, this.oi);
  }

  /**
   * Parse text containing a complete JSON object.
   *
   * @param in The InputStream to read the text from
   * @return A List of Objects, one for each field in the JSON object
   * @throws IOException Unable to parse the JSON text
   * @throws SerDeException The SerDe is not configured correctly
   */
  public Object parseStruct(final InputStream in)
      throws IOException, SerDeException {
    Preconditions.checkNotNull(in);
    Preconditions.checkState(this.oi != null);
    final JsonNode rootNode = this.objectMapper.reader().readTree(in);
    return visitNode(rootNode, this.oi);
  }

  /**
   * Visit a node and parse it based on the provided ObjectInspector.
   *
   * @param rootNode The root node to process
   * @param oi The ObjectInspector to use
   * @return The value in this node. Return value may be null, primitive, and
   *         may be a complex type if nested.
   * @throws SerDeException The SerDe is not configured correctly
   */
  private Object visitNode(final JsonNode rootNode, final ObjectInspector oi)
      throws SerDeException {

    if (!rootNode.isNull()) {
      switch (oi.getCategory()) {
      case PRIMITIVE:
        final Object value = visitLeafNode(rootNode, oi);
        return optionallyWrapWritable(value, oi);
      case LIST:
        return visitArrayNode(rootNode, oi);
      case STRUCT:
        return visitStructNode(rootNode, oi);
      case MAP:
        return visitMapNode(rootNode, oi);
      default:
        throw new SerDeException(
            "Parsing of: " + oi.getCategory() + " is not supported");
      }
    }

    return null;
  }

  /**
   * The typical usage of this SerDe requires that it return Hadoop Writable
   * objects. However, some uses of this SerDe want the return values to be Java
   * primitive objects. This SerDe works explicitly in Java primitive objects
   * and will wrap the objects in Writable containers if required.
   *
   * @param value The Java primitive object to wrap
   * @param oi The ObjectInspector provides the type to wrap into
   * @return A Hadoop Writable if required; otherwise the object itself
   */
  private Object optionallyWrapWritable(final Object value,
      final ObjectInspector oi) {
    if (!isEnabled(Feature.PRIMITIVE_TO_WRITABLE)) {
      return value;
    }

    final PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
    final PrimitiveTypeInfo typeInfo = poi.getTypeInfo();

    return PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(typeInfo)
        .getPrimitiveWritableObject(value);
  }

  /**
   * Visit a node if it is expected to be a Map (a.k.a. JSON Object)
   *
   * @param rootNode The node pointing at the JSON object
   * @param oi The ObjectInspector to parse the Map (must be a
   *          MapObjectInspector)
   * @return A Java Map containing the contents of the JSON map
   * @throws SerDeException The SerDe is not configured correctly
   */
  private Map<Object, Object> visitMapNode(final JsonNode rootNode,
      final ObjectInspector oi) throws SerDeException {
    Preconditions.checkArgument(JsonNodeType.OBJECT == rootNode.getNodeType());

    final Map<Object, Object> ret = new LinkedHashMap<>();

    final ObjectInspector mapKeyInspector =
        ((MapObjectInspector) oi).getMapKeyObjectInspector();

    final ObjectInspector mapValueInspector =
        ((MapObjectInspector) oi).getMapValueObjectInspector();

    if (!(mapKeyInspector instanceof PrimitiveObjectInspector)) {
      throw new SerDeException("Map key must be a primitive type");
    }

    final Iterator<Entry<String, JsonNode>> it = rootNode.fields();
    while (it.hasNext()) {
      final Entry<String, JsonNode> field = it.next();
      final Object key =
          visitNode(new TextNode(field.getKey()), mapKeyInspector);
      final Object val = visitNode(field.getValue(), mapValueInspector);
      ret.put(key, val);
    }

    return ret;
  }

  /**
   * Visit a node if it is expected to be a Struct data type (a.k.a. JSON
   * Object)
   *
   * @param rootNode The node pointing at the JSON object
   * @param oi The ObjectInspector to parse the Map (must be a
   *          StructObjectInspector)
   * @return A primitive array of Objects, each element is an element of the
   *         struct
   * @throws SerDeException The SerDe is not configured correctly
   */
  private List<Object> visitStructNode(final JsonNode rootNode,
      final ObjectInspector oi) throws SerDeException {

    Preconditions.checkArgument(JsonNodeType.OBJECT == rootNode.getNodeType());

    final StructObjectInspector structInspector = (StructObjectInspector) oi;

    final int fieldCount = structInspector.getAllStructFieldRefs().size();
    final List<Object> ret = Arrays.asList(new Object[fieldCount]);

    final Iterator<Entry<String, JsonNode>> it = rootNode.fields();
    while (it.hasNext()) {
      final Entry<String, JsonNode> field = it.next();
      final String fieldName = field.getKey();
      final JsonNode childNode = field.getValue();
      final StructField structField =
          getStructField(structInspector, fieldName);

      // If the struct field is null it is because there is a field defined in
      // the JSON object that was not defined in the table definition. Ignore.
      if (structField != null) {
        final Object childValue =
            visitNode(childNode, structField.getFieldObjectInspector());
        ret.set(structField.getFieldID(), childValue);
      }
    }

    return ret;
  }

  /**
   * Visit a node if it is expected to be a JSON Array data type (a.k.a. Hive
   * Array type)
   *
   * @param rootNode The node pointing at the JSON object
   * @param oi The ObjectInspector to parse the List (must be a
   *          ListObjectInspector)
   * @return A Java List of Objects, each element is an element of the array
   * @throws SerDeException The SerDe is not configured correctly
   */
  private List<Object> visitArrayNode(final JsonNode rootNode,
      final ObjectInspector oi) throws SerDeException {
    Preconditions.checkArgument(JsonNodeType.ARRAY == rootNode.getNodeType());

    final ObjectInspector loi =
        ((ListObjectInspector) oi).getListElementObjectInspector();

    final List<Object> ret = new ArrayList<>(rootNode.size());
    final Iterator<JsonNode> it = rootNode.elements();

    while (it.hasNext()) {
      final JsonNode element = it.next();
      ret.add(visitNode(element, loi));
    }

    return ret;
  }

  /**
   * Visit a node if it is expected to be a primitive value (JSON leaf node).
   *
   * @param leafNode The node pointing at the JSON object
   * @param oi The ObjectInspector to parse the value (must be a
   *          PrimitiveObjectInspector)
   * @return A Java primitive Object
   * @throws SerDeException The SerDe is not configured correctly
   */
  private Object visitLeafNode(final JsonNode leafNode,
      final ObjectInspector oi) throws SerDeException {
    Preconditions.checkArgument(leafNode.getNodeType() != JsonNodeType.OBJECT);
    Preconditions.checkArgument(leafNode.getNodeType() != JsonNodeType.ARRAY);

    final PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
    final PrimitiveTypeInfo typeInfo = poi.getTypeInfo();

    switch (typeInfo.getPrimitiveCategory()) {
    case INT:
      return Integer.valueOf(leafNode.asInt());
    case BYTE:
      return Byte.valueOf((byte) leafNode.asInt());
    case SHORT:
      return Short.valueOf((short) leafNode.asInt());
    case LONG:
      return Long.valueOf(leafNode.asLong());
    case BOOLEAN:
      return Boolean.valueOf(leafNode.asBoolean());
    case FLOAT:
      return Float.valueOf((float) leafNode.asDouble());
    case DOUBLE:
      return Double.valueOf(leafNode.asDouble());
    case STRING:
      return leafNode.asText();
    case BINARY:
      return getByteValue(leafNode);
    case DATE:
      return Date.valueOf(leafNode.asText());
    case TIMESTAMP:
      return tsParser.parseTimestamp(leafNode.asText());
    case DECIMAL:
      return HiveDecimal.create(leafNode.asText());
    case TIMESTAMPLOCALTZ:
      final Timestamp ts = tsParser.parseTimestamp(leafNode.asText());
      final ZoneId zid = ((TimestampLocalTZTypeInfo) typeInfo).timeZone();
      final TimestampTZ tstz = new TimestampTZ();
      tstz.set(ts.toEpochSecond(), ts.getNanos(), zid);
      return tstz;
    case VARCHAR:
      return new HiveVarchar(leafNode.asText(),
          ((BaseCharTypeInfo) typeInfo).getLength());
    case CHAR:
      return new HiveChar(leafNode.asText(),
          ((BaseCharTypeInfo) typeInfo).getLength());
    default:
      throw new SerDeException(
          "Could not convert from string to type: " + typeInfo.getTypeName());
    }
  }

  /**
   * A user may configure the encoding for binary data represented as text
   * within a JSON object. This method applies that encoding to the text.
   *
   * @param binaryNode JSON Node containing the binary data
   * @return A byte array with the binary data
   * @throws SerDeException The SerDe is not configured correctly
   */
  private byte[] getByteValue(final JsonNode binaryNode) throws SerDeException {
    try {
      switch (this.binaryEncoding) {
      case RAWSTRING:
        final String byteText = binaryNode.textValue();
        return byteText.getBytes(StandardCharsets.UTF_8);
      case BASE64:
        return binaryNode.binaryValue();
      default:
        throw new SerDeException(
            "No such binary encoding: " + this.binaryEncoding);
      }
    } catch (IOException e) {
      throw new SerDeException("Error generating JSON binary type from record.",
          e);
    }
  }

  /**
   * Matches the JSON object's field name with the Hive data type.
   *
   * @param oi The ObjectInsepctor to lookup the matching in
   * @param fieldName The name of the field parsed from the JSON text
   * @return The meta data of regarding this field
   * @throws SerDeException The SerDe is not configured correctly
   */
  private StructField getStructField(final StructObjectInspector oi,
      final String fieldName) throws SerDeException {

    final Pair<StructObjectInspector, String> pair =
        ImmutablePair.of(oi, fieldName);

    // Ignore the field if it has been ignored before
    if (this.discoveredUnknownFields.contains(pair)) {
      return null;
    }

    // Return from cache if the field has already been discovered
    StructField structField = this.discoveredFields.get(pair);
    if (structField != null) {
      return structField;
    }

    // Otherwise attempt to discover the field
    if (isEnabled(Feature.COL_INDEX_PARSING)) {
      int colIndex = getColIndex(fieldName);
      if (colIndex >= 0) {
        structField = oi.getAllStructFieldRefs().get(colIndex);
      }
    }
    if (structField == null) {
      try {
        structField = oi.getStructFieldRef(fieldName);
      } catch (Exception e) {
        // No such field
      }
    }
    if (structField != null) {
      // cache it for next time
      this.discoveredFields.put(pair, structField);
    } else {
      // Tried everything and did not discover this field
      if (isEnabled(Feature.IGNORE_UKNOWN_FIELDS)
          && this.discoveredUnknownFields.add(pair)) {
        LOG.warn("Discovered unknown field: {}. Ignoring.", fieldName);
      } else {
        throw new SerDeException(
            "Field found in JSON does not match table definition: "
                + fieldName);
      }
    }

    return structField;
  }

  private final Pattern internalPattern = Pattern.compile("^_col([0-9]+)$");

  /**
   * Look up a column based on its index.
   *
   * @param internalName The name of the column
   * @return The index of the field or -1 if the field name does not contain its
   *         index number too
   */
  private int getColIndex(final String internalName) {
    // The above line should have been all the implementation that
    // we need, but due to a bug in that impl which recognizes
    // only single-digit columns, we need another impl here.
    final Matcher m = internalPattern.matcher(internalName);
    return m.matches() ? Integer.parseInt(m.group(1)) : -1;
  }

  public void enable(Feature feature) {
    this.features.add(feature);
  }

  public void disable(Feature feature) {
    this.features.remove(feature);
  }

  public Set<Feature> getFeatures() {
    return Collections.unmodifiableSet(this.features);
  }

  public boolean isEnabled(Feature feature) {
    return this.features.contains(feature);
  }

  public ObjectInspector getObjectInspector() {
    return oi;
  }

  public BinaryEncoding getBinaryEncodingType() {
    return binaryEncoding;
  }

  public void setBinaryEncoding(BinaryEncoding encoding) {
    this.binaryEncoding = encoding;
  }

  @Override
  public String toString() {
    return "HiveJsonReader [features=" + features + ", tsParser=" + tsParser
        + ", binaryEncoding=" + binaryEncoding + "]";
  }

}
