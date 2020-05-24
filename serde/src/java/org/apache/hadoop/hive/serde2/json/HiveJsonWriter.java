/**
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

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;

/**
 * A class which takes Java Objects and a ObjectInspector to produce a JSON
 * string representation of the structure. The Java Object can be a Collection
 * containing other Collections and therefore can be thought of as a tree or as
 * a directed acyclic graph that must be walked and each node recorded as JSON.
 */
public class HiveJsonWriter {

  private static final Logger LOG =
      LoggerFactory.getLogger(HiveJsonWriter.class);

  private final EnumSet<Feature> features = EnumSet.noneOf(Feature.class);

  private final List<String> rootStructFieldNames;

  private final JsonNodeFactory nodeFactory;
  private final ObjectMapper mapper;
  private BinaryEncoding binaryEncoding;

  /**
   * Enumeration that defines all on/off features for this writer.
   */
  public enum Feature {
    PRETTY_PRINT
  }

  /**
   * Default constructor. Uses base-64 mime encoding for any binary data.
   */
  public HiveJsonWriter() {
    this(BinaryEncoding.BASE64, Collections.emptyList());
  }

  /**
   * Constructs a JSON writer.
   *
   * @param encoding The encoding to use for binary data
   * @param rootStructFieldNames The names of the fields at the root level
   */
  public HiveJsonWriter(final BinaryEncoding encoding,
      final List<String> rootStructFieldNames) {
    this.binaryEncoding = encoding;
    this.rootStructFieldNames = rootStructFieldNames;
    this.nodeFactory = JsonNodeFactory.instance;
    this.mapper = new ObjectMapper();
  }

  /**
   * Given an Object and an ObjectInspector, convert the structure into a JSON
   * text string.
   *
   * @param o The object to convert
   * @param objInspector The ObjectInspector describing the object
   * @return A String containing the JSON text
   * @throws SerDeException The object cannot be transformed
   */
  public String write(final Object o, final ObjectInspector objInspector)
      throws SerDeException {

    final JsonNode rootNode =
        walkObjectGraph(objInspector, o, rootStructFieldNames);

    LOG.debug("Create JSON tree from Object tree: {}", rootNode);

    try {
      final ObjectWriter ow = isEnabled(Feature.PRETTY_PRINT)
          ? this.mapper.writerWithDefaultPrettyPrinter()
          : this.mapper.writer();

      return ow.writeValueAsString(rootNode);
    } catch (JsonProcessingException e) {
      throw new SerDeException(e);
    }
  }

  /**
   * Walk the object graph.
   *
   * @param oi The ObjectInspector describing the Object
   * @param o The object to convert
   * @param fieldNames List of field names to use, default names used otherwise
   * @return A JsonNode representation of the Object
   * @throws SerDeException The Object cannot be parsed
   */
  private JsonNode walkObjectGraph(final ObjectInspector oi, final Object o,
      final List<String> fieldNames)
      throws SerDeException {

    if (o != null) {
      switch (oi.getCategory()) {
      case LIST:
        return visitList(oi, o);
      case STRUCT:
        return visitStruct(oi, o, fieldNames);
      case MAP:
        return visitMap(oi, o);
      case PRIMITIVE:
        return primitiveToNode(oi, o);
      case UNION:
        return visitUnion(oi, o);
      default:
        throw new SerDeException(
            "Parsing of: " + oi.getCategory() + " is not supported");
      }
    }
    return this.nodeFactory.nullNode();
  }

  /**
   * Visit a vertex in the graph that is a Java Map.
   *
   * @param oi The map's OjectInspector
   * @param o The Map object
   * @return A JsonNode representation of the Map
   * @throws SerDeException The Map cannot be parsed
   */
  private ObjectNode visitMap(final ObjectInspector oi, final Object o)
      throws SerDeException {
    final MapObjectInspector moi = (MapObjectInspector) oi;

    final ObjectInspector mapKeyInspector = moi.getMapKeyObjectInspector();
    final ObjectInspector mapValueInspector = moi.getMapValueObjectInspector();

    final Map<?, ?> map = moi.getMap(o);
    final ObjectNode objectNode = this.nodeFactory.objectNode();

    for (Map.Entry<?, ?> entry : map.entrySet()) {
      final JsonNode keyNode = primitiveToNode(mapKeyInspector, entry.getKey());
      final JsonNode valueNode = walkObjectGraph(mapValueInspector,
          entry.getValue(), Collections.emptyList());
      objectNode.set(keyNode.asText(), valueNode);
    }

    return objectNode;
  }

  /**
   * Visit a vertex in the graph that is a Java List.
   *
   * @param oi The list's OjectInspector
   * @param o The List object
   * @return A JsonNode representation of the List
   * @throws SerDeException The List cannot be parsed
   */
  private ContainerNode<?> visitList(final ObjectInspector oi, final Object o)
      throws SerDeException {
    final ListObjectInspector loi = (ListObjectInspector) oi;

    final List<?> list = loi.getList(o);
    final ArrayNode arrayNode = this.nodeFactory.arrayNode(list.size());

    for (final Object item : list) {
      final JsonNode valueNode = walkObjectGraph(
          loi.getListElementObjectInspector(), item, Collections.emptyList());
      arrayNode.add(valueNode);
    }

    return arrayNode;
  }

  /**
   * Visit a vertex in the graph that is a struct data type. A struct is
   * represented as a Java List where the name associated with each element in
   * the list is stored in the ObjectInspector.
   *
   * @param oi The struct's OjectInspector
   * @param o The List object
   * @param fieldNames List of names to override the default field names
   * @return A JsonNode representation of the List
   * @throws SerDeException The struct cannot be parsed
   */
  private ObjectNode visitStruct(final ObjectInspector oi, final Object o,
      final List<String> fieldNames) throws SerDeException {
    final StructObjectInspector structInspector = (StructObjectInspector) oi;

    final ObjectNode structNode = this.nodeFactory.objectNode();

    for (final StructField field : structInspector.getAllStructFieldRefs()) {
      final Object fieldValue = structInspector.getStructFieldData(o, field);
      final ObjectInspector coi = field.getFieldObjectInspector();
      final JsonNode fieldNode =
          walkObjectGraph(coi, fieldValue, Collections.emptyList());

      // Map field names to something else if require
      final String fieldName = (fieldNames.isEmpty()) ? field.getFieldName()
          : fieldNames.get(field.getFieldID());

      structNode.set(fieldName, fieldNode);
    }

    return structNode;
  }

  /**
   * Visit a vertex in the graph that is a union data type.
   *
   * @param oi The union's OjectInspector
   * @param o The Union object
   * @return A JsonNode representation of the union
   * @throws SerDeException The union cannot be parsed
   */
  private ObjectNode visitUnion(final ObjectInspector oi, final Object o)
      throws SerDeException {
    final UnionObjectInspector unionInspector = (UnionObjectInspector) oi;

    final ObjectNode unionNode = this.nodeFactory.objectNode();

    final byte tag = unionInspector.getTag(o);
    final String tagText = Byte.toString(tag);
    final JsonNode valueNode =
        walkObjectGraph(unionInspector.getObjectInspectors().get(tag),
            unionInspector.getField(o), Collections.emptyList());

    unionNode.set(tagText, valueNode);

    return unionNode;
  }

  /**
   * Convert a primitive Java object to a JsonNode.
   *
   * @param oi The primitive ObjectInspector
   * @param o The primitive Object
   * @return A JSON node containing the value of the primitive Object
   * @throws SerDeException The primitive value cannot be parsed
   */
  private ValueNode primitiveToNode(final ObjectInspector oi, final Object o)
      throws SerDeException {
    final PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;

    switch (poi.getPrimitiveCategory()) {
    case BINARY:
      return getByteValue(
          ((BinaryObjectInspector) poi).getPrimitiveJavaObject(o));
    case BOOLEAN:
      return this.nodeFactory.booleanNode(((BooleanObjectInspector) poi).get(o));
    case BYTE:
      return this.nodeFactory.numberNode(((ByteObjectInspector) poi).get(o));
    case DATE:
      return this.nodeFactory.textNode(
          ((DateObjectInspector) poi).getPrimitiveJavaObject(o).toString());
    case DECIMAL:
      return this.nodeFactory.numberNode(((HiveDecimalObjectInspector) poi)
          .getPrimitiveJavaObject(o).bigDecimalValue());
    case DOUBLE:
      return this.nodeFactory.numberNode(((DoubleObjectInspector) poi).get(o));
    case FLOAT:
      return this.nodeFactory.numberNode(((FloatObjectInspector) poi).get(o));
    case INT:
      return this.nodeFactory.numberNode(((IntObjectInspector) poi).get(o));
    case LONG:
      return this.nodeFactory.numberNode(((LongObjectInspector) poi).get(o));
    case SHORT:
      return this.nodeFactory.numberNode(((ShortObjectInspector) poi).get(o));
    case STRING:
      return this.nodeFactory
          .textNode(((StringObjectInspector) poi).getPrimitiveJavaObject(o));
    case CHAR:
      return this.nodeFactory.textNode(
          ((HiveCharObjectInspector) poi).getPrimitiveJavaObject(o).toString());
    case VARCHAR:
      return this.nodeFactory.textNode(((HiveVarcharObjectInspector) poi)
          .getPrimitiveJavaObject(o).toString());
    case TIMESTAMP:
      return this.nodeFactory.textNode(((TimestampObjectInspector) poi)
          .getPrimitiveJavaObject(o).toString());
    default:
      throw new SerDeException(
          "Unsupported type: " + poi.getPrimitiveCategory());
    }
  }

  /**
   * Convert a byte array into a text representation.
   *
   * @param o The byte array to convert to text
   * @return A JsonNode with the binary data encoded
   * @throws SerDeException The struct cannot be parsed
   */
  private ValueNode getByteValue(final byte[] buf) throws SerDeException {
    switch (this.binaryEncoding) {
    case RAWSTRING:
      final Text txt = new Text();
      txt.set(buf, 0, buf.length);
      return this.nodeFactory.textNode(txt.toString());
    case BASE64:
      return this.nodeFactory.binaryNode(buf);
    default:
      throw new SerDeException(
          "Error generating JSON binary type from record.");
    }
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

  public BinaryEncoding getBinaryEncodingType() {
    return binaryEncoding;
  }

  public void setBinaryEncoding(BinaryEncoding encoding) {
    this.binaryEncoding = encoding;
  }

  @Override
  public String toString() {
    return "HiveJsonWriter [features=" + features + ", rootStructFieldNames="
        + rootStructFieldNames + ", binaryEncoding=" + binaryEncoding + "]";
  }

}
