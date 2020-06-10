/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo.serde;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloMapColumnMapping;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

/**
 * Serializes a Struct to an Accumulo row as per the definition provided by the
 * {@link ColumnMapping}s
 */
public class AccumuloRowSerializer {
  private static final Logger log = LoggerFactory.getLogger(AccumuloRowSerializer.class);

  private final int rowIdOffset;
  private final ByteStream.Output output;
  private final LazySerDeParameters serDeParams;
  private final List<ColumnMapping> mappings;
  private final ColumnVisibility visibility;
  private final AccumuloRowIdFactory rowIdFactory;

  public AccumuloRowSerializer(int primaryKeyOffset, LazySerDeParameters serDeParams,
      List<ColumnMapping> mappings, ColumnVisibility visibility, AccumuloRowIdFactory rowIdFactory) {
    Preconditions.checkArgument(primaryKeyOffset >= 0,
        "A valid offset to the mapping for the Accumulo RowID is required, received "
            + primaryKeyOffset);
    this.rowIdOffset = primaryKeyOffset;
    this.output = new ByteStream.Output();
    this.serDeParams = serDeParams;
    this.mappings = mappings;
    this.visibility = visibility;
    this.rowIdFactory = rowIdFactory;
  }

  public Mutation serialize(Object obj, ObjectInspector objInspector) throws SerDeException,
      IOException {
    if (objInspector.getCategory() != ObjectInspector.Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: " + objInspector.getTypeName());
    }

    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> columnValues = soi.getStructFieldsDataAsList(obj);

    // Fail if we try to access an offset out of bounds
    if (rowIdOffset >= fields.size()) {
      throw new IllegalStateException(
          "Attempted to access field outside of definition for struct. Have " + fields.size()
              + " fields and tried to access offset " + rowIdOffset);
    }

    StructField field = fields.get(rowIdOffset);
    Object value = columnValues.get(rowIdOffset);

    // The ObjectInspector for the row ID
    ObjectInspector fieldObjectInspector = field.getFieldObjectInspector();

    // Serialize the row component using the RowIdFactory. In the normal case, this will just
    // delegate back to the "local" serializeRowId method
    byte[] data = rowIdFactory.serializeRowId(value, field, output);

    // Set that as the row id in the mutation
    Mutation mutation = new Mutation(data);

    // Each column in the row
    for (int i = 0; i < fields.size(); i++) {
      if (rowIdOffset == i) {
        continue;
      }

      // Get the relevant information for this column
      field = fields.get(i);
      value = columnValues.get(i);

      // Despite having a fixed schema from Hive, we have sparse columns in Accumulo
      if (null == value) {
        continue;
      }

      // The ObjectInspector for the current column
      fieldObjectInspector = field.getFieldObjectInspector();

      // Make sure we got the right implementation of a ColumnMapping
      ColumnMapping mapping = mappings.get(i);
      if (mapping instanceof HiveAccumuloColumnMapping) {
        serializeColumnMapping((HiveAccumuloColumnMapping) mapping, fieldObjectInspector, value,
            mutation);
      } else if (mapping instanceof HiveAccumuloMapColumnMapping) {
        serializeColumnMapping((HiveAccumuloMapColumnMapping) mapping, fieldObjectInspector, value,
            mutation);
      } else {
        throw new IllegalArgumentException("Mapping for " + field.getFieldName()
            + " was not a HiveColumnMapping, but was " + mapping.getClass());
      }

    }

    return mutation;
  }

  protected void serializeColumnMapping(HiveAccumuloColumnMapping columnMapping,
      ObjectInspector fieldObjectInspector, Object value, Mutation mutation) throws IOException {
    // Get the serialized value for the column
    byte[] serializedValue = getSerializedValue(fieldObjectInspector, value, output, columnMapping);

    // Put it all in the Mutation
    mutation.put(columnMapping.getColumnFamilyBytes(), columnMapping.getColumnQualifierBytes(),
        visibility, serializedValue);
  }

  /**
   * Serialize the Hive Map into an Accumulo row
   */
  protected void serializeColumnMapping(HiveAccumuloMapColumnMapping columnMapping,
      ObjectInspector fieldObjectInspector, Object value, Mutation mutation) throws IOException {
    MapObjectInspector mapObjectInspector = (MapObjectInspector) fieldObjectInspector;

    Map<?,?> map = mapObjectInspector.getMap(value);
    if (map == null) {
      return;
    }

    ObjectInspector keyObjectInspector = mapObjectInspector.getMapKeyObjectInspector(), valueObjectInspector = mapObjectInspector
        .getMapValueObjectInspector();

    byte[] cfBytes = columnMapping.getColumnFamily().getBytes(Charsets.UTF_8), cqPrefixBytes = columnMapping
        .getColumnQualifierPrefix().getBytes(Charsets.UTF_8);
    byte[] cqBytes, valueBytes;
    for (Entry<?,?> entry : map.entrySet()) {
      output.reset();

      // If the cq prefix is non-empty, add it to the CQ before we set the mutation
      if (0 < cqPrefixBytes.length) {
        output.write(cqPrefixBytes, 0, cqPrefixBytes.length);
      }

      // Write the "suffix" of the cq
      writeWithLevel(keyObjectInspector, entry.getKey(), output, columnMapping, 3);
      cqBytes = output.toByteArray();

      output.reset();

      // Write the value
      writeWithLevel(valueObjectInspector, entry.getValue(), output, columnMapping, 3);
      valueBytes = output.toByteArray();

      mutation.put(cfBytes, cqBytes, visibility, valueBytes);
    }
  }

  /**
   * Serialize an Accumulo rowid
   */
  protected byte[] serializeRowId(Object rowId, StructField rowIdField, ColumnMapping rowIdMapping)
      throws IOException {
    if (rowId == null) {
      throw new IOException("Accumulo rowId cannot be NULL");
    }
    // Reset the buffer we're going to use
    output.reset();
    ObjectInspector rowIdFieldOI = rowIdField.getFieldObjectInspector();
    String rowIdMappingType = rowIdMapping.getColumnType();
    TypeInfo rowIdTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(rowIdMappingType);

    if (!rowIdFieldOI.getCategory().equals(ObjectInspector.Category.PRIMITIVE)
        && rowIdTypeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE) {
      // we always serialize the String type using the escaped algorithm for LazyString
      writeString(output, SerDeUtils.getJSONString(rowId, rowIdFieldOI),
          PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      return output.toByteArray();
    }

    // use the serialization option switch to write primitive values as either a variable
    // length UTF8 string or a fixed width bytes if serializing in binary format
    getSerializedValue(rowIdFieldOI, rowId, output, rowIdMapping);
    return output.toByteArray();
  }

  /**
   * Compute the serialized value from the given element and object inspectors. Based on the Hive
   * types, represented through the ObjectInspectors for the whole object and column within the
   * object, serialize the object appropriately.
   *
   * @param fieldObjectInspector
   *          ObjectInspector for the column value being serialized
   * @param value
   *          The Object itself being serialized
   * @param output
   *          A temporary buffer to reduce object creation
   * @return The serialized bytes from the provided value.
   * @throws IOException
   *           An error occurred when performing IO to serialize the data
   */
  protected byte[] getSerializedValue(ObjectInspector fieldObjectInspector, Object value,
      ByteStream.Output output, ColumnMapping mapping) throws IOException {
    // Reset the buffer we're going to use
    output.reset();

    // Start by only serializing primitives as-is
    if (fieldObjectInspector.getCategory() == ObjectInspector.Category.PRIMITIVE) {
      writeSerializedPrimitive((PrimitiveObjectInspector) fieldObjectInspector, output, value,
          mapping.getEncoding());
    } else {
      // We only accept a struct, which means that we're already nested one level deep
      writeWithLevel(fieldObjectInspector, value, output, mapping, 2);
    }

    return output.toByteArray();
  }

  /**
   * Recursively serialize an Object using its {@link ObjectInspector}, respecting the
   * separators defined by the {@link LazySerDeParameters}.
   * @param oi ObjectInspector for the current object
   * @param value The current object
   * @param output A buffer output is written to
   * @param mapping The mapping for this Hive column
   * @param level The current level/offset for the SerDe separator
   * @throws IOException
   */
  protected void writeWithLevel(ObjectInspector oi, Object value, ByteStream.Output output,
      ColumnMapping mapping, int level) throws IOException {
    switch (oi.getCategory()) {
      case PRIMITIVE:
        if (mapping.getEncoding() == ColumnEncoding.BINARY) {
          this.writeBinary(output, value, (PrimitiveObjectInspector) oi);
        } else {
          this.writeString(output, value, (PrimitiveObjectInspector) oi);
        }
        return;
      case LIST:
        char separator = (char) serDeParams.getSeparators()[level];
        ListObjectInspector loi = (ListObjectInspector) oi;
        List<?> list = loi.getList(value);
        ObjectInspector eoi = loi.getListElementObjectInspector();
        if (list == null) {
          log.debug("No objects found when serializing list");
          return;
        } else {
          for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
              output.write(separator);
            }
            writeWithLevel(eoi, list.get(i), output, mapping, level + 1);
          }
        }
        return;
      case MAP:
        char sep = (char) serDeParams.getSeparators()[level];
        char keyValueSeparator = (char) serDeParams.getSeparators()[level + 1];
        MapObjectInspector moi = (MapObjectInspector) oi;
        ObjectInspector koi = moi.getMapKeyObjectInspector();
        ObjectInspector voi = moi.getMapValueObjectInspector();

        Map<?,?> map = moi.getMap(value);
        if (map == null) {
          log.debug("No object found when serializing map");
          return;
        } else {
          boolean first = true;
          for (Map.Entry<?,?> entry : map.entrySet()) {
            if (first) {
              first = false;
            } else {
              output.write(sep);
            }
            writeWithLevel(koi, entry.getKey(), output, mapping, level + 2);
            output.write(keyValueSeparator);
            writeWithLevel(voi, entry.getValue(), output, mapping, level + 2);
          }
        }
        return;
      case STRUCT:
        sep = (char) serDeParams.getSeparators()[level];
        StructObjectInspector soi = (StructObjectInspector) oi;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        list = soi.getStructFieldsDataAsList(value);
        if (list == null) {
          log.debug("No object found when serializing struct");
          return;
        } else {
          for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
              output.write(sep);
            }

            writeWithLevel(fields.get(i).getFieldObjectInspector(), list.get(i), output, mapping,
                level + 1);
          }
        }

        return;
      default:
        throw new RuntimeException("Unknown category type: " + oi.getCategory());
    }
  }

  /**
   * Serialize the given primitive to the given output buffer, using the provided encoding
   * mechanism.
   *
   * @param objectInspector
   *          The PrimitiveObjectInspector for this Object
   * @param output
   *          A buffer to write the serialized value to
   * @param value
   *          The Object being serialized
   * @param encoding
   *          The means in which the Object should be serialized
   * @throws IOException
   */
  protected void writeSerializedPrimitive(PrimitiveObjectInspector objectInspector,
      ByteStream.Output output, Object value, ColumnEncoding encoding) throws IOException {
    // Despite STRING being a primitive, it can't be serialized as binary
    if (objectInspector.getPrimitiveCategory() != PrimitiveCategory.STRING && ColumnEncoding.BINARY == encoding) {
      writeBinary(output, value, objectInspector);
    } else {
      writeString(output, value, objectInspector);
    }
  }

  protected void writeBinary(ByteStream.Output output, Object value,
      PrimitiveObjectInspector inspector) throws IOException {
    LazyUtils.writePrimitive(output, value, inspector);
  }

  protected void writeString(ByteStream.Output output, Object value,
      PrimitiveObjectInspector inspector) throws IOException {
    LazyUtils.writePrimitiveUTF8(output, value, inspector, serDeParams.isEscaped(),
        serDeParams.getEscapeChar(), serDeParams.getNeedsEscape());
  }

  protected ColumnVisibility getVisibility() {
    return visibility;
  }
}
