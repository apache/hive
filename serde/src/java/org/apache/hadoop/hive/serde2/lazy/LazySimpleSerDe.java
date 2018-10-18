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

package org.apache.hadoop.hive.serde2.lazy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.Public;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Stable;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractEncodingAwareSerDe;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyObjectInspectorParametersImpl;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * LazySimpleSerDe can be used to read the same data format as
 * MetadataTypedColumnsetSerDe and TCTLSeparatedProtocol.
 *
 * However, LazySimpleSerDe creates Objects in a lazy way, to provide better
 * performance.
 *
 * Also LazySimpleSerDe outputs typed columns instead of treating all columns as
 * String like MetadataTypedColumnsetSerDe.
 */
@Public
@Stable
@SerDeSpec(schemaProps = {
    serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES,
    serdeConstants.FIELD_DELIM, serdeConstants.COLLECTION_DELIM, serdeConstants.MAPKEY_DELIM,
    serdeConstants.SERIALIZATION_FORMAT, serdeConstants.SERIALIZATION_NULL_FORMAT,
    serdeConstants.SERIALIZATION_ESCAPE_CRLF,
    serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST,
    serdeConstants.ESCAPE_CHAR,
    serdeConstants.SERIALIZATION_ENCODING,
    LazySerDeParameters.SERIALIZATION_EXTEND_NESTING_LEVELS,
    LazySerDeParameters.SERIALIZATION_EXTEND_ADDITIONAL_NESTING_LEVELS
    })
public class LazySimpleSerDe extends AbstractEncodingAwareSerDe {

  private LazySerDeParameters serdeParams = null;

  private ObjectInspector cachedObjectInspector;

  private long serializedSize;
  private SerDeStats stats;
  private boolean lastOperationSerialize;
  private boolean lastOperationDeserialize;

  @Override
  public String toString() {
    return getClass().toString()
        + "["
        + Arrays.asList(serdeParams.getSeparators())
        + ":"
        + ((StructTypeInfo) serdeParams.getRowTypeInfo()).getAllStructFieldNames()
        + ":"
        + ((StructTypeInfo) serdeParams.getRowTypeInfo())
            .getAllStructFieldTypeInfos() + "]";
  }

  public LazySimpleSerDe() throws SerDeException {
  }

  /**
   * Initialize the SerDe given the parameters. serialization.format: separator
   * char or byte code (only supports byte-value up to 127) columns:
   * ","-separated column names columns.types: ",", ":", or ";"-separated column
   * types
   *
   * @see  org.apache.hadoop.hive.serde2.AbstractSerDe#initialize(Configuration, Properties)
   */
  @Override
  public void initialize(Configuration job, Properties tbl)
      throws SerDeException {

    super.initialize(job, tbl);

    serdeParams = new LazySerDeParameters(job, tbl, getClass().getName());

    // Create the ObjectInspectors for the fields
    cachedObjectInspector = LazyFactory.createLazyStructInspector(serdeParams
        .getColumnNames(), serdeParams.getColumnTypes(),
        new LazyObjectInspectorParametersImpl(serdeParams));

    cachedLazyStruct = (LazyStruct) LazyFactory
        .createLazyObject(cachedObjectInspector);

    serializedSize = 0;
    stats = new SerDeStats();
    lastOperationSerialize = false;
    lastOperationDeserialize = false;
  }

  // The object for storing row data
  LazyStruct cachedLazyStruct;

  // The wrapper for byte array
  ByteArrayRef byteArrayRef;

  /**
   * Deserialize a row from the Writable to a LazyObject.
   *
   * @param field
   *          the Writable that contains the data
   * @return The deserialized row Object.
   * @see  org.apache.hadoop.hive.serde2.AbstractSerDe#deserialize(Writable)
   */
  @Override
  public Object doDeserialize(Writable field) throws SerDeException {
    if (byteArrayRef == null) {
      byteArrayRef = new ByteArrayRef();
    }
    BinaryComparable b = (BinaryComparable) field;
    byteArrayRef.setData(b.getBytes());
    cachedLazyStruct.init(byteArrayRef, 0, b.getLength());
    lastOperationSerialize = false;
    lastOperationDeserialize = true;
    return cachedLazyStruct;
  }

  /**
   * Returns the ObjectInspector for the row.
   */
  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  /**
   * Returns the Writable Class after serialization.
   *
   * @see org.apache.hadoop.hive.serde2.AbstractSerDe#getSerializedClass()
   */
  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  Text serializeCache = new Text();
  ByteStream.Output serializeStream = new ByteStream.Output();

  /**
   * Serialize a row of data.
   *
   * @param obj
   *          The row object
   * @param objInspector
   *          The ObjectInspector for the row object
   * @return The serialized Writable object
   * @throws SerDeException
   * @see org.apache.hadoop.hive.serde2.AbstractSerDe#serialize(Object, ObjectInspector)
   */
  @Override
  public Writable doSerialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {

    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: "
          + objInspector.getTypeName());
    }

    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> list = soi.getStructFieldsDataAsList(obj);
    List<? extends StructField> declaredFields = (serdeParams.getRowTypeInfo() != null && ((StructTypeInfo) serdeParams.getRowTypeInfo())
        .getAllStructFieldNames().size() > 0) ? ((StructObjectInspector) getObjectInspector())
        .getAllStructFieldRefs()
        : null;

    serializeStream.reset();
    serializedSize = 0;

    // Serialize each field
    for (int i = 0; i < fields.size(); i++) {
      // Append the separator if needed.
      if (i > 0) {
        serializeStream.write(serdeParams.getSeparators()[0]);
      }
      // Get the field objectInspector and the field object.
      ObjectInspector foi = fields.get(i).getFieldObjectInspector();
      Object f = (list == null ? null : list.get(i));

      if (declaredFields != null && i >= declaredFields.size()) {
        throw new SerDeException("Error: expecting " + declaredFields.size()
            + " but asking for field " + i + "\n" + "data=" + obj + "\n"
            + "tableType=" + serdeParams.getRowTypeInfo().toString() + "\n"
            + "dataType="
            + TypeInfoUtils.getTypeInfoFromObjectInspector(objInspector));
      }

      serializeField(serializeStream, f, foi, serdeParams);
    }

    // TODO: The copy of data is unnecessary, but there is no work-around
    // since we cannot directly set the private byte[] field inside Text.
    serializeCache
        .set(serializeStream.getData(), 0, serializeStream.getLength());
    serializedSize = serializeStream.getLength();
    lastOperationSerialize = true;
    lastOperationDeserialize = false;
    return serializeCache;
  }

  protected void serializeField(ByteStream.Output out, Object obj, ObjectInspector objInspector,
      LazySerDeParameters serdeParams) throws SerDeException {
    try {
      serialize(out, obj, objInspector, serdeParams.getSeparators(), 1, serdeParams.getNullSequence(),
          serdeParams.isEscaped(), serdeParams.getEscapeChar(), serdeParams.getNeedsEscape());
    } catch (IOException e) {
      throw new SerDeException(e);
    }
  }

  /**
   * Serialize the row into the StringBuilder.
   *
   * @param out
   *          The StringBuilder to store the serialized data.
   * @param obj
   *          The object for the current field.
   * @param objInspector
   *          The ObjectInspector for the current Object.
   * @param separators
   *          The separators array.
   * @param level
   *          The current level of separator.
   * @param nullSequence
   *          The byte sequence representing the NULL value.
   * @param escaped
   *          Whether we need to escape the data when writing out
   * @param escapeChar
   *          Which char to use as the escape char, e.g. '\\'
   * @param needsEscape
   *          Which byte needs to be escaped for 256 bytes. 
   * @throws IOException
   * @throws SerDeException
   */
  public static void serialize(ByteStream.Output out, Object obj,
      ObjectInspector objInspector, byte[] separators, int level,
      Text nullSequence, boolean escaped, byte escapeChar, boolean[] needsEscape)
      throws IOException, SerDeException {

    if (obj == null) {
      out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
      return;
    }

    char separator;
    List<?> list;
    switch (objInspector.getCategory()) {
    case PRIMITIVE:
      LazyUtils.writePrimitiveUTF8(out, obj,
          (PrimitiveObjectInspector) objInspector, escaped, escapeChar,
          needsEscape);
      return;
    case LIST:
      separator = (char) LazyUtils.getSeparator(separators, level);
      ListObjectInspector loi = (ListObjectInspector) objInspector;
      list = loi.getList(obj);
      ObjectInspector eoi = loi.getListElementObjectInspector();
      if (list == null) {
        out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
      } else {
        for (int i = 0; i < list.size(); i++) {
          if (i > 0) {
            out.write(separator);
          }
          serialize(out, list.get(i), eoi, separators, level + 1, nullSequence,
              escaped, escapeChar, needsEscape);
        }
      }
      return;
    case MAP:
      separator = (char) LazyUtils.getSeparator(separators, level);
      char keyValueSeparator =
           (char) LazyUtils.getSeparator(separators, level + 1);

      MapObjectInspector moi = (MapObjectInspector) objInspector;
      ObjectInspector koi = moi.getMapKeyObjectInspector();
      ObjectInspector voi = moi.getMapValueObjectInspector();
      Map<?, ?> map = moi.getMap(obj);
      if (map == null) {
        out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
      } else {
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          if (first) {
            first = false;
          } else {
            out.write(separator);
          }
          serialize(out, entry.getKey(), koi, separators, level + 2,
              nullSequence, escaped, escapeChar, needsEscape);
          out.write(keyValueSeparator);
          serialize(out, entry.getValue(), voi, separators, level + 2,
              nullSequence, escaped, escapeChar, needsEscape);
        }
      }
      return;
    case STRUCT:
      separator = (char) LazyUtils.getSeparator(separators, level);
      StructObjectInspector soi = (StructObjectInspector) objInspector;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      list = soi.getStructFieldsDataAsList(obj);
      if (list == null) {
        out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
      } else {
        for (int i = 0; i < list.size(); i++) {
          if (i > 0) {
            out.write(separator);
          }
          serialize(out, list.get(i), fields.get(i).getFieldObjectInspector(),
              separators, level + 1, nullSequence, escaped, escapeChar,
              needsEscape);
        }
      }
      return;
    case UNION:
      separator = (char) LazyUtils.getSeparator(separators, level);
      UnionObjectInspector uoi = (UnionObjectInspector) objInspector;
      List<? extends ObjectInspector> ois = uoi.getObjectInspectors();
      if (ois == null) {
        out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
      } else {
        LazyUtils.writePrimitiveUTF8(out, new Byte(uoi.getTag(obj)),
            PrimitiveObjectInspectorFactory.javaByteObjectInspector,
            escaped, escapeChar, needsEscape);
        out.write(separator);
        serialize(out, uoi.getField(obj), ois.get(uoi.getTag(obj)),
            separators, level + 1, nullSequence, escaped, escapeChar,
            needsEscape);
      }
      return;
    default:
      break;
    }

    throw new RuntimeException("Unknown category type: "
        + objInspector.getCategory());
  }

  /**
   * Returns the statistics after (de)serialization)
   */

  @Override
  public SerDeStats getSerDeStats() {
    // must be different
    assert (lastOperationSerialize != lastOperationDeserialize);

    if (lastOperationSerialize) {
      stats.setRawDataSize(serializedSize);
    } else {
      stats.setRawDataSize(cachedLazyStruct.getRawDataSerializedSize());
    }
    return stats;

  }

  @Override
  protected Writable transformFromUTF8(Writable blob) {
    Text text = (Text)blob;
    return SerDeUtils.transformTextFromUTF8(text, this.charset);
  }

  @Override
  protected Writable transformToUTF8(Writable blob) {
    Text text = (Text)blob;
    return SerDeUtils.transformTextToUTF8(text, this.charset);
  }

  /**
   * This method is deprecated and is only used for backward compatibility.
   * Replaced by @see org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters#LazySerDeParameters(Configuration, Properties, String)
   */
  @Deprecated
  public static SerDeParameters initSerdeParams(Configuration job,
      Properties tbl,
      String serdeName) throws SerDeException {
    return new SerDeParameters(job, tbl, serdeName);
  }

  /**
   * This class is deprecated and is only used for backward compatibility. Replace by
   * @see org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters .
   */
  @Deprecated
  public static class SerDeParameters extends org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters {

    public SerDeParameters(Configuration job,
        Properties tbl,
        String serdeName) throws SerDeException {
      super(job, tbl, serdeName);
    }
  }
}
