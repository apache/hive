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

package org.apache.hadoop.hive.serde2.lazy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
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
public class LazySimpleSerDe extends AbstractSerDe {

  public static final Log LOG = LogFactory.getLog(LazySimpleSerDe.class
      .getName());

  public static final byte[] DefaultSeparators = {(byte) 1, (byte) 2, (byte) 3};

  private ObjectInspector cachedObjectInspector;

  private long serializedSize;
  private SerDeStats stats;
  private boolean lastOperationSerialize;
  private boolean lastOperationDeserialize;

  @Override
  public String toString() {
    return getClass().toString()
        + "["
        + Arrays.asList(serdeParams.separators)
        + ":"
        + ((StructTypeInfo) serdeParams.rowTypeInfo).getAllStructFieldNames()
        + ":"
        + ((StructTypeInfo) serdeParams.rowTypeInfo)
            .getAllStructFieldTypeInfos() + "]";
  }

  public LazySimpleSerDe() throws SerDeException {
  }

  /**
   * Return the byte value of the number string.
   *
   * @param altValue
   *          The string containing a number.
   * @param defaultVal
   *          If the altValue does not represent a number, return the
   *          defaultVal.
   */
  public static byte getByte(String altValue, byte defaultVal) {
    if (altValue != null && altValue.length() > 0) {
      try {
        return Byte.valueOf(altValue).byteValue();
      } catch (NumberFormatException e) {
        return (byte) altValue.charAt(0);
      }
    }
    return defaultVal;
  }

  /**
   * SerDeParameters.
   *
   */
  public static class SerDeParameters {
    byte[] separators = DefaultSeparators;
    String nullString;
    Text nullSequence;
    TypeInfo rowTypeInfo;
    boolean lastColumnTakesRest;
    List<String> columnNames;
    List<TypeInfo> columnTypes;

    boolean escaped;
    byte escapeChar;
    boolean[] needsEscape;

    public List<TypeInfo> getColumnTypes() {
      return columnTypes;
    }

    public List<String> getColumnNames() {
      return columnNames;
    }

    public byte[] getSeparators() {
      return separators;
    }

    public String getNullString() {
      return nullString;
    }

    public Text getNullSequence() {
      return nullSequence;
    }

    public TypeInfo getRowTypeInfo() {
      return rowTypeInfo;
    }

    public boolean isLastColumnTakesRest() {
      return lastColumnTakesRest;
    }

    public boolean isEscaped() {
      return escaped;
    }

    public byte getEscapeChar() {
      return escapeChar;
    }

    public boolean[] getNeedsEscape() {
      return needsEscape;
    }
  }

  SerDeParameters serdeParams = null;

  /**
   * Initialize the SerDe given the parameters. serialization.format: separator
   * char or byte code (only supports byte-value up to 127) columns:
   * ","-separated column names columns.types: ",", ":", or ";"-separated column
   * types
   *
   * @see SerDe#initialize(Configuration, Properties)
   */
  public void initialize(Configuration job, Properties tbl)
      throws SerDeException {

    serdeParams = LazySimpleSerDe.initSerdeParams(job, tbl, getClass()
        .getName());

    // Create the ObjectInspectors for the fields
    cachedObjectInspector = LazyFactory.createLazyStructInspector(serdeParams
        .getColumnNames(), serdeParams.getColumnTypes(), serdeParams
        .getSeparators(), serdeParams.getNullSequence(), serdeParams
        .isLastColumnTakesRest(), serdeParams.isEscaped(), serdeParams
        .getEscapeChar());

    cachedLazyStruct = (LazyStruct) LazyFactory
        .createLazyObject(cachedObjectInspector);

    LOG.debug(getClass().getName() + " initialized with: columnNames="
        + serdeParams.columnNames + " columnTypes=" + serdeParams.columnTypes
        + " separator=" + Arrays.asList(serdeParams.separators)
        + " nullstring=" + serdeParams.nullString + " lastColumnTakesRest="
        + serdeParams.lastColumnTakesRest);

    serializedSize = 0;
    stats = new SerDeStats();
    lastOperationSerialize = false;
    lastOperationDeserialize = false;

  }

  public static SerDeParameters initSerdeParams(Configuration job,
      Properties tbl, String serdeName) throws SerDeException {
    SerDeParameters serdeParams = new SerDeParameters();
    // Read the separators: We use 8 levels of separators by default, but we
    // should change this when we allow users to specify more than 10 levels
    // of separators through DDL.
    serdeParams.separators = new byte[8];
    serdeParams.separators[0] = getByte(tbl.getProperty(serdeConstants.FIELD_DELIM,
        tbl.getProperty(serdeConstants.SERIALIZATION_FORMAT)), DefaultSeparators[0]);
    serdeParams.separators[1] = getByte(tbl
        .getProperty(serdeConstants.COLLECTION_DELIM), DefaultSeparators[1]);
    serdeParams.separators[2] = getByte(
        tbl.getProperty(serdeConstants.MAPKEY_DELIM), DefaultSeparators[2]);
    for (int i = 3; i < serdeParams.separators.length; i++) {
      serdeParams.separators[i] = (byte) (i + 1);
    }

    serdeParams.nullString = tbl.getProperty(
        serdeConstants.SERIALIZATION_NULL_FORMAT, "\\N");
    serdeParams.nullSequence = new Text(serdeParams.nullString);

    String lastColumnTakesRestString = tbl
        .getProperty(serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST);
    serdeParams.lastColumnTakesRest = (lastColumnTakesRestString != null && lastColumnTakesRestString
        .equalsIgnoreCase("true"));

    LazyUtils.extractColumnInfo(tbl, serdeParams, serdeName);

    // Create the LazyObject for storing the rows
    serdeParams.rowTypeInfo = TypeInfoFactory.getStructTypeInfo(
        serdeParams.columnNames, serdeParams.columnTypes);

    // Get the escape information
    String escapeProperty = tbl.getProperty(serdeConstants.ESCAPE_CHAR);
    serdeParams.escaped = (escapeProperty != null);
    if (serdeParams.escaped) {
      serdeParams.escapeChar = getByte(escapeProperty, (byte) '\\');
    }
    if (serdeParams.escaped) {
      serdeParams.needsEscape = new boolean[128];
      for (int i = 0; i < 128; i++) {
        serdeParams.needsEscape[i] = false;
      }
      serdeParams.needsEscape[serdeParams.escapeChar] = true;
      for (int i = 0; i < serdeParams.separators.length; i++) {
        serdeParams.needsEscape[serdeParams.separators[i]] = true;
      }
    }

    return serdeParams;
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
   * @see SerDe#deserialize(Writable)
   */
  public Object deserialize(Writable field) throws SerDeException {
    if (byteArrayRef == null) {
      byteArrayRef = new ByteArrayRef();
    }
    if (field instanceof BytesWritable) {
      BytesWritable b = (BytesWritable) field;
      // For backward-compatibility with hadoop 0.17
      byteArrayRef.setData(b.getBytes());
      cachedLazyStruct.init(byteArrayRef, 0, b.getLength());
    } else if (field instanceof Text) {
      Text t = (Text) field;
      byteArrayRef.setData(t.getBytes());
      cachedLazyStruct.init(byteArrayRef, 0, t.getLength());
    } else {
      throw new SerDeException(getClass().toString()
          + ": expects either BytesWritable or Text object!");
    }
    lastOperationSerialize = false;
    lastOperationDeserialize = true;
    return cachedLazyStruct;
  }

  /**
   * Returns the ObjectInspector for the row.
   */
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  /**
   * Returns the Writable Class after serialization.
   *
   * @see SerDe#getSerializedClass()
   */
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
   * @throws IOException
   * @see SerDe#serialize(Object, ObjectInspector)
   */
  public Writable serialize(Object obj, ObjectInspector objInspector)
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
    List<? extends StructField> declaredFields = (serdeParams.rowTypeInfo != null && ((StructTypeInfo) serdeParams.rowTypeInfo)
        .getAllStructFieldNames().size() > 0) ? ((StructObjectInspector) getObjectInspector())
        .getAllStructFieldRefs()
        : null;

    serializeStream.reset();
    serializedSize = 0;

    // Serialize each field
    for (int i = 0; i < fields.size(); i++) {
      // Append the separator if needed.
      if (i > 0) {
        serializeStream.write(serdeParams.separators[0]);
      }
      // Get the field objectInspector and the field object.
      ObjectInspector foi = fields.get(i).getFieldObjectInspector();
      Object f = (list == null ? null : list.get(i));

      if (declaredFields != null && i >= declaredFields.size()) {
        throw new SerDeException("Error: expecting " + declaredFields.size()
            + " but asking for field " + i + "\n" + "data=" + obj + "\n"
            + "tableType=" + serdeParams.rowTypeInfo.toString() + "\n"
            + "dataType="
            + TypeInfoUtils.getTypeInfoFromObjectInspector(objInspector));
      }

      serializeField(serializeStream, f, foi, serdeParams);
    }

    // TODO: The copy of data is unnecessary, but there is no work-around
    // since we cannot directly set the private byte[] field inside Text.
    serializeCache
        .set(serializeStream.getData(), 0, serializeStream.getCount());
    serializedSize = serializeStream.getCount();
    lastOperationSerialize = true;
    lastOperationDeserialize = false;
    return serializeCache;
  }

  protected void serializeField(ByteStream.Output out, Object obj, ObjectInspector objInspector,
      SerDeParameters serdeParams) throws SerDeException {
    try {
      serialize(out, obj, objInspector, serdeParams.separators, 1, serdeParams.nullSequence,
          serdeParams.escaped, serdeParams.escapeChar, serdeParams.needsEscape);
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
   *          Which chars needs to be escaped. This array should have size of
   *          128. Negative byte values (or byte values >= 128) are never
   *          escaped.
   * @throws IOException
   */
  public static void serialize(ByteStream.Output out, Object obj,
      ObjectInspector objInspector, byte[] separators, int level,
      Text nullSequence, boolean escaped, byte escapeChar, boolean[] needsEscape)
      throws IOException {

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
      separator = (char) separators[level];
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
      separator = (char) separators[level];
      char keyValueSeparator = (char) separators[level + 1];
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
      separator = (char) separators[level];
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
      separator = (char) separators[level];
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
}
