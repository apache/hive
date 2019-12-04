/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.write;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.hadoop.hive.common.type.CalendarUtils;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
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
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 *
 * DataWritableWriter sends a record to the Parquet API with the expected schema in order
 * to be written to a file.
 * This class is only used through DataWritableWriteSupport class.
 */
public class DataWritableWriter {
  private static final Logger LOG = LoggerFactory.getLogger(DataWritableWriter.class);
  protected final RecordConsumer recordConsumer;
  private final GroupType schema;
  private final boolean defaultDateProleptic;

  /* This writer will be created when writing the first row in order to get
  information about how to inspect the record data.  */
  private DataWriter messageWriter;

  public DataWritableWriter(final RecordConsumer recordConsumer, final GroupType schema,
      final boolean defaultDateProleptic) {
    this.recordConsumer = recordConsumer;
    this.schema = schema;
    this.defaultDateProleptic = defaultDateProleptic;
  }

  /**
   * It writes a record to Parquet.
   * @param record Contains the record that is going to be written.
   */
  public void write(final ParquetHiveRecord record) {
    if (record != null) {
      if (messageWriter == null) {
        try {
          messageWriter = createMessageWriter(record.getObjectInspector(), schema);
        } catch (RuntimeException e) {
          String errorMessage = "Parquet record is malformed: " + e.getMessage();
          LOG.error(errorMessage, e);
          throw new RuntimeException(errorMessage, e);
        }
      }

      messageWriter.write(record.getObject());
    }
  }

  private MessageDataWriter createMessageWriter(StructObjectInspector inspector, GroupType schema) {
    return new MessageDataWriter(inspector, schema);
  }

  /**
   * Creates a writer for the specific object inspector. The returned writer will be used
   * to call Parquet API for the specific data type.
   * @param inspector The object inspector used to get the correct value type.
   * @param type Type that contains information about the type schema.
   * @return A ParquetWriter object used to call the Parquet API fo the specific data type.
   */
  private DataWriter createWriter(ObjectInspector inspector, Type type) {
    if (type.isPrimitive()) {
      checkInspectorCategory(inspector, ObjectInspector.Category.PRIMITIVE);
      PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector)inspector;
      switch (primitiveInspector.getPrimitiveCategory()) {
        case BOOLEAN:
          return new BooleanDataWriter((BooleanObjectInspector)inspector);
        case BYTE:
          return new ByteDataWriter((ByteObjectInspector)inspector);
        case SHORT:
          return new ShortDataWriter((ShortObjectInspector)inspector);
        case INT:
          return new IntDataWriter((IntObjectInspector)inspector);
        case LONG:
          return new LongDataWriter((LongObjectInspector)inspector);
        case FLOAT:
          return new FloatDataWriter((FloatObjectInspector)inspector);
        case DOUBLE:
          return new DoubleDataWriter((DoubleObjectInspector)inspector);
        case STRING:
          return new StringDataWriter((StringObjectInspector)inspector);
        case CHAR:
          return new CharDataWriter((HiveCharObjectInspector)inspector);
        case VARCHAR:
          return new VarcharDataWriter((HiveVarcharObjectInspector)inspector);
        case BINARY:
          return new BinaryDataWriter((BinaryObjectInspector)inspector);
        case TIMESTAMP:
          return new TimestampDataWriter((TimestampObjectInspector)inspector);
        case DECIMAL:
          return new DecimalDataWriter((HiveDecimalObjectInspector)inspector);
        case DATE:
          return new DateDataWriter((DateObjectInspector)inspector);
        default:
          throw new IllegalArgumentException("Unsupported primitive data type: " + primitiveInspector.getPrimitiveCategory());
      }
    } else {
      GroupType groupType = type.asGroupType();
      OriginalType originalType = type.getOriginalType();

      if (originalType != null && originalType.equals(OriginalType.LIST)) {
        checkInspectorCategory(inspector, ObjectInspector.Category.LIST);
        return new ListDataWriter((ListObjectInspector)inspector, groupType);
      } else if (originalType != null && originalType.equals(OriginalType.MAP)) {
        checkInspectorCategory(inspector, ObjectInspector.Category.MAP);
        return new MapDataWriter((MapObjectInspector)inspector, groupType);
      } else {
        checkInspectorCategory(inspector, ObjectInspector.Category.STRUCT);
        return new StructDataWriter((StructObjectInspector)inspector, groupType);
      }
    }
  }

  /**
   * Checks that an inspector matches the category indicated as a parameter.
   * @param inspector The object inspector to check
   * @param category The category to match
   * @throws IllegalArgumentException if inspector does not match the category
   */
  private void checkInspectorCategory(ObjectInspector inspector, ObjectInspector.Category category) {
    if (!inspector.getCategory().equals(category)) {
      throw new IllegalArgumentException("Invalid data type: expected " + category
          + " type, but found: " + inspector.getCategory());
    }
  }

  private interface DataWriter {
    void write(Object value);
  }

  private class GroupDataWriter implements DataWriter {
    private StructObjectInspector inspector;
    private List<? extends StructField> structFields;
    private DataWriter[] structWriters;

    public GroupDataWriter(StructObjectInspector inspector, GroupType groupType) {
      this.inspector = inspector;

      structFields = this.inspector.getAllStructFieldRefs();
      structWriters = new DataWriter[structFields.size()];

      for (int i = 0; i < structFields.size(); i++) {
        StructField field = structFields.get(i);
        structWriters[i] = createWriter(field.getFieldObjectInspector(), groupType.getType(i));
      }
    }

    @Override
    public void write(Object value) {
      for (int i = 0; i < structFields.size(); i++) {
        StructField field = structFields.get(i);
        Object fieldValue = inspector.getStructFieldData(value, field);

        if (fieldValue != null) {
          String fieldName = field.getFieldName();
          DataWriter writer = structWriters[i];

          recordConsumer.startField(fieldName, i);
          writer.write(fieldValue);
          recordConsumer.endField(fieldName, i);
        }
      }
    }
  }

  private class MessageDataWriter extends GroupDataWriter implements DataWriter {
    public MessageDataWriter(StructObjectInspector inspector, GroupType groupType) {
      super(inspector, groupType);
    }

    @Override
    public void write(Object value) {
      recordConsumer.startMessage();
      if (value != null) {
        super.write(value);
      }
      recordConsumer.endMessage();
    }
  }

  private class StructDataWriter extends GroupDataWriter implements DataWriter {
    public StructDataWriter(StructObjectInspector inspector, GroupType groupType) {
      super(inspector, groupType);
    }

    @Override
    public void write(Object value) {
      recordConsumer.startGroup();
      super.write(value);
      recordConsumer.endGroup();
    }
  }

  private class ListDataWriter implements DataWriter {
    private ListObjectInspector inspector;
    private String elementName;
    private DataWriter elementWriter;
    private String repeatedGroupName;

    public ListDataWriter(ListObjectInspector inspector, GroupType groupType) {
      this.inspector = inspector;

      // Get the internal array structure
      GroupType repeatedType = groupType.getType(0).asGroupType();
      this.repeatedGroupName = repeatedType.getName();

      Type elementType = repeatedType.getType(0);
      this.elementName = elementType.getName();

      ObjectInspector elementInspector = this.inspector.getListElementObjectInspector();
      this.elementWriter = createWriter(elementInspector, elementType);
    }

    @Override
    public void write(Object value) {
      recordConsumer.startGroup();
      int listLength = inspector.getListLength(value);

      if (listLength > 0) {
        recordConsumer.startField(repeatedGroupName, 0);

        for (int i = 0; i < listLength; i++) {
          Object element = inspector.getListElement(value, i);
          recordConsumer.startGroup();
          if (element != null) {
            recordConsumer.startField(elementName, 0);
            elementWriter.write(element);
            recordConsumer.endField(elementName, 0);
          }
          recordConsumer.endGroup();
        }

        recordConsumer.endField(repeatedGroupName, 0);
      }
      recordConsumer.endGroup();
    }
  }

  private class MapDataWriter implements DataWriter {
    private MapObjectInspector inspector;
    private String repeatedGroupName;
    private String keyName, valueName;
    private DataWriter keyWriter, valueWriter;

    public MapDataWriter(MapObjectInspector inspector, GroupType groupType) {
      this.inspector = inspector;

      // Get the internal map structure (MAP_KEY_VALUE)
      GroupType repeatedType = groupType.getType(0).asGroupType();
      this.repeatedGroupName = repeatedType.getName();

      // Get key element information
      Type keyType = repeatedType.getType(0);
      ObjectInspector keyInspector = this.inspector.getMapKeyObjectInspector();
      this.keyName = keyType.getName();
      this.keyWriter = createWriter(keyInspector, keyType);

      // Get value element information
      Type valuetype = repeatedType.getType(1);
      ObjectInspector valueInspector = this.inspector.getMapValueObjectInspector();
      this.valueName = valuetype.getName();
      this.valueWriter = createWriter(valueInspector, valuetype);
    }

    @Override
    public void write(Object value) {
      recordConsumer.startGroup();

      Map<?, ?> mapValues = inspector.getMap(value);
      if (mapValues != null && mapValues.size() > 0) {
        recordConsumer.startField(repeatedGroupName, 0);
        for (Map.Entry<?, ?> keyValue : mapValues.entrySet()) {
          recordConsumer.startGroup();
          if (keyValue != null) {
            // write key element
            Object keyElement = keyValue.getKey();
            recordConsumer.startField(keyName, 0);
            keyWriter.write(keyElement);
            recordConsumer.endField(keyName, 0);

            // write value element
            Object valueElement = keyValue.getValue();
            if (valueElement != null) {
              recordConsumer.startField(valueName, 1);
              valueWriter.write(valueElement);
              recordConsumer.endField(valueName, 1);
            }
          }
          recordConsumer.endGroup();
        }

        recordConsumer.endField(repeatedGroupName, 0);
      }
      recordConsumer.endGroup();
    }
  }

  private class BooleanDataWriter implements DataWriter {
    private BooleanObjectInspector inspector;

    public BooleanDataWriter(BooleanObjectInspector inspector) {
      this.inspector = inspector;
    }

    @Override
    public void write(Object value) {
      recordConsumer.addBoolean(inspector.get(value));
    }
  }

  private class ByteDataWriter implements DataWriter {
    private ByteObjectInspector inspector;

    public ByteDataWriter(ByteObjectInspector inspector) {
      this.inspector = inspector;
    }

    @Override
    public void write(Object value) {
      recordConsumer.addInteger(inspector.get(value));
    }
  }

  private class ShortDataWriter implements DataWriter {
    private ShortObjectInspector inspector;
    public ShortDataWriter(ShortObjectInspector inspector) {
      this.inspector = inspector;
    }

    @Override
    public void write(Object value) {
      recordConsumer.addInteger(inspector.get(value));
    }
  }

  private class IntDataWriter implements DataWriter {
    private IntObjectInspector inspector;

    public IntDataWriter(IntObjectInspector inspector) {
      this.inspector = inspector;
    }

    @Override
    public void write(Object value) {
      recordConsumer.addInteger(inspector.get(value));
    }
  }

  private class LongDataWriter implements DataWriter {
    private LongObjectInspector inspector;

    public LongDataWriter(LongObjectInspector inspector) {
      this.inspector = inspector;
    }

    @Override
    public void write(Object value) {
      recordConsumer.addLong(inspector.get(value));
    }
  }

  private class FloatDataWriter implements DataWriter {
    private FloatObjectInspector inspector;

    public FloatDataWriter(FloatObjectInspector inspector) {
      this.inspector = inspector;
    }

    @Override
    public void write(Object value) {
      recordConsumer.addFloat(inspector.get(value));
    }
  }

  private class DoubleDataWriter implements DataWriter {
    private DoubleObjectInspector inspector;

    public DoubleDataWriter(DoubleObjectInspector inspector) {
      this.inspector = inspector;
    }

    @Override
    public void write(Object value) {
      recordConsumer.addDouble(inspector.get(value));
    }
  }

  private class StringDataWriter implements DataWriter {
    private StringObjectInspector inspector;

    public StringDataWriter(StringObjectInspector inspector) {
      this.inspector = inspector;
    }

    @Override
    public void write(Object value) {
      String v = inspector.getPrimitiveJavaObject(value);
      recordConsumer.addBinary(Binary.fromString(v));
    }
  }

  private class CharDataWriter implements DataWriter {
    private HiveCharObjectInspector inspector;

    public CharDataWriter(HiveCharObjectInspector inspector) {
      this.inspector = inspector;
    }

    @Override
    public void write(Object value) {
      String v = inspector.getPrimitiveJavaObject(value).getStrippedValue();
      recordConsumer.addBinary(Binary.fromString(v));
    }
  }

  private class VarcharDataWriter implements DataWriter {
    private HiveVarcharObjectInspector inspector;

    public VarcharDataWriter(HiveVarcharObjectInspector inspector) {
      this.inspector = inspector;
    }

    @Override
    public void write(Object value) {
      String v = inspector.getPrimitiveJavaObject(value).getValue();
      recordConsumer.addBinary(Binary.fromString(v));
    }
  }

  private class BinaryDataWriter implements DataWriter {
    private BinaryObjectInspector inspector;

    public BinaryDataWriter(BinaryObjectInspector inspector) {
      this.inspector = inspector;
    }

    @Override
    public void write(Object value) {
      byte[] vBinary = inspector.getPrimitiveJavaObject(value);
      recordConsumer.addBinary(Binary.fromByteArray(vBinary));
    }
  }

  private class TimestampDataWriter implements DataWriter {
    private TimestampObjectInspector inspector;

    public TimestampDataWriter(TimestampObjectInspector inspector) {
      this.inspector = inspector;
    }

    @Override
    public void write(Object value) {
      Timestamp ts = inspector.getPrimitiveJavaObject(value);
      recordConsumer.addBinary(NanoTimeUtils.getNanoTime(ts, false).toBinary());
    }
  }

  private class DecimalDataWriter implements DataWriter {
    private HiveDecimalObjectInspector inspector;

    public DecimalDataWriter(HiveDecimalObjectInspector inspector) {
      this.inspector = inspector;
    }

    @Override
    public void write(Object value) {
      HiveDecimal vDecimal = inspector.getPrimitiveJavaObject(value);
      DecimalTypeInfo decTypeInfo = (DecimalTypeInfo)inspector.getTypeInfo();
      recordConsumer.addBinary(decimalToBinary(vDecimal, decTypeInfo));
    }

    private Binary decimalToBinary(final HiveDecimal hiveDecimal, final DecimalTypeInfo decimalTypeInfo) {
      int prec = decimalTypeInfo.precision();
      int scale = decimalTypeInfo.scale();

      byte[] decimalBytes = hiveDecimal.bigIntegerBytesScaled(scale);

      // Estimated number of bytes needed.
      int precToBytes = ParquetHiveSerDe.PRECISION_TO_BYTE_COUNT[prec - 1];
      if (precToBytes == decimalBytes.length) {
        // No padding needed.
        return Binary.fromByteArray(decimalBytes);
      }

      byte[] tgt = new byte[precToBytes];
      if (hiveDecimal.signum() == -1) {
        // For negative number, initializing bits to 1
        for (int i = 0; i < precToBytes; i++) {
          tgt[i] |= 0xFF;
        }
      }

      System.arraycopy(decimalBytes, 0, tgt, precToBytes - decimalBytes.length, decimalBytes.length); // Padding leading zeroes/ones.
      return Binary.fromByteArray(tgt);
    }
  }

  private class DateDataWriter implements DataWriter {
    private DateObjectInspector inspector;

    public DateDataWriter(DateObjectInspector inspector) {
      this.inspector = inspector;
    }

    @Override
    public void write(Object value) {
      Date vDate = inspector.getPrimitiveJavaObject(value);
      recordConsumer.addInteger(
          defaultDateProleptic ? DateWritableV2.dateToDays(vDate) :
              CalendarUtils.convertDateToHybrid(DateWritableV2.dateToDays(vDate)));
    }
  }
}
