package org.apache.hadoop.hive.llap;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.hive.llap.Row;
import org.apache.hadoop.hive.llap.FieldDesc;
import org.apache.hadoop.hive.llap.Schema;
import org.apache.hadoop.hive.llap.TypeDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LlapRowRecordReader implements RecordReader<NullWritable, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(LlapRowRecordReader.class);

  Configuration conf;
  RecordReader<NullWritable, Text> reader;
  Schema schema;
  SerDe serde;
  final Text textData = new Text();

  public LlapRowRecordReader(Configuration conf, Schema schema, RecordReader<NullWritable, Text> reader) {
    this.conf = conf;
    this.schema = schema;
    this.reader = reader;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public Row createValue() {
    return new Row(schema);
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  public float getProgress() throws IOException {
    return 0;
  }

  @Override
  public boolean next(NullWritable key, Row value) throws IOException {
    Preconditions.checkArgument(value != null);

    if (serde == null) {
      try {
        serde = initSerDe(conf);
      } catch (SerDeException err) {
        throw new IOException(err);
      }
    }

    boolean hasNext = reader.next(key,  textData);
    if (hasNext) {
      // Deserialize Text to column values, and populate the row record
      Object rowObj;
      try {
        StructObjectInspector rowOI = (StructObjectInspector) serde.getObjectInspector();
        rowObj = serde.deserialize(textData);
        List<? extends StructField> colFields = rowOI.getAllStructFieldRefs();
        for (int idx = 0; idx < colFields.size(); ++idx) {
          StructField field = colFields.get(idx);
          Object colValue = rowOI.getStructFieldData(rowObj, field);
          Preconditions.checkState(field.getFieldObjectInspector().getCategory() == Category.PRIMITIVE,
              "Cannot handle non-primitive column type " + field.getFieldObjectInspector().getTypeName());

          PrimitiveObjectInspector poi = (PrimitiveObjectInspector) field.getFieldObjectInspector();
          // char/varchar special cased here since the row record handles them using Text
          switch (poi.getPrimitiveCategory()) {
            case CHAR:
              value.setValue(idx, ((HiveCharWritable) poi.getPrimitiveWritableObject(colValue)).getPaddedValue());
              break;
            case VARCHAR:
              value.setValue(idx, ((HiveVarcharWritable) poi.getPrimitiveWritableObject(colValue)).getTextValue());
              break;
            default:
              value.setValue(idx, (Writable) poi.getPrimitiveWritableObject(colValue));
              break;
          }
        }
      } catch (SerDeException err) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Error deserializing row from text: " + textData);
        }
        throw new IOException("Error deserializing row data", err);
      }
    }

    return hasNext;
  }

  public Schema getSchema() {
    return schema;
  }

  protected SerDe initSerDe(Configuration conf) throws SerDeException {
    Properties props = new Properties();
    StringBuffer columnsBuffer = new StringBuffer();
    StringBuffer typesBuffer = new StringBuffer();
    boolean isFirst = true;
    for (FieldDesc colDesc : schema.getColumns()) {
      if (!isFirst) {
        columnsBuffer.append(',');
        typesBuffer.append(',');
      }
      columnsBuffer.append(colDesc.getName());
      typesBuffer.append(colDesc.getTypeDesc().toString());
      isFirst = false;
    }
    String columns = columnsBuffer.toString();
    String types = typesBuffer.toString();
    props.put(serdeConstants.LIST_COLUMNS, columns);
    props.put(serdeConstants.LIST_COLUMN_TYPES, types);
    SerDe serde = new LazySimpleSerDe();
    serde.initialize(conf, props);

    return serde;
  }
}
