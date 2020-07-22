package org.apache.hadoop.hive.serde2.lazybinary;

import java.util.ArrayList;
import java.util.Properties;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

public class TestLazyBinaryStruct extends TestCase {

  @Test
  public void testEmptyStruct() {
    LazyBinaryStructObjectInspector oi = LazyBinaryObjectInspectorFactory
        .getLazyBinaryStructObjectInspector(new ArrayList<>(), new ArrayList<>());

    ByteArrayRef byteRef = new ByteArrayRef();
    byteRef.setData(new byte[]{0});

    LazyBinaryStruct data = (LazyBinaryStruct) LazyBinaryFactory.createLazyBinaryObject(oi);
    data.init(byteRef, 0, 0);

    assertEquals(data.getRawDataSerializedSize(), 0);
  }
  
  @Test
  public void testEmptyStructWithSerde() throws SerDeException {
    LazyBinaryStructObjectInspector oi = LazyBinaryObjectInspectorFactory
        .getLazyBinaryStructObjectInspector(new ArrayList<>(), new ArrayList<>());
    StandardStructObjectInspector standardOI = ObjectInspectorFactory
        .getStandardStructObjectInspector(new ArrayList<>(), new ArrayList<>());
    Properties schema = new Properties();
    schema.setProperty(serdeConstants.LIST_COLUMNS, "col0");
    schema.setProperty(serdeConstants.LIST_COLUMN_TYPES, "struct<>");

    LazyBinarySerDe serde = new LazyBinarySerDe();
    SerDeUtils.initializeSerDe(serde, new Configuration(), schema, null);
    Writable writable = serde.serialize(standardOI.create(), standardOI);
    Object out = serde.deserialize(writable);
    assertNull(oi.getStructFieldsDataAsList(out));
  }
}