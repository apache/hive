package org.apache.hadoop.hive.ql.io.parquet.serde;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.List;
import java.util.Properties;

public class ParquetHiveSerDeCaseSensitivityTest extends TestCase {

  public void testInitialize() throws Exception {
    System.out.println("test: parquetHiveSerdeCaseSensitivityTest");
    try {
      serdeWithCaseSensitiveProperty();
      System.out.println("test: parquetHiveSerdeCaseSensitivityTest - OK");
    }
    catch(final Exception e){
      e.printStackTrace();
      throw e;
    }
  }

  private void serdeWithCaseSensitiveProperty() throws Exception{
    final ParquetHiveSerDe serDe = new ParquetHiveSerDe();
    final Configuration conf = new Configuration();
    final Properties tbl = new Properties();
    tbl.setProperty("columns", "field1,field2,field3");
    tbl.setProperty("columns.types", "int:int:int");
    tbl.setProperty("casesensitive", "Field1,field2,FIELD3");
    tbl.setProperty(org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");
    serDe.initialize(conf, tbl);
    assertEquals("SerDe did not return a struct object inspector", serDe.getObjectInspector().getCategory(), ObjectInspector.Category.STRUCT);
    List<StructField> fieldRefs= (List<StructField>) ((StructObjectInspector)(serDe.getObjectInspector())).getAllStructFieldRefs();
    assertEquals("Wrong number of fields",fieldRefs.size(),3);
    assertTrue("Field1 name mismatch "+fieldRefs.get(0).getFieldName(), fieldRefs.get(0).getFieldName().compareTo("Field1") == 0);
    assertTrue("Field2 name mismatch", fieldRefs.get(1).getFieldName().compareTo("field2") == 0 );
    assertTrue("Field3 name mismatch", fieldRefs.get(2).getFieldName().compareTo("FIELD3") == 0 );

    System.out.println("test: parquetHiveSerdeCaseSensitivityTest - OK");
  }

}
