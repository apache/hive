//package org.apache.hadoop.hive.ql.io.parquet;
//
//import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
//import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetColumnReader;
//import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
//import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
//import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
//import org.apache.parquet.schema.MessageType;
//import org.junit.Test;
//
//import java.util.List;
//
//import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
//
//public class TestVectorizedParquetRecordReader {
//  private static final MessageType schema = parseMessageType(
//    "message hive_schema { "
//      + "required int32 int32_field; "
//      + "required int64 int64_field; "
//      + "required int96 int96_field; "
//      + "required double double_field; "
//      + "required float float_field; "
//      + "required boolean boolean_field; "
//      + "required fixed_len_byte_array(3) flba_field; "
//      + "optional fixed_len_byte_array(1) some_null_field; "
//      + "optional fixed_len_byte_array(1) all_null_field; "
//      + "optional binary binary_field; "
//      + "optional binary binary_field_non_repeating; "
//      + "optional group struct_field {"
//      + "  optional int32 a;\n"
//      + "  optional double b;\n"
//      + "}\n"
//      + "optional group map_field (MAP) {\n"
//      + "  repeated group map (MAP_KEY_VALUE) {\n"
//      + "    required binary key;\n"
//      + "    optional binary value;\n"
//      + "  }\n"
//      + "}\n"
//      + "optional group array_list (LIST) {\n"
//      + "  repeated group bag {\n"
//      + "    optional int32 array_element;\n"
//      + "  }\n"
//      + "}\n"
//      + "} ");
//
//  @Test
//  public void testVectorizedReaderBuilder() {
//    String types = "int";
//    String colNames = "int32_field";
//    VectorizedParquetColumnReader vectorizedParquetColumnReader = build(types, colNames);
//
//  }
//
//  private VectorizedParquetColumnReader build(String types, String colNames){
//
//    List<String> columnNamesList = DataWritableReadSupport.getColumnNames("int32_field");
//    List<TypeInfo> columnTypesList = DataWritableReadSupport.getColumnTypes("int");
//    TypeInfo rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNamesList, columnTypesList);
//    schema.getFields();
//    VectorizedParquetRecordReader.buildVectorizedParquetReader(rowTypeInfo);
//  }
//
//}
