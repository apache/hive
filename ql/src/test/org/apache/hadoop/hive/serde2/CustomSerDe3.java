package org.apache.hadoop.hive.serde2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class CustomSerDe3 extends CustomSerDe1 {
  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {

    // Read the configuration parameters
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

    // The input column can either be a string or a list of list of integer values.
    List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
    List<TypeInfo> columnTypes = TypeInfoUtils
        .getTypeInfosFromTypeString(columnTypeProperty);
    assert columnNames.size() == columnTypes.size();
    numColumns = columnNames.size();

    // No exception for type checking for simplicity
    // Constructing the row ObjectInspector:
    // The row consists of some string columns, some Array<Array<int> > columns.
    List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(
        columnNames.size());
    for (int c = 0; c < numColumns; c++) {
      if (columnTypes.get(c).equals(TypeInfoFactory.stringTypeInfo)) {
        columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      } else {
        // Blindly add this as a non settable list of list of integers,
        // should be sufficient for the test case.
        // Use the standard list object inspector.
        columnOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
            new CustomNonSettableListObjectInspector1(PrimitiveObjectInspectorFactory.javaIntObjectInspector)));
      }
    }
    // Use non-settable struct object inspector.
    rowOI = new CustomNonSettableStructObjectInspector1(
        columnNames, columnOIs);

    // Constructing the row object, etc, which will be reused for all rows.
    row = new ArrayList<String>(numColumns);
    for (int c = 0; c < numColumns; c++) {
      row.add(null);
    }
  }
}
