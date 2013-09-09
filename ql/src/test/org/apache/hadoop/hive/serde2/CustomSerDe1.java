package org.apache.hadoop.hive.serde2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CustomSerDe1 extends AbstractSerDe {

  int numColumns;

  StructObjectInspector rowOI;
  ArrayList<String> row;

  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {

    // Read the configuration parameters
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

    // The input column can either be a string or a list of integer values.
    List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
    List<TypeInfo> columnTypes = TypeInfoUtils
        .getTypeInfosFromTypeString(columnTypeProperty);
    assert columnNames.size() == columnTypes.size();
    numColumns = columnNames.size();

    // No exception for type checking for simplicity
    // Constructing the row ObjectInspector:
    // The row consists of some string columns, some Array<int> columns.
    List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(
        columnNames.size());
    for (int c = 0; c < numColumns; c++) {
      if (columnTypes.get(c).equals(TypeInfoFactory.stringTypeInfo)) {
        columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      } else {
        // Blindly add this as a integer list, should be sufficient for the test case.
        // Use the non-settable list object inspector.
        columnOIs.add(new CustomNonSettableListObjectInspector1(
            PrimitiveObjectInspectorFactory.javaIntObjectInspector));
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

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowOI;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    // Now all the column values should always return NULL!
    return row;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {
    return null;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }

}
