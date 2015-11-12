package org.apache.hadoop.hive.udf.example;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

public class GenericUDFExampleAdd extends GenericUDF {

  Converter converter0;
  Converter converter1;
  DoubleWritable result = new DoubleWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    ObjectInspector doubleOI = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
    converter0 = (Converter) ObjectInspectorConverters.getConverter(arguments[0], doubleOI);
    converter1 = (Converter) ObjectInspectorConverters.getConverter(arguments[1], doubleOI);
    return doubleOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    DoubleWritable dw0 = (DoubleWritable) converter0.convert(arguments[0].get());
    DoubleWritable dw1 = (DoubleWritable) converter1.convert(arguments[0].get());
    if (dw0 == null || dw1 == null) {
      return null;
    }
    result.set(dw0.get() + dw1.get());
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "GenericUDFExampleAdd";
  }

}
