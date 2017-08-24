package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

//added by zhaowei 20170824
//函数基本是个空函数，只是增加一个函数名，到es handler里面会处理这个函数名
public class GenericUDFFullText extends GenericUDF {
	private final BooleanWritable output = new BooleanWritable();

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		output.set(true);
		return output;
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("fulltext", children);
	}
}
