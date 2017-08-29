package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

//added by zhaowei 20170824
//检查字符串是否包含的函数
public class GenericUDFContains extends GenericUDF {
	private transient Converter stringConverter;
	private final BooleanWritable output = new BooleanWritable();

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		// 检查参数
		if (arguments.length != 2) {
			throw new UDFArgumentLengthException("contains() requires 2 argument, got " + arguments.length);
		}
		if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but "
					+ arguments[0].getTypeName() + " is passed. as first arguments");
		}
		if (arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(1, "Only primitive type arguments are accepted but "
					+ arguments[1].getTypeName() + " is passed. as second arguments");
		}
		PrimitiveCategory inputType1 = ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory();
		PrimitiveCategory inputType2 = ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory();
		if(inputType1!=PrimitiveCategory.STRING||inputType2!=PrimitiveCategory.STRING){
			throw new UDFArgumentTypeException(3, "contains function parameters only support \"String\" for now");
		}
		stringConverter = ObjectInspectorConverters.getConverter((PrimitiveObjectInspector) arguments[0],
				PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		String str1 = stringConverter.convert(arguments[0].get()).toString();
		String str2 = stringConverter.convert(arguments[1].get()).toString();
		boolean contains = str1.contains(str2);
		output.set(contains);
		return output;
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("contains", children);
	}
}
