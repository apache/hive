package org.apache.hadoop.hive.hbase.struct;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang.ClassUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.hbase.HBaseSerDeParameters;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStatsStruct;
import org.apache.hadoop.hive.serde2.StructObject;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.lazy.LazyUnion;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyListObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;

public class ProtoBufHBaseValueFactory extends DefaultHBaseValueFactory {
	private static class ClassMethod {

		public Class<?> clazz;
		public String method;

		public ClassMethod(Class<?> c, String m) {
			this.clazz = c;
			this.method = m;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final ClassMethod other = (ClassMethod) obj;
			if (this.clazz != other.clazz && (this.clazz == null || !this.clazz.equals(other.clazz))) {
				return false;
			}
			if ((this.method == null) ? (other.method != null) : !this.method.equals(other.method)) {
				return false;
			}
			return true;
		}

		@Override
		public int hashCode() {
			int hash = 5;
			hash = 67 * hash + (this.clazz != null ? this.clazz.hashCode() : 0);
			hash = 67 * hash + (this.method != null ? this.method.hashCode() : 0);
			return hash;
		}

	}

	private static final class NoSuchMethod {
		@SuppressWarnings("unused")
		public final void none() {
		}
	}

	public static final Method NO_SUCH_METHOD;

	static {
		try {
			NO_SUCH_METHOD = NoSuchMethod.class.getDeclaredMethod("none");
		} catch (NoSuchMethodException e) {
			throw new RuntimeException("Can't happen", e);
		}
	}

	private Class<?> serClass;

	List<String> valueColumnNames = new ArrayList<String>();
	List<TypeInfo> valueColumnTypes = new ArrayList<TypeInfo>();
	List<ObjectInspector> valueOIs = new ArrayList<ObjectInspector>();

	Map<Class<?>, Map<String, Method>> cached = new HashMap<Class<?>, Map<String, Method>>();
	Map<Class<?>, Map<String, Method>> cachedHas = new HashMap<Class<?>, Map<String, Method>>();
	Map<ClassMethod, FieldDescriptor> protoCache = new HashMap<ClassMethod, FieldDescriptor>();
	Map<ClassMethod, FieldDescriptor> protoHasCache = new HashMap<ClassMethod, FieldDescriptor>();

	private Method parseFromMethod;

	public static final Object[] noArgs = new Object[0];
	public static final Class<?>[] byteArrayParameters = new Class[] { new byte[0].getClass() };

	public ProtoBufHBaseValueFactory(int fieldID, Class<?> serClass)
			throws NoSuchMethodException, SecurityException, SerDeException {
		super(fieldID);

		this.serClass = serClass;
		this.parseFromMethod = this.serClass.getMethod("parseFrom", byteArrayParameters);
	}

	@Override
	public void init(HBaseSerDeParameters hbaseParams, Configuration conf, Properties properties)
			throws SerDeException {
		super.init(hbaseParams, conf, properties);
		
		valueColumnNames = new ArrayList<String>();
		valueColumnTypes = new ArrayList<TypeInfo>();
		valueOIs = new ArrayList<ObjectInspector>();

		ProtoBufHBaseValueFactory.populateTypeInfoForClass(serClass, valueColumnNames, valueColumnTypes, 0);

		for (int i = 0; i < valueColumnNames.size(); i++) {
			valueOIs.add(i, createValueObjectInspector(valueColumnTypes.get(i)));
		}
	}

	@Override
	public ObjectInspector createValueObjectInspector(TypeInfo type) throws SerDeException {
		return LazyFactory.createLazyObjectInspector(type, 1, serdeParams, ObjectInspectorOptions.JAVA);
	}

	public static void generateProtoBufStructFromClass(Class<?> serClass, StringBuilder sb) {
		List<String> valueColumnNames = new ArrayList<String>();
		List<TypeInfo> valueColumnTypes = new ArrayList<TypeInfo>();
		populateTypeInfoForClass(serClass, valueColumnNames, valueColumnTypes, 0);

		populateColumTypes(sb, valueColumnNames, valueColumnTypes);
	}

	private static void populateColumTypes(StringBuilder sb, List<String> names, List<TypeInfo> types) {
		for (int i = 0; i < names.size(); i++) {
			if (i > 0)
				sb.append(",");
			sb.append(names.get(i)).append(":").append(createColumnType(types.get(i)));
		}
	}

	private static String createColumnType(TypeInfo ti) {
		switch (ti.getCategory()) {
		case PRIMITIVE:
			return ti.getTypeName();
		case LIST:
			ListTypeInfo lti = (ListTypeInfo) ti;
			TypeInfo subType = lti.getListElementTypeInfo();
			return new StringBuilder().append("ARRAY<").append(createColumnType(subType)).append(">").toString();
		case STRUCT:
			StructTypeInfo sti = (StructTypeInfo) ti;
			ArrayList<String> subnames = sti.getAllStructFieldNames();
			ArrayList<TypeInfo> subtypes = sti.getAllStructFieldTypeInfos();
			StringBuilder subTypesBuilder = new StringBuilder();
			populateColumTypes(subTypesBuilder, subnames, subtypes);
			return new StringBuilder().append("STRUCT<").append(subTypesBuilder).append(">").toString();
		default:
			throw new IllegalArgumentException("Not implemented:" + ti.getCategory());
		}
	}

	private static void populateTypeInfoForClass(Class<?> kclass, List<String> columnNames, List<TypeInfo> columnTypes,
			int indent) {

		// believe it or not the order is not preserved
		// this could obliterate overloaded methods
		// but getters could not be overloaded so we are ok
		Method[] methods = kclass.getDeclaredMethods(); // getMethods()
		SortedMap<String, Method> sortedMethods = new TreeMap<String, Method>();
		for (Method m : methods) {
			sortedMethods.put(m.getName(), m);
		}

		for (Method m : sortedMethods.values()) {
			if (!m.getName().startsWith("get")) {
				continue;
			}
			if (m.getParameterTypes().length != 0) {
				continue;
			}
			if (m.getReturnType() == null) {
				continue;
			}
			if (m.getName().equals("getDefaultInstance")) {
				continue;
			}
			if (m.getName().equals("getDefaultInstanceForType")) {
				continue;
			}
			if (m.getName().equalsIgnoreCase("getSerializedSize")) {
				continue;
			}

			if (m.getReturnType().getName().contains("Descriptor")) {
				continue;
			}

			if (m.getReturnType().isPrimitive() || m.getReturnType().equals(String.class)) {
				columnNames.add(m.getName().substring(3));
				columnTypes.add(TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(m.getReturnType()));
			}
			if (m.getName().contains("OrBuilderList")) {
				continue;
			}

			if (isaList(m.getReturnType())) {
				String columnName = m.getName().substring(3);
				Class<?> listClass = null;
				Type returnType = m.getGenericReturnType();
				if (returnType instanceof ParameterizedType) {
					ParameterizedType type = (ParameterizedType) returnType;
					Type[] typeArguments = type.getActualTypeArguments();
					for (Type typeArgument : typeArguments) {
						listClass = (Class<?>) typeArgument;
					}
				}
				if (protoPrimitives.contains(listClass)) {
					columnNames.add(columnName);
					columnTypes.add(TypeInfoFactory
							.getListTypeInfo(TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(listClass)));
				} else {
					List<String> subColumnNames = new ArrayList<String>();
					List<TypeInfo> subColumnTypes = new ArrayList<TypeInfo>();
					populateTypeInfoForClass(listClass, subColumnNames, subColumnTypes, 0);
					columnNames.add(columnName);
					TypeInfo build = TypeInfoFactory.getStructTypeInfo(subColumnNames, subColumnTypes);
					columnTypes.add(TypeInfoFactory.getListTypeInfo(build));
				}
				// nested list not possible with protobuf
			}

			if (m.getReturnType().getSuperclass() != null) {
				if (m.getReturnType().getSuperclass().equals(com.google.protobuf.GeneratedMessage.class)) {
					List<String> subColumnNames = new ArrayList<String>();
					List<TypeInfo> subColumnTypes = new ArrayList<TypeInfo>();
					populateTypeInfoForClass(m.getReturnType(), subColumnNames, subColumnTypes, indent + 1);
					columnNames.add(m.getName().substring(3));
					columnTypes.add(TypeInfoFactory.getStructTypeInfo(subColumnNames, subColumnTypes));
				}
			}
		}
	}

	private static Set<Class<?>> protoPrimitives = Sets.<Class<?>> newHashSet(Boolean.class, Byte.class, Short.class,
			Integer.class, Long.class, Float.class, Double.class, String.class);

	public Object protoGet(Object o, String prop) throws Exception {
		prop = prop.toLowerCase();
		if (prop.equals("serializedsize")) {
			return reflectGet(o, prop);
		}
		GeneratedMessage m = (GeneratedMessage) o;
		return m.getField(m.getDescriptorForType().findFieldByName(prop));
	}

	public Object protoCacheGet(Message m, String prop) throws Exception {
		prop = prop.toLowerCase();
		if (prop.endsWith("count")) {
			return reflectGet(m, prop);
		}
		ClassMethod cm = new ClassMethod(m.getClass(), prop);
		FieldDescriptor f = this.protoCache.get(cm);
		if (f == null) {
			f = m.getDescriptorForType().findFieldByName(prop);
			this.protoCache.put(cm, f);
		}
		return m.getField(f);
	}

	public boolean protoHas(Message m, String prop) throws Exception {
		prop = prop.toLowerCase();
		if (prop.contains("count")) {
			return reflectHas(m, prop);
		}
		ClassMethod meth = new ClassMethod(m.getClass(), prop);
		FieldDescriptor f = protoHasCache.get(meth);
		if (f == null) {
			f = m.getDescriptorForType().findFieldByName(prop);
			protoHasCache.put(meth, f);
		}
		return m.hasField(f);
	}

	public Method findMethodIgnoreCase(Class<?> cls, String methodName) {
		Method[] methods = cls.getMethods();
		for (int i = 0; i < methods.length; i++) {
			if (methods[i].getName().equalsIgnoreCase(methodName)) {
				return methods[i];
			}
		}
		return NO_SUCH_METHOD;
	}

	public Map<String, Method> getCachedSubMap(Map<Class<?>, Map<String, Method>> cache, Class<?> cls) {
		Map<String, Method> classMap = cache.get(cls);
		if (classMap == null) {
			classMap = new HashMap<String, Method>();
			cache.put(cls, classMap);
		}
		return classMap;
	}

	public boolean reflectHas(Object o, String prop) throws Exception {
		final Class<?> cls = o.getClass();
		final Map<String, Method> classMap = getCachedSubMap(this.cachedHas, cls);
		Method m = classMap.get(prop);
		if (m == null) {
			m = findMethodIgnoreCase(cls, "has" + prop);
			classMap.put(prop, m);
		}
		// Methods like xCount (which need to be removed) are artifically
		// generated
		// thus we say they always exist for now
		if (m == NO_SUCH_METHOD) {
			return true;
		} else {
			Object result = m.invoke(o, noArgs);
			return (Boolean.TRUE.equals(result));
		}
	}

	public Object reflectGet(Object o, String prop) throws Exception {
		final Class<?> cls = o.getClass();
		final Map<String, Method> classMap = getCachedSubMap(this.cached, cls);
		Method m = classMap.get(prop);
		if (m == null) {
			m = findMethodIgnoreCase(cls, "get" + prop);
			classMap.put(prop, m);
		}
		Object result = m.invoke(o, noArgs);
		return result;
	}

	private static boolean isaList(Class<?> c) {
		if (c.equals(java.util.ArrayList.class)) {
			return true;
		} else if (c.equals(java.util.List.class)) {
			return true;
		} else if (c.equals(java.util.Collection.class)) {
			return true;
		} else {
			return false;
		}
	}

	private void deserialize(byte[] value, List<Object> valueRow) throws SerDeException {
		Message vparsedResult = null;
		try {
			vparsedResult = (Message) parseFromMethod.invoke(null, value);
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
			throw new SerDeException(ex.getMessage(), ex);
		}

		try {
			this.matchProtoToRow(vparsedResult, valueRow, valueOIs, valueColumnNames);
		} catch (Exception ex) {
			throw new SerDeException(ex);
		}
	}

	private void matchProtoToRow(Message proto, List<Object> row, List<ObjectInspector> ois, List<String> columnNames)
			throws Exception {
		for (int i = 0; i < columnNames.size(); i++) {
			matchProtoToRowField(proto, row, ois.get(i), columnNames.get(i));
		}
	}

	private void matchProtoToRow(Message proto, List<Object> row, List<? extends StructField> structFields)
			throws Exception {
		for (int i = 0; i < structFields.size(); i++) {
			ObjectInspector oi = structFields.get(i).getFieldObjectInspector();
			String colName = structFields.get(i).getFieldName();
			matchProtoToRowField(proto, row, oi, colName);
		}
	}

	private void matchProtoToRowField(Message m, List<Object> row, ObjectInspector oi, String colName)
			throws Exception {
		switch (oi.getCategory()) {
		case PRIMITIVE:
			if (this.reflectHas(m, colName)) {
				row.add(reflectGet(m, colName));
			} else {
				row.add(null);
			}
			break;
		case LIST:
			/*
			 * there is no hasList in proto a null list is an empty list. no
			 * need to call hasX
			 */
			Object listObject = reflectGet(m, colName);
			ListObjectInspector li = (ListObjectInspector) oi;
			ObjectInspector subOi = li.getListElementObjectInspector();
			if (subOi.getCategory() == Category.PRIMITIVE) {
				row.add(listObject);
			} else if (subOi.getCategory() == Category.STRUCT) {
				@SuppressWarnings("unchecked")
				List<Message> x = (List<Message>) listObject;
				StructObjectInspector soi = (StructObjectInspector) subOi;
				List<? extends StructField> substructs = soi.getAllStructFieldRefs();
				List<Object> arrayOfStruct = new ArrayList<Object>(x.size());
				for (int it = 0; it < x.size(); it++) {
					List<Object> subList = new ArrayList<Object>(substructs.size());
					matchProtoToRow(x.get(it), subList, substructs);
					arrayOfStruct.add(new LazyProtoBufObject(subList, soi));
				}
				row.add(arrayOfStruct);
			} else {
				// never should happen
				throw new IllegalArgumentException("Not implemented:" + subOi.getCategory());
			}
			break;
		case STRUCT:
			if (this.reflectHas(m, colName)) {
				Message subObject = (Message) reflectGet(m, colName);
				StructObjectInspector so = (StructObjectInspector) oi;
				List<? extends StructField> substructs = so.getAllStructFieldRefs();
				List<Object> subList = new ArrayList<Object>(substructs.size());
				matchProtoToRow(subObject, subList, substructs);
				row.add(new LazyProtoBufObject(subList, so));
			} else {
				row.add(null);
			}
			break;
		default:
			throw new IllegalArgumentException("Not implemented:" + oi.getCategory());
		}
	}

	private class LazyProtoBufObject implements LazyObjectBase, StructObject, SerDeStatsStruct {
		private boolean isNull = false;
		private byte[] buf = null;
		private List<Object> valueRow = null;
		private StructObjectInspector inspector;
		private boolean doDeserialize;

		public LazyProtoBufObject(ObjectInspector inspector) {
			this.inspector = (StructObjectInspector) inspector;
			this.doDeserialize = true;
		}

		public LazyProtoBufObject(List<Object> subList, StructObjectInspector soi) {
			this.valueRow = subList;
			this.inspector = soi;
			this.doDeserialize = false;
		}

		public void init(ByteArrayRef bytes, int start, int length) {
			buf = new byte[length];
			System.arraycopy(bytes.getData(), start, buf, 0, length);

			if (!this.doDeserialize)
				return;

			isNull = false;
			valueRow = new ArrayList<Object>();
			try {
				deserialize(buf, valueRow);
			} catch (SerDeException e) {
				isNull = true;
				throw new IllegalStateException("unable to deserialize", e);
			}
		}

		public void setNull() {
			isNull = true;
		}

		public Object getObject() {
			if (isNull)
				return null;
			return this;
		}

		@Override
		public long getRawDataSerializedSize() {
			return buf.length;
		}

		@Override
		public Object getField(int fieldID) {
			try {
				Object field = valueRow.get(fieldID);
				if (field == null)
					return null;
				return toLazyObject(field,
						this.inspector.getAllStructFieldRefs().get(fieldID).getFieldObjectInspector());
			} catch (Throwable t) {
				t.printStackTrace();
				return null;
			}
		}

		@Override
		public List<Object> getFieldsAsList() {
			return valueRow;
		}
	}

	@Override
	public LazyObjectBase createValueObject(ObjectInspector inspector) throws SerDeException {
		return new LazyProtoBufObject(inspector);
	}

	/**
	 * Converts the given field to a lazy object
	 *
	 * @param field
	 *            to be converted to a lazy object
	 * @param fieldOI
	 *            {@link ObjectInspector} for the given field
	 * @return returns the converted lazy object
	 */
	private Object toLazyObject(Object field, ObjectInspector fieldOI) {
		if (isPrimitive(field.getClass())) {
			return toLazyPrimitiveObject(field, fieldOI);
		} else if (fieldOI instanceof LazyListObjectInspector) {
			return toLazyListObject(field, fieldOI);
		} else if (field instanceof StandardUnion) {
			return toLazyUnionObject(field, fieldOI);
		} else if (fieldOI instanceof LazyMapObjectInspector) {
			return toLazyMapObject(field, fieldOI);
		} else {
			return field;
		}
	}

	/**
	 * Convert the given object to a lazy object using the given
	 * {@link ObjectInspector}
	 *
	 * @param obj
	 *            Object to be converted to a {@link LazyObject}
	 * @param oi
	 *            ObjectInspector used for the conversion
	 * @return the created {@link LazyObject lazy object}
	 */
	private LazyObject<? extends ObjectInspector> toLazyPrimitiveObject(Object obj, ObjectInspector oi) {
		if (obj == null) {
			return null;
		}

		LazyObject<? extends ObjectInspector> lazyObject = LazyFactory.createLazyObject(oi);
		ByteArrayRef ref = new ByteArrayRef();

		String objAsString = obj.toString().trim();

		ref.setData(objAsString.getBytes());

		// initialize the lazy object
		lazyObject.init(ref, 0, ref.getData().length);

		return lazyObject;
	}

	/**
	 * Convert the given object to a lazy object using the given
	 * {@link ObjectInspector}
	 *
	 * @param obj
	 *            Object to be converted to a {@link LazyObject}
	 * @param oi
	 *            ObjectInspector used for the conversion
	 * @return the created {@link LazyObject lazy object}
	 */
	private Object toLazyListObject(Object obj, ObjectInspector objectInspector) {
		if (obj == null) {
			return null;
		}

		List<?> listObj = (List<?>) obj;

		LazyArray retList = (LazyArray) LazyFactory.createLazyObject(objectInspector);

		List<Object> lazyList = retList.getList();

		ObjectInspector listElementOI = ((ListObjectInspector) objectInspector).getListElementObjectInspector();

		for (int i = 0; i < listObj.size(); i++) {
			lazyList.add(toLazyObject(listObj.get(i), listElementOI));
		}

		return retList;
	}

	/**
	 * Convert the given object to a lazy object using the given
	 * {@link ObjectInspector}
	 *
	 * @param obj
	 *            Object to be converted to a {@link LazyObject}
	 * @param oi
	 *            ObjectInspector used for the conversion
	 * @return the created {@link LazyObject lazy object}
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object toLazyMapObject(Object obj, ObjectInspector objectInspector) {
		if (obj == null) {
			return null;
		}

		// avro guarantees that the key will be of type string. So we just need
		// to worry about
		// deserializing the value here

		LazyMap lazyMap = (LazyMap) LazyFactory.createLazyObject(objectInspector);

		Map map = lazyMap.getMap();

		Map<Object, Object> origMap = (Map) obj;

		ObjectInspector keyObjectInspector = ((MapObjectInspector) objectInspector).getMapKeyObjectInspector();
		ObjectInspector valueObjectInspector = ((MapObjectInspector) objectInspector).getMapValueObjectInspector();

		for (Entry entry : origMap.entrySet()) {
			Object value = entry.getValue();

			map.put(toLazyPrimitiveObject(entry.getKey(), keyObjectInspector),
					toLazyObject(value, valueObjectInspector));
		}

		return lazyMap;
	}

	/**
	 * Convert the given object to a lazy object using the given
	 * {@link ObjectInspector}
	 *
	 * @param obj
	 *            Object to be converted to a {@link LazyObject}
	 * @param oi
	 *            ObjectInspector used for the conversion
	 * @return the created {@link LazyObject lazy object}
	 */
	private Object toLazyUnionObject(Object obj, ObjectInspector objectInspector) {
		if (obj == null) {
			return null;
		}

		if (!(objectInspector instanceof LazyUnionObjectInspector)) {
			throw new IllegalArgumentException(
					"Invalid objectinspector found. Expected LazyUnionObjectInspector, Found "
							+ objectInspector.getClass());
		}

		StandardUnion standardUnion = (StandardUnion) obj;
		LazyUnionObjectInspector lazyUnionOI = (LazyUnionObjectInspector) objectInspector;

		// Grab the tag and the field
		byte tag = standardUnion.getTag();
		Object field = standardUnion.getObject();

		ObjectInspector fieldOI = lazyUnionOI.getObjectInspectors().get(tag);

		// convert to lazy object
		Object convertedObj = null;

		if (field != null) {
			convertedObj = toLazyObject(field, fieldOI);
		}

		if (convertedObj == null) {
			return null;
		}

		return new LazyUnion(lazyUnionOI, tag, convertedObj);
	}

	private boolean isPrimitive(Class<?> clazz) {
		return clazz.isPrimitive() || ClassUtils.wrapperToPrimitive(clazz) != null
				|| clazz.getSimpleName().equals("String");
	}
}
