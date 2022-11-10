/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.udf.generic;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * A Generic User-defined function (GenericUDF) for the use with Hive.
 *
 * New GenericUDF classes need to inherit from this GenericUDF class.
 *
 * The GenericUDF are superior to normal UDFs in the following ways: 1. It can
 * accept arguments of complex types, and return complex types. 2. It can accept
 * variable length of arguments. 3. It can accept an infinite number of function
 * signature - for example, it's easy to write a GenericUDF that accepts
 * array&lt;int&gt;, array&lt;array&lt;int&gt;&gt; and so on (arbitrary levels of nesting). 4. It
 * can do short-circuit evaluations using {@link DeferredObject}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@UDFType(deterministic = true)
public abstract class GenericUDF implements Closeable {

  private static final String[] ORDINAL_SUFFIXES = new String[] { "th", "st", "nd", "rd", "th",
      "th", "th", "th", "th", "th" };

  /**
   * A Deferred Object allows us to do lazy-evaluation and short-circuiting.
   * GenericUDF use {@link DeferredObject} to pass arguments.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public static interface DeferredObject {
    void prepare(int version) throws HiveException;
    Object get() throws HiveException;
  };

  /**
   * A basic dummy implementation of DeferredObject which just stores a Java
   * Object reference.
   */
  public static class DeferredJavaObject implements DeferredObject {
    private final Object value;

    public DeferredJavaObject(Object value) {
      this.value = value;
    }

    @Override
    public void prepare(int version) throws HiveException {
    }

    @Override
    public Object get() throws HiveException {
      return value;
    }
  }

  /**
   * The constructor.
   */
  public GenericUDF() {
  }

  /**
   * Initialize this GenericUDF. This will be called once and only once per
   * GenericUDF instance.
   *
   * @param arguments
   *          The ObjectInspector for the arguments
   * @throws UDFArgumentException
   *           Thrown when arguments have wrong types, wrong length, etc.
   * @return The ObjectInspector for the return value
   */
  public abstract ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException;

  /**
   * Additionally setup GenericUDF with MapredContext before initializing.
   * This is only called in runtime of MapRedTask.
   *
   * @param context context
   */
  public void configure(MapredContext context) {
  }

  /**
   * Initialize this GenericUDF.  Additionally, if the arguments are constant
   * and the function is eligible to be folded, then the constant value
   * returned by this UDF will be computed and stored in the
   * ConstantObjectInspector returned.  Otherwise, the function behaves exactly
   * like initialize().
   */
  public ObjectInspector initializeAndFoldConstants(ObjectInspector[] arguments)
      throws UDFArgumentException {

    ObjectInspector oi = initialize(arguments);

    // If the UDF depends on any external resources, we can't fold because the
    // resources may not be available at compile time.
    if (getRequiredFiles() != null ||
        getRequiredJars() != null) {
      return oi;
    }

    boolean allConstant = true;
    for (int ii = 0; ii < arguments.length; ++ii) {
      if (!ObjectInspectorUtils.isConstantObjectInspector(arguments[ii])) {
        allConstant = false;
        break;
      }
    }

    if (allConstant &&
        !ObjectInspectorUtils.isConstantObjectInspector(oi) &&
        FunctionRegistry.isConsistentWithinQuery(this) &&
        ObjectInspectorUtils.supportsConstantObjectInspector(oi)) {
      DeferredObject[] argumentValues =
        new DeferredJavaObject[arguments.length];
      for (int ii = 0; ii < arguments.length; ++ii) {
        argumentValues[ii] = new DeferredJavaObject(
            ((ConstantObjectInspector)arguments[ii]).getWritableConstantValue());
      }
      try {
        Object constantValue = evaluate(argumentValues);
        oi = ObjectInspectorUtils.getConstantObjectInspector(oi, constantValue);
      } catch (HiveException e) {
        throw new UDFArgumentException(e);
      }
    }
    return oi;
  }

  /**
   * The following two functions can be overridden to automatically include
   * additional resources required by this UDF.  The return types should be
   * arrays of paths.
   */
  public String[] getRequiredJars() {
    return null;
  }

  public String[] getRequiredFiles() {
    return null;
  }

  /**
   * Evaluate the GenericUDF with the arguments.
   *
   * @param arguments
   *          The arguments as DeferedObject, use DeferedObject.get() to get the
   *          actual argument Object. The Objects can be inspected by the
   *          ObjectInspectors passed in the initialize call.
   * @return The
   */
  public abstract Object evaluate(DeferredObject[] arguments)
      throws HiveException;

  /**
   * Get the String to be displayed in explain.
   */
  public abstract String getDisplayString(String[] children);

  /**
   * Close GenericUDF.
   * This is only called in runtime of MapRedTask.
   */
  @Override
  public void close() throws IOException {
  }

  /**
   * Some functions like comparisons may be affected by appearing order of arguments.
   * This is to convert a function, such as 3 &gt; x to x &lt; 3. The flip function of
   * GenericUDFOPGreaterThan is GenericUDFOPLessThan.
   */
  public GenericUDF flip() {
    return this;
  }

  /**
   * Gets the negative function of the current one. E.g., GenericUDFOPNotEqual for
   * GenericUDFOPEqual, or GenericUDFOPNull for GenericUDFOPNotNull.
   * @return Negative function
   */
  public GenericUDF negative() {
    throw new UnsupportedOperationException("Negative function doesn't exist for " + getFuncName());
  }

  public String getUdfName() {
    return getClass().getName();
  }

  /**
   * Some information may be set during initialize() which needs to be saved when the UDF is copied.
   * This will be called by FunctionRegistry.cloneGenericUDF()
   */
  public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
    // newInstance should always be the same type of object as this
    if (this.getClass() != newInstance.getClass()) {
      throw new UDFArgumentException("Invalid copy between " + this.getClass().getName()
          + " and " + newInstance.getClass().getName());
    }
  }

  protected String getStandardDisplayString(String name, String[] children) {
    return getStandardDisplayString(name, children, ", ");
  }

  protected String getStandardDisplayString(String name, String[] children, String delim) {
    StringBuilder sb = new StringBuilder();
    sb.append(name);
    sb.append("(");
    if (children.length > 0) {
      sb.append(children[0]);
      for (int i = 1; i < children.length; i++) {
        sb.append(delim);
        sb.append(children[i]);
      }
    }
    sb.append(")");
    return sb.toString();
  }

  protected String getFuncName() {
    return getClass().getSimpleName().substring(10).toLowerCase();
  }

  protected void checkArgsSize(ObjectInspector[] arguments, int min, int max)
      throws UDFArgumentLengthException {
    if (arguments.length < min || arguments.length > max) {
      StringBuilder sb = new StringBuilder();
      sb.append(getFuncName());
      sb.append(" requires ");
      if (min == max) {
        sb.append(min);
      } else {
        sb.append(min).append("..").append(max);
      }
      sb.append(" argument(s), got ");
      sb.append(arguments.length);
      throw new UDFArgumentLengthException(sb.toString());
    }
  }

  protected void checkArgPrimitive(ObjectInspector[] arguments, int i)
      throws UDFArgumentTypeException {
    ObjectInspector.Category oiCat = arguments[i].getCategory();
    if (oiCat != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(i, getFuncName() + " only takes primitive types as "
          + getArgOrder(i) + " argument, got " + oiCat);
    }
  }

  protected void checkArgGroups(ObjectInspector[] arguments, int i, PrimitiveCategory[] inputTypes,
      PrimitiveGrouping... grps) throws UDFArgumentTypeException {
    PrimitiveCategory inputType = ((PrimitiveObjectInspector) arguments[i]).getPrimitiveCategory();
    for (PrimitiveGrouping grp : grps) {
      if (PrimitiveObjectInspectorUtils.getPrimitiveGrouping(inputType) == grp) {
        inputTypes[i] = inputType;
        return;
      }
    }
    // build error message
    StringBuilder sb = new StringBuilder();
    sb.append(getFuncName());
    sb.append(" only takes ");
    sb.append(grps[0]);
    for (int j = 1; j < grps.length; j++) {
      sb.append(", ");
      sb.append(grps[j]);
    }
    sb.append(" types as ");
    sb.append(getArgOrder(i));
    sb.append(" argument, got ");
    sb.append(inputType);
    throw new UDFArgumentTypeException(i, sb.toString());
  }

  protected void obtainStringConverter(ObjectInspector[] arguments, int i,
      PrimitiveCategory[] inputTypes, Converter[] converters) throws UDFArgumentTypeException {
    PrimitiveObjectInspector inOi = (PrimitiveObjectInspector) arguments[i];
    PrimitiveCategory inputType = inOi.getPrimitiveCategory();

    Converter converter = ObjectInspectorConverters.getConverter(
        arguments[i],
        PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    converters[i] = converter;
    inputTypes[i] = inputType;
  }

  protected void obtainIntConverter(ObjectInspector[] arguments, int i,
      PrimitiveCategory[] inputTypes, Converter[] converters) throws UDFArgumentTypeException {
    PrimitiveObjectInspector inOi = (PrimitiveObjectInspector) arguments[i];
    PrimitiveCategory inputType = inOi.getPrimitiveCategory();
    switch (inputType) {
    case BYTE:
    case SHORT:
    case INT:
    case VOID:
      break;
    default:
      throw new UDFArgumentTypeException(i, getFuncName() + " only takes INT/SHORT/BYTE types as "
          + getArgOrder(i) + " argument, got " + inputType);
    }

    Converter converter = ObjectInspectorConverters.getConverter(
        arguments[i],
        PrimitiveObjectInspectorFactory.writableIntObjectInspector);
    converters[i] = converter;
    inputTypes[i] = inputType;
  }

  protected void obtainLongConverter(ObjectInspector[] arguments, int i,
      PrimitiveCategory[] inputTypes, Converter[] converters) throws UDFArgumentTypeException {
    PrimitiveObjectInspector inOi = (PrimitiveObjectInspector) arguments[i];
    PrimitiveCategory inputType = inOi.getPrimitiveCategory();
    switch (inputType) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case VOID:
      break;
    default:
      throw new UDFArgumentTypeException(i, getFuncName()
          + " only takes LONG/INT/SHORT/BYTE types as " + getArgOrder(i) + " argument, got "
          + inputType);
    }

    Converter converter = ObjectInspectorConverters.getConverter(
        arguments[i],
        PrimitiveObjectInspectorFactory.writableLongObjectInspector);
    converters[i] = converter;
    inputTypes[i] = inputType;
  }

  protected void obtainDoubleConverter(ObjectInspector[] arguments, int i,
      PrimitiveCategory[] inputTypes, Converter[] converters) throws UDFArgumentTypeException {
    PrimitiveObjectInspector inOi = (PrimitiveObjectInspector) arguments[i];
    PrimitiveCategory inputType = inOi.getPrimitiveCategory();
    Converter converter = ObjectInspectorConverters.getConverter(
        arguments[i],
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
    converters[i] = converter;
    inputTypes[i] = inputType;
  }

  protected void obtainDateConverter(ObjectInspector[] arguments, int i,
      PrimitiveCategory[] inputTypes, Converter[] converters) throws UDFArgumentTypeException {
    PrimitiveObjectInspector inOi = (PrimitiveObjectInspector) arguments[i];
    PrimitiveCategory inputType = inOi.getPrimitiveCategory();
    ObjectInspector outOi;
    switch (inputType) {
    case STRING:
    case VARCHAR:
    case CHAR:
    case TIMESTAMP:
    case DATE:
    case VOID:
    case TIMESTAMPLOCALTZ:
      break;
    default:
      throw new UDFArgumentTypeException(i, getFuncName()
          + " only takes STRING_GROUP/DATE_GROUP/VOID_GROUP types as " + getArgOrder(i) + " argument, got "
          + inputType);
    }
    outOi = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    converters[i] = ObjectInspectorConverters.getConverter(inOi, outOi);
    inputTypes[i] = inputType;
  }

  protected void obtainTimestampConverter(ObjectInspector[] arguments, int i,
      PrimitiveCategory[] inputTypes, Converter[] converters) throws UDFArgumentTypeException {
    PrimitiveObjectInspector inOi = (PrimitiveObjectInspector) arguments[i];
    PrimitiveCategory inputType = inOi.getPrimitiveCategory();
    ObjectInspector outOi;
    switch (inputType) {
    case STRING:
    case VARCHAR:
    case CHAR:
    case TIMESTAMP:
    case DATE:
    case TIMESTAMPLOCALTZ:
    case INT:
    case SHORT:
    case LONG:
    case DOUBLE:
    case FLOAT:
    case DECIMAL:
    case VOID:
    case BOOLEAN:
    case BYTE:
      break;
    default:
      throw new UDFArgumentTypeException(i, getFuncName()
          + " only takes STRING_GROUP/DATE_GROUP/NUMERIC_GROUP/VOID_GROUP/BOOLEAN_GROUP types as " + getArgOrder(i) + " argument, got "
          + inputType);
    }
    outOi = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    converters[i] = ObjectInspectorConverters.getConverter(inOi, outOi);
    inputTypes[i] = inputType;
  }

  protected String getStringValue(DeferredObject[] arguments, int i, Converter[] converters)
      throws HiveException {
    Object obj;
    if ((obj = arguments[i].get()) == null) {
      return null;
    }
    return converters[i].convert(obj).toString();
  }

  protected Integer getIntValue(DeferredObject[] arguments, int i, Converter[] converters)
      throws HiveException {
    Object obj;
    if ((obj = arguments[i].get()) == null) {
      return null;
    }
    Object writableValue = converters[i].convert(obj);
    int v = ((IntWritable) writableValue).get();
    return v;
  }

  protected Long getLongValue(DeferredObject[] arguments, int i, Converter[] converters)
      throws HiveException {
    Object obj;
    if ((obj = arguments[i].get()) == null) {
      return null;
    }
    Object writableValue = converters[i].convert(obj);
    long v = ((LongWritable) writableValue).get();
    return v;
  }

  protected Double getDoubleValue(DeferredObject[] arguments, int i, Converter[] converters)
      throws HiveException {
    Object obj;
    if ((obj = arguments[i].get()) == null) {
      return null;
    }
    Object writableValue = converters[i].convert(obj);
    double v = ((DoubleWritable) writableValue).get();
    return v;
  }

  protected Date getDateValue(DeferredObject[] arguments, int i, Converter[] converters)
      throws HiveException {
    Object obj;
    if ((obj = arguments[i].get()) == null) {
      return null;
    }
    Object writableValue = converters[i].convert(obj);

    return writableValue == null ? null : ((DateWritableV2) writableValue).get();
  }

  protected Timestamp getTimestampValue(DeferredObject[] arguments, int i, Converter[] converters)
      throws HiveException {
    Object obj;
    if ((obj = arguments[i].get()) == null) {
      return null;
    }
    Object writableValue = converters[i].convert(obj);
    // if string can not be parsed converter will return null
    if (writableValue == null) {
      return null;
    }
    Timestamp ts = ((TimestampWritableV2) writableValue).getTimestamp();
    return ts;
  }

  protected HiveIntervalYearMonth getIntervalYearMonthValue(DeferredObject[] arguments, int i, PrimitiveCategory[] inputTypes,
      Converter[] converters) throws HiveException {
    Object obj;
    if ((obj = arguments[i].get()) == null) {
      return null;
    }

    HiveIntervalYearMonth intervalYearMonth;
    switch (inputTypes[i]) {
      case STRING:
      case VARCHAR:
      case CHAR:
        String intervalYearMonthStr = converters[i].convert(obj).toString();
        intervalYearMonth = HiveIntervalYearMonth.valueOf(intervalYearMonthStr);
        break;
      case INTERVAL_YEAR_MONTH:
        Object writableValue = converters[i].convert(obj);
        intervalYearMonth = ((HiveIntervalYearMonthWritable) writableValue).getHiveIntervalYearMonth();
        break;
      default:
        throw new UDFArgumentTypeException(0, getFuncName()
            + " only takes INTERVAL_YEAR_MONTH and STRING_GROUP types, got " + inputTypes[i]);
    }
    return intervalYearMonth;
  }

  protected HiveIntervalDayTime getIntervalDayTimeValue(DeferredObject[] arguments, int i, PrimitiveCategory[] inputTypes,
      Converter[] converters) throws HiveException {
    Object obj;
    if ((obj = arguments[i].get()) == null) {
      return null;
    }

    HiveIntervalDayTime intervalDayTime;
    switch (inputTypes[i]) {
      case STRING:
      case VARCHAR:
      case CHAR:
        String intervalDayTimeStr = converters[i].convert(obj).toString();
        intervalDayTime = HiveIntervalDayTime.valueOf(intervalDayTimeStr);
        break;
      case INTERVAL_DAY_TIME:
        Object writableValue = converters[i].convert(obj);
        intervalDayTime = ((HiveIntervalDayTimeWritable) writableValue).getHiveIntervalDayTime();
        break;
      default:
        throw new UDFArgumentTypeException(0, getFuncName()
            + " only takes INTERVAL_DAY_TIME and STRING_GROUP types, got " + inputTypes[i]);
    }
    return intervalDayTime;
  }

  protected String getConstantStringValue(ObjectInspector[] arguments, int i) {
    Object constValue = ((ConstantObjectInspector) arguments[i]).getWritableConstantValue();
    String str = constValue == null ? null : constValue.toString();
    return str;
  }

  protected Boolean getConstantBooleanValue(ObjectInspector[] arguments, int i)
      throws UDFArgumentTypeException {
    Object constValue = ((ConstantObjectInspector) arguments[i]).getWritableConstantValue();
    if (constValue == null) {
      return false;
    }
    if (constValue instanceof BooleanWritable) {
      return ((BooleanWritable) constValue).get();
    } else {
      throw new UDFArgumentTypeException(i, getFuncName() + " only takes BOOLEAN types as "
          + getArgOrder(i) + " argument, got " + constValue.getClass());
    }
  }

  protected Integer getConstantIntValue(ObjectInspector[] arguments, int i)
      throws UDFArgumentTypeException {
    Object constValue = ((ConstantObjectInspector) arguments[i]).getWritableConstantValue();
    if (constValue == null) {
      return null;
    }
    int v;
    if (constValue instanceof IntWritable) {
      v = ((IntWritable) constValue).get();
    } else if (constValue instanceof ShortWritable) {
      v = ((ShortWritable) constValue).get();
    } else if (constValue instanceof ByteWritable) {
      v = ((ByteWritable) constValue).get();
    } else {
      throw new UDFArgumentTypeException(i, getFuncName() + " only takes INT/SHORT/BYTE types as "
          + getArgOrder(i) + " argument, got " + constValue.getClass());
    }
    return v;
  }

  protected String getArgOrder(int i) {
    i++;
    switch (i % 100) {
    case 11:
    case 12:
    case 13:
      return i + "th";
    default:
      return i + ORDINAL_SUFFIXES[i % 10];
    }
  }

  @SuppressWarnings("unchecked")
  public <T> Optional<T> adapt(Class<T> clazz) {
    if (clazz.isInstance(this)) {
      return Optional.of((T) this);
    }
    return Optional.empty();
  }
}
