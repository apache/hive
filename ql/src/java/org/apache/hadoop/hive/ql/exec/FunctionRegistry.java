/**
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

package org.apache.hadoop.hive.ql.exec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.groupByDesc;
import org.apache.hadoop.hive.ql.udf.*;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.util.ReflectionUtils;

public class FunctionRegistry {

  private static Log LOG = LogFactory.getLog("org.apache.hadoop.hive.ql.exec.FunctionRegistry");

  /**
   * The mapping from expression function names to expression classes.
   */
  static LinkedHashMap<String, FunctionInfo> mFunctions;
  static {
    mFunctions = new LinkedHashMap<String, FunctionInfo>();
    registerUDF("concat", UDFConcat.class, false);
    registerUDF("substr", UDFSubstr.class, false);
    registerUDF("substring", UDFSubstr.class, false);
    registerUDF("space", UDFSpace.class, false);
    registerUDF("repeat", UDFRepeat.class, false);
    registerUDF("ascii", UDFAscii.class, false);
    registerUDF("lpad", UDFLpad.class, false);
    registerUDF("rpad", UDFRpad.class, false);
    
    registerGenericUDF("size", GenericUDFSize.class);

    registerUDF("round", UDFRound.class, false);
    registerUDF("floor", UDFFloor.class, false);
    registerUDF("sqrt", UDFSqrt.class, false);
    registerUDF("ceil", UDFCeil.class, false);
    registerUDF("ceiling", UDFCeil.class, false);
    registerUDF("rand", UDFRand.class, false);
    registerUDF("abs", UDFAbs.class, false);
    registerUDF("pmod", UDFPosMod.class, false);

    registerUDF("ln", UDFLn.class, false);
    registerUDF("log2", UDFLog2.class, false);
    registerUDF("sin",UDFSin.class, false);
    registerUDF("asin",UDFAsin.class, false);
    registerUDF("cos",UDFCos.class, false);
    registerUDF("acos",UDFAcos.class, false);
    registerUDF("log10", UDFLog10.class, false);
    registerUDF("log", UDFLog.class, false);
    registerUDF("exp", UDFExp.class, false);
    registerUDF("power", UDFPower.class, false);
    registerUDF("pow", UDFPower.class, false);

    registerUDF("conv", UDFConv.class, false);
    registerUDF("bin", UDFBin.class, false);
    registerUDF("hex", UDFHex.class, false);
    registerUDF("unhex", UDFUnhex.class, false);
    
    registerUDF("upper", UDFUpper.class, false);
    registerUDF("lower", UDFLower.class, false);
    registerUDF("ucase", UDFUpper.class, false);
    registerUDF("lcase", UDFLower.class, false);
    registerUDF("trim", UDFTrim.class, false);
    registerUDF("ltrim", UDFLTrim.class, false);
    registerUDF("rtrim", UDFRTrim.class, false);
    registerUDF("length", UDFLength.class, false);
    registerUDF("reverse", UDFReverse.class, false);
    registerGenericUDF("field", GenericUDFField.class);
    registerUDF("find_in_set", UDFFindInSet.class, false);
    
    registerUDF("like", UDFLike.class, true);
    registerUDF("rlike", UDFRegExp.class, true);
    registerUDF("regexp", UDFRegExp.class, true);
    registerUDF("regexp_replace", UDFRegExpReplace.class, false);
    registerUDF("regexp_extract", UDFRegExpExtract.class, false);
    registerUDF("parse_url", UDFParseUrl.class, false);
    registerGenericUDF("split", GenericUDFSplit.class);

    registerUDF("positive", UDFOPPositive.class, true, "+");
    registerUDF("negative", UDFOPNegative.class, true, "-");

    registerUDF("day", UDFDayOfMonth.class, false);
    registerUDF("dayofmonth", UDFDayOfMonth.class, false);
    registerUDF("month", UDFMonth.class, false);
    registerUDF("year", UDFYear.class, false);
    registerUDF("hour", UDFHour.class, false);
    registerUDF("minute", UDFMinute.class, false);
    registerUDF("second", UDFSecond.class, false);
    registerUDF("from_unixtime", UDFFromUnixTime.class, false);
    registerUDF("unix_timestamp", UDFUnixTimeStamp.class, false);
    registerUDF("to_date", UDFDate.class, false);
    registerUDF("weekofyear", UDFWeekOfYear.class, false);
    
    registerUDF("date_add", UDFDateAdd.class, false);
    registerUDF("date_sub", UDFDateSub.class, false);
    registerUDF("datediff", UDFDateDiff.class, false);

    registerUDF("get_json_object", UDFJson.class, false);

    registerUDF("+", UDFOPPlus.class, true);
    registerUDF("-", UDFOPMinus.class, true);
    registerUDF("*", UDFOPMultiply.class, true);
    registerUDF("/", UDFOPDivide.class, true);
    registerUDF("%", UDFOPMod.class, true);
    registerUDF("div", UDFOPLongDivide.class, true);

    registerUDF("&", UDFOPBitAnd.class, true);
    registerUDF("|", UDFOPBitOr.class, true);
    registerUDF("^", UDFOPBitXor.class, true);
    registerUDF("~", UDFOPBitNot.class, true);

    registerUDF("=", UDFOPEqual.class, true);
    registerUDF("==", UDFOPEqual.class, true, "=");
    registerUDF("<>", UDFOPNotEqual.class, true);
    registerUDF("<", UDFOPLessThan.class, true);
    registerUDF("<=", UDFOPEqualOrLessThan.class, true);
    registerUDF(">", UDFOPGreaterThan.class, true);
    registerUDF(">=", UDFOPEqualOrGreaterThan.class, true);

    registerUDF("and", UDFOPAnd.class, true);
    registerUDF("or", UDFOPOr.class, true);
    registerUDF("not", UDFOPNot.class, true);
    registerUDF("!", UDFOPNot.class, true, "not");

    registerGenericUDF("isnull", GenericUDFOPNull.class);
    registerGenericUDF("isnotnull", GenericUDFOPNotNull.class);

    registerGenericUDF("if", GenericUDFIf.class);
    
    // Aliases for Java Class Names
    // These are used in getImplicitConvertUDFMethod
    registerUDF(Constants.BOOLEAN_TYPE_NAME, UDFToBoolean.class, false,
                UDFToBoolean.class.getSimpleName());
    registerUDF(Constants.TINYINT_TYPE_NAME, UDFToByte.class, false,
                UDFToByte.class.getSimpleName());
    registerUDF(Constants.SMALLINT_TYPE_NAME, UDFToShort.class, false,
                UDFToShort.class.getSimpleName());
    registerUDF(Constants.INT_TYPE_NAME, UDFToInteger.class, false,
                UDFToInteger.class.getSimpleName());
    registerUDF(Constants.BIGINT_TYPE_NAME, UDFToLong.class, false,
                UDFToLong.class.getSimpleName());
    registerUDF(Constants.FLOAT_TYPE_NAME, UDFToFloat.class, false,
                UDFToFloat.class.getSimpleName());
    registerUDF(Constants.DOUBLE_TYPE_NAME, UDFToDouble.class, false,
                UDFToDouble.class.getSimpleName());
    registerUDF(Constants.STRING_TYPE_NAME, UDFToString.class, false,
                UDFToString.class.getSimpleName());

    // Aggregate functions
    registerGenericUDAF("sum", new GenericUDAFSum());
    registerGenericUDAF("count", new GenericUDAFCount());
    registerGenericUDAF("avg", new GenericUDAFAverage());
    
    registerGenericUDAF("std", new GenericUDAFStd());
    registerGenericUDAF("stddev", new GenericUDAFStd());
    registerGenericUDAF("stddev_pop", new GenericUDAFStd());
    registerGenericUDAF("stddev_samp", new GenericUDAFStdSample());
    registerGenericUDAF("variance", new GenericUDAFVariance());
    registerGenericUDAF("var_pop", new GenericUDAFVariance());
    registerGenericUDAF("var_samp", new GenericUDAFVarianceSample());
    
    registerUDAF("max", UDAFMax.class);
    registerUDAF("min", UDAFMin.class);
    
    // Generic UDFs
    registerGenericUDF("array", GenericUDFArray.class);
    registerGenericUDF("map", GenericUDFMap.class);

    registerGenericUDF("case", GenericUDFCase.class);
    registerGenericUDF("when", GenericUDFWhen.class);
    registerGenericUDF("hash", GenericUDFHash.class);
    registerGenericUDF("coalesce", GenericUDFCoalesce.class);
    registerGenericUDF("index", GenericUDFIndex.class);
    registerGenericUDF("instr", GenericUDFInstr.class);
    registerGenericUDF("locate", GenericUDFLocate.class);
    registerGenericUDF("elt", GenericUDFElt.class);
    registerGenericUDF("concat_ws", GenericUDFConcatWS.class);
    
    // Generic UDTF's
    registerGenericUDTF("explode", GenericUDTFExplode.class);
  }

  public static void registerTemporaryUDF(String functionName, Class<? extends UDF> UDFClass,
      boolean isOperator) {
    registerUDF(false, functionName, UDFClass, isOperator);
  }

  static void registerUDF(String functionName, Class<? extends UDF> UDFClass,
                                 boolean isOperator) {
    registerUDF(true, functionName, UDFClass, isOperator);
  }

  public static void registerUDF(boolean isNative, String functionName, Class<? extends UDF> UDFClass,
      boolean isOperator) {
    registerUDF(isNative, functionName, UDFClass, isOperator, functionName.toLowerCase());
  }

  public static void registerUDF(String functionName, Class<? extends UDF> UDFClass,
      boolean isOperator, String displayName) {
    registerUDF(true, functionName, UDFClass, isOperator, displayName);
  }
  
  public static void registerUDF(boolean isNative, String functionName, Class<? extends UDF> UDFClass,
                                 boolean isOperator, String displayName) {
    if (UDF.class.isAssignableFrom(UDFClass)) {
      FunctionInfo fI = new FunctionInfo(isNative, displayName, 
          new GenericUDFBridge(displayName, isOperator, UDFClass));
      mFunctions.put(functionName.toLowerCase(), fI);
    } else {
      throw new RuntimeException("Registering UDF Class " + UDFClass + " which does not extend " + UDF.class);
    }
  }

  public static void registerTemporaryGenericUDF(String functionName, Class<? extends GenericUDF> genericUDFClass) {
    registerGenericUDF(false, functionName, genericUDFClass);
  }

  static void registerGenericUDF(String functionName, Class<? extends GenericUDF> genericUDFClass) {
    registerGenericUDF(true, functionName, genericUDFClass);
  }

  public static void registerGenericUDF(boolean isNative, String functionName, Class<? extends GenericUDF> genericUDFClass) {
    if (GenericUDF.class.isAssignableFrom(genericUDFClass)) {
      FunctionInfo fI = new FunctionInfo(isNative, functionName, 
          (GenericUDF)ReflectionUtils.newInstance(genericUDFClass, null));
      mFunctions.put(functionName.toLowerCase(), fI);
    } else {
      throw new RuntimeException("Registering GenericUDF Class " + genericUDFClass
          + " which does not extend " + GenericUDF.class);
    }
  }

  public static void registerTemporaryGenericUDTF(String functionName, Class<? extends GenericUDTF> genericUDTFClass) {
    registerGenericUDTF(false, functionName, genericUDTFClass);
  }
  static void registerGenericUDTF(String functionName, Class<? extends GenericUDTF> genericUDTFClass) {
    registerGenericUDTF(true, functionName, genericUDTFClass);
  }

  public static void registerGenericUDTF(boolean isNative, String functionName, Class<? extends GenericUDTF> genericUDTFClass) {
    if (GenericUDTF.class.isAssignableFrom(genericUDTFClass)) {
      FunctionInfo fI = new FunctionInfo(isNative, functionName, 
          (GenericUDTF)ReflectionUtils.newInstance(genericUDTFClass, null));
      mFunctions.put(functionName.toLowerCase(), fI);
    } else {
      throw new RuntimeException("Registering GenericUDTF Class " + genericUDTFClass
          + " which does not extend " + GenericUDTF.class);
    }
  }
  
  public static FunctionInfo getFunctionInfo(String functionName) {
    return mFunctions.get(functionName.toLowerCase());
  }

  /**
   * Returns a set of registered function names.
   * This is used for the CLI command "SHOW FUNCTIONS;"
   * @return      set of strings contains function names
   */
  public static Set<String> getFunctionNames() {
    return mFunctions.keySet();
  }

  /**
   * Returns a set of registered function names.
   * This is used for the CLI command "SHOW FUNCTIONS 'regular expression';"
   * Returns an empty set when the regular expression is not valid.
   * @param  funcPatternStr  regular expression of the intersted function names
   * @return                 set of strings contains function names
   */
  public static Set<String> getFunctionNames(String funcPatternStr) {
    TreeSet<String> funcNames = new TreeSet<String>();
    Pattern funcPattern = null;
    try {
      funcPattern = Pattern.compile(funcPatternStr);
    } catch (PatternSyntaxException e) {
      return funcNames;
    }
    for (String funcName : mFunctions.keySet()) {
      if (funcPattern.matcher(funcName).matches()) {
        funcNames.add(funcName);
      }
    }
    return funcNames;
  }

  static Map<TypeInfo, Integer> numericTypes = new HashMap<TypeInfo, Integer>();
  static List<TypeInfo> numericTypeList = new ArrayList<TypeInfo>();
  static void registerNumericType(String typeName, int level) {
    TypeInfo t = TypeInfoFactory.getPrimitiveTypeInfo(typeName);
    numericTypeList.add(t);
    numericTypes.put(t, level); 
  }
  static {
    registerNumericType(Constants.TINYINT_TYPE_NAME, 1);
    registerNumericType(Constants.SMALLINT_TYPE_NAME, 2);
    registerNumericType(Constants.INT_TYPE_NAME, 3);
    registerNumericType(Constants.BIGINT_TYPE_NAME, 4);
    registerNumericType(Constants.FLOAT_TYPE_NAME, 5);
    registerNumericType(Constants.DOUBLE_TYPE_NAME, 6);
    registerNumericType(Constants.STRING_TYPE_NAME, 7);
  }

  /**
   * Find a common class that objects of both TypeInfo a and TypeInfo b can convert to.
   * This is used for comparing objects of type a and type b.
   * 
   * When we are comparing string and double, we will always convert both of them
   * to double and then compare.
   * 
   * @return null if no common class could be found.
   */
  public static TypeInfo getCommonClassForComparison(TypeInfo a, TypeInfo b) {
    // If same return one of them
    if (a.equals(b)) return a;
    
    for (TypeInfo t: numericTypeList) {
      if (FunctionRegistry.implicitConvertable(a, t) &&
          FunctionRegistry.implicitConvertable(b, t)) {
        return t;
      }
    }
    return null;
  }

  /**
   * Find a common class that objects of both TypeInfo a and TypeInfo b can convert to.
   * This is used for places other than comparison.
   * 
   * The common class of string and double is string.
   * 
   * @return null if no common class could be found.
   */
  public static TypeInfo getCommonClass(TypeInfo a, TypeInfo b) {
    Integer ai = numericTypes.get(a);
    Integer bi = numericTypes.get(b);
    if (ai == null || bi == null) {
      // If either is not a numeric type, return null.
      return null;
    }
    return (ai > bi) ? a : b;
  }

  /** Returns whether it is possible to implicitly convert an object of Class from to Class to.
   */
  public static boolean implicitConvertable(TypeInfo from, TypeInfo to) {
    if (from.equals(to)) {
      return true;
    }
    // Allow implicit String to Double conversion
    if (from.equals(TypeInfoFactory.stringTypeInfo)
        && to.equals(TypeInfoFactory.doubleTypeInfo)) {
      return true;
    }
    // Void can be converted to any type
    if (from.equals(TypeInfoFactory.voidTypeInfo)) {
      return true;
    }

    // Allow implicit conversion from Byte -> Integer -> Long -> Float -> Double -> String
    Integer f = numericTypes.get(from);
    Integer t = numericTypes.get(to);
    if (f == null || t == null) return false;
    if (f.intValue() > t.intValue()) return false;
    return true;
  }

  /**
   * Get the GenericUDAF evaluator for the name and argumentClasses.
   * @param name the name of the UDAF
   * @param argumentTypeInfos
   * @return The UDAF evaluator
   */
  public static GenericUDAFEvaluator getGenericUDAFEvaluator(String name, List<TypeInfo> argumentTypeInfos) 
      throws SemanticException {
    GenericUDAFResolver udaf = getGenericUDAFResolver(name);
    if (udaf == null) return null;

    TypeInfo[] parameters = new TypeInfo[argumentTypeInfos.size()];
    for(int i=0; i<parameters.length; i++) {
      parameters[i] = argumentTypeInfos.get(i);
    }
    return udaf.getEvaluator(parameters);
  }

  /**
   * This method is shared between UDFRegistry and UDAFRegistry.
   * methodName will be "evaluate" for UDFRegistry, and "aggregate"/"evaluate"/"evaluatePartial" for UDAFRegistry.
   */
  public static <T> Method getMethodInternal(Class<? extends T> udfClass, String methodName, boolean exact, 
      List<TypeInfo> argumentClasses) {

    ArrayList<Method> mlist = new ArrayList<Method>();

    for(Method m: Arrays.asList(udfClass.getMethods())) {
      if (m.getName().equals(methodName)) {
        mlist.add(m);
      }
    }

    return getMethodInternal(mlist, exact, argumentClasses);
  }

  public static void registerTemporaryGenericUDAF(String functionName, GenericUDAFResolver genericUDAFResolver) {
    registerGenericUDAF(false, functionName, genericUDAFResolver);
  }

  static void registerGenericUDAF(String functionName, GenericUDAFResolver genericUDAFResolver) {
    registerGenericUDAF(true, functionName, genericUDAFResolver);
  }

  public static void registerGenericUDAF(boolean isNative, String functionName, GenericUDAFResolver genericUDAFResolver) {
    mFunctions.put(functionName.toLowerCase(), 
        new FunctionInfo(isNative, functionName.toLowerCase(), genericUDAFResolver));
  }

  public static void registerTemporaryUDAF(String functionName, Class<? extends UDAF> udafClass) {
    registerUDAF(false, functionName, udafClass);
  }

  static void registerUDAF(String functionName, Class<? extends UDAF> udafClass) {
    registerUDAF(true, functionName, udafClass);
  }

  public static void registerUDAF(boolean isNative, String functionName, Class<? extends UDAF> udafClass) {
    mFunctions.put(functionName.toLowerCase(), 
        new FunctionInfo(isNative, functionName.toLowerCase(), 
            new GenericUDAFBridge((UDAF)ReflectionUtils.newInstance(udafClass, null))));
  }

  public static void unregisterTemporaryUDF(String functionName) throws HiveException {
    FunctionInfo fi = mFunctions.get(functionName.toLowerCase());
    if(fi != null) {
      if(!fi.isNative())
        mFunctions.remove(functionName.toLowerCase());
      else
        throw new HiveException("Function " + functionName 
            + " is hive native, it can't be dropped");
    }
  }

  public static GenericUDAFResolver getGenericUDAFResolver(String functionName) {
    LOG.debug("Looking up GenericUDAF: " + functionName);
    FunctionInfo finfo = mFunctions.get(functionName.toLowerCase());
    if (finfo == null) {
      return null;
    }
    GenericUDAFResolver result = finfo.getGenericUDAFResolver();
    return result;
  }

  public static Object invoke(Method m, Object thisObject, Object... arguments) throws HiveException {
    Object o;
    try {
      o = m.invoke(thisObject, arguments);
    } catch (Exception e) {
      String thisObjectString = "" + thisObject + " of class " +
        (thisObject == null? "null" : thisObject.getClass().getName());

      StringBuilder argumentString = new StringBuilder();
      if (arguments == null) {
        argumentString.append("null");
      } else {
        argumentString.append("{");
        for (int i=0; i<arguments.length; i++) {
          if (i>0) {
            argumentString.append(", ");
          }
          if (arguments[i] == null) {
            argumentString.append("null");
          } else {
            argumentString.append("" + arguments[i] + ":" + arguments[i].getClass().getName());
          }
        }
        argumentString.append("} of size " + arguments.length);
      }

      throw new HiveException("Unable to execute method " + m + " "
          + " on object " + thisObjectString
          + " with arguments " + argumentString.toString(), e);
    }
    return o;
  }

  /**
   * Returns -1 if passed does not match accepted.
   * Otherwise return the cost (usually 0 for no conversion and 1 for conversion).
   */
  public static int matchCost(TypeInfo argumentPassed, TypeInfo argumentAccepted, boolean exact) {
    if (argumentAccepted.equals(argumentPassed)) {
      // matches
      return 0;
    }
    if (argumentPassed.equals(TypeInfoFactory.voidTypeInfo)) {
      // passing null matches everything
      return 0;
    }
    if (argumentPassed.getCategory().equals(Category.LIST) 
        && argumentAccepted.getCategory().equals(Category.LIST)) {
      // lists are compatible if and only-if the elements are compatible
      TypeInfo argumentPassedElement = ((ListTypeInfo)argumentPassed).getListElementTypeInfo();
      TypeInfo argumentAcceptedElement = ((ListTypeInfo)argumentAccepted).getListElementTypeInfo();
      return matchCost(argumentPassedElement, argumentAcceptedElement, exact);
    }
    if (argumentPassed.getCategory().equals(Category.MAP) 
        && argumentAccepted.getCategory().equals(Category.MAP)) {
      // lists are compatible if and only-if the elements are compatible
      TypeInfo argumentPassedKey = ((MapTypeInfo)argumentPassed).getMapKeyTypeInfo();
      TypeInfo argumentAcceptedKey = ((MapTypeInfo)argumentAccepted).getMapKeyTypeInfo();
      TypeInfo argumentPassedValue = ((MapTypeInfo)argumentPassed).getMapValueTypeInfo();
      TypeInfo argumentAcceptedValue = ((MapTypeInfo)argumentAccepted).getMapValueTypeInfo();
      int cost1 = matchCost(argumentPassedKey, argumentAcceptedKey, exact);
      int cost2 = matchCost(argumentPassedValue, argumentAcceptedValue, exact);
      if (cost1 < 0 || cost2 < 0) return -1;
      return Math.max(cost1, cost2);
    }
    
    if (argumentAccepted.equals(TypeInfoFactory.unknownTypeInfo)) {
      // accepting Object means accepting everything,
      // but there is a conversion cost.
      return 1;
    }
    if (!exact && implicitConvertable(argumentPassed, argumentAccepted)) {
      return 1;
    }
    
    return -1;
  }
  
  /**
   * Gets the closest matching method corresponding to the argument list from a list of methods.
   *
   * @param mlist The list of methods to inspect.
   * @param exact Boolean to indicate whether this is an exact match or not.
   * @param argumentsPassed The classes for the argument.
   * @return The matching method.
   */
  public static Method getMethodInternal(ArrayList<Method> mlist, boolean exact,
      List<TypeInfo> argumentsPassed) {
    int leastConversionCost = Integer.MAX_VALUE;
    Method udfMethod = null;

    for (Method m: mlist) {
      List<TypeInfo> argumentsAccepted = TypeInfoUtils.getParameterTypeInfos(m,
          argumentsPassed.size());
      if (argumentsAccepted == null) {
        // null means the method does not accept number of arguments passed.
        continue;
      }
      
      boolean match = (argumentsAccepted.size() == argumentsPassed.size());
      int conversionCost = 0;

      for(int i=0; i<argumentsPassed.size() && match; i++) {
        int cost = matchCost(argumentsPassed.get(i), argumentsAccepted.get(i), exact);
        if (cost == -1) {
          match = false;
        } else {
          conversionCost += cost;
        }
      }
      LOG.debug("Method " + (match ? "did": "didn't") + " match: passed = " + argumentsPassed
          + " accepted = " + argumentsAccepted + " method = " + m);
      if (match) {
        // Always choose the function with least implicit conversions.
        if (conversionCost < leastConversionCost) {
          udfMethod = m;
          leastConversionCost = conversionCost;
          // Found an exact match
          if (leastConversionCost == 0) break;
        } else if (conversionCost == leastConversionCost){
          // Ambiguous call: two methods with the same number of implicit conversions
          LOG.info("Ambigious methods: passed = " + argumentsPassed
              + " method 1 = " + udfMethod + " method 2 = " + m);
          udfMethod = null;
          // Don't break! We might find a better match later. 
        } else {
          // do nothing if implicitConversions > leastImplicitConversions
        }
      }
    }
    return udfMethod;
  }
  
  /**
   * A shortcut to get the "index" GenericUDF.
   * This is used for getting elements out of array and getting values out of map.
   */
  public static GenericUDF getGenericUDFForIndex() {
    return FunctionRegistry.getFunctionInfo("index").getGenericUDF();
  }

  /**
   * A shortcut to get the "and" GenericUDF.
   */
  public static GenericUDF getGenericUDFForAnd() {
    return FunctionRegistry.getFunctionInfo("and").getGenericUDF();
  }
  
  /**
   * Create a copy of an existing GenericUDF.
   */
  public static GenericUDF cloneGenericUDF(GenericUDF genericUDF) {
    if (genericUDF instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge)genericUDF;
      return new GenericUDFBridge(bridge.getUdfName(), bridge.isOperator(), bridge.getUdfClass());
    } else {
      return (GenericUDF)ReflectionUtils.newInstance(genericUDF.getClass(), null);
    }
  }

  /**
   * Create a copy of an existing GenericUDTF.
   */
  public static GenericUDTF cloneGenericUDTF(GenericUDTF genericUDTF) {
      return (GenericUDTF)ReflectionUtils.newInstance(genericUDTF.getClass(), null);
  }
  
  /**
   * Get the UDF class from an exprNodeDesc.
   * Returns null if the exprNodeDesc does not contain a UDF class.  
   */
  private static Class<? extends UDF> getUDFClassFromExprDesc(exprNodeDesc desc) {
    if (!(desc instanceof exprNodeGenericFuncDesc)) {
      return null;
    }
    exprNodeGenericFuncDesc genericFuncDesc = (exprNodeGenericFuncDesc)desc;
    if (!(genericFuncDesc.getGenericUDF() instanceof GenericUDFBridge)) {
      return null;
    }
    GenericUDFBridge bridge = (GenericUDFBridge)(genericFuncDesc.getGenericUDF());
    return bridge.getUdfClass();
  }
  
  /**
   * Returns whether a GenericUDF is deterministic or not.
   */
  public static boolean isDeterministic(GenericUDF genericUDF) {
    UDFType genericUDFType = genericUDF.getClass().getAnnotation(UDFType.class);
    if (genericUDFType != null && genericUDFType.deterministic() == false) {
      return false;
    }
    
    if (genericUDF instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge)(genericUDF);
      UDFType bridgeUDFType = bridge.getUdfClass().getAnnotation(UDFType.class);
      if (bridgeUDFType != null && bridgeUDFType.deterministic() == false) {
        return false;
      }
    }
    
    return true;
  }
  
  /**
   * Returns whether the exprNodeDesc is a node of "and", "or", "not".  
   */
  public static boolean isOpAndOrNot(exprNodeDesc desc) {
    Class<? extends UDF> udfClass = getUDFClassFromExprDesc(desc);
    return UDFOPAnd.class == udfClass
        || UDFOPOr.class == udfClass
        || UDFOPNot.class == udfClass;
  }
  
  /**
   * Returns whether the exprNodeDesc is a node of "and".  
   */
  public static boolean isOpAnd(exprNodeDesc desc) {
    Class<? extends UDF> udfClass = getUDFClassFromExprDesc(desc);
    return UDFOPAnd.class == udfClass;
  }
  
  /**
   * Returns whether the exprNodeDesc is a node of "positive".  
   */
  public static boolean isOpPositive(exprNodeDesc desc) {
    Class<? extends UDF> udfClass = getUDFClassFromExprDesc(desc);
    return UDFOPPositive.class == udfClass;
  }
  
}
