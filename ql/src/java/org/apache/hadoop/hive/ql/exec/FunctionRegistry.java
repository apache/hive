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

import org.apache.hadoop.hive.ql.exec.FunctionInfo.OperatorType;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.groupByDesc;
import org.apache.hadoop.hive.ql.udf.*;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
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
    registerUDF("concat", UDFConcat.class, OperatorType.PREFIX, false);
    registerUDF("substr", UDFSubstr.class, OperatorType.PREFIX, false);
    registerUDF("substring", UDFSubstr.class, OperatorType.PREFIX, false);
    registerUDF("space", UDFSpace.class, OperatorType.PREFIX, false);
    registerUDF("repeat", UDFRepeat.class, OperatorType.PREFIX, false);
    registerUDF("ascii", UDFAscii.class, OperatorType.PREFIX, false);
    registerUDF("lpad", UDFLpad.class, OperatorType.PREFIX, false);
    registerUDF("rpad", UDFRpad.class, OperatorType.PREFIX, false);
    
    registerGenericUDF("size", GenericUDFSize.class);

    registerUDF("round", UDFRound.class, OperatorType.PREFIX, false);
    registerUDF("floor", UDFFloor.class, OperatorType.PREFIX, false);
    registerUDF("sqrt", UDFSqrt.class, OperatorType.PREFIX, false);
    registerUDF("ceil", UDFCeil.class, OperatorType.PREFIX, false);
    registerUDF("ceiling", UDFCeil.class, OperatorType.PREFIX, false);
    registerUDF("rand", UDFRand.class, OperatorType.PREFIX, false);
    registerUDF("abs", UDFAbs.class, OperatorType.PREFIX, false);
    registerUDF("pmod", UDFPosMod.class, OperatorType.PREFIX, false);

    registerUDF("ln", UDFLn.class, OperatorType.PREFIX, false);
    registerUDF("log2", UDFLog2.class, OperatorType.PREFIX, false);
    registerUDF("sin",UDFSin.class, OperatorType.PREFIX, false);
    registerUDF("asin",UDFAsin.class, OperatorType.PREFIX, false);
    registerUDF("cos",UDFCos.class, OperatorType.PREFIX, false);
    registerUDF("acos",UDFAcos.class, OperatorType.PREFIX, false);
    registerUDF("log10", UDFLog10.class, OperatorType.PREFIX, false);
    registerUDF("log", UDFLog.class, OperatorType.PREFIX, false);
    registerUDF("exp", UDFExp.class, OperatorType.PREFIX, false);
    registerUDF("power", UDFPower.class, OperatorType.PREFIX, false);
    registerUDF("pow", UDFPower.class, OperatorType.PREFIX, false);

    registerUDF("conv", UDFConv.class, OperatorType.PREFIX, false);
    registerUDF("bin", UDFBin.class, OperatorType.PREFIX, false);
    registerUDF("hex", UDFHex.class, OperatorType.PREFIX, false);
    
    registerUDF("upper", UDFUpper.class, OperatorType.PREFIX, false);
    registerUDF("lower", UDFLower.class, OperatorType.PREFIX, false);
    registerUDF("ucase", UDFUpper.class, OperatorType.PREFIX, false);
    registerUDF("lcase", UDFLower.class, OperatorType.PREFIX, false);
    registerUDF("trim", UDFTrim.class, OperatorType.PREFIX, false);
    registerUDF("ltrim", UDFLTrim.class, OperatorType.PREFIX, false);
    registerUDF("rtrim", UDFRTrim.class, OperatorType.PREFIX, false);
    registerUDF("length", UDFLength.class, OperatorType.PREFIX, false);
    registerUDF("reverse", UDFReverse.class, OperatorType.PREFIX, false);

    registerUDF("like", UDFLike.class, OperatorType.INFIX, true);
    registerUDF("rlike", UDFRegExp.class, OperatorType.INFIX, true);
    registerUDF("regexp", UDFRegExp.class, OperatorType.INFIX, true);
    registerUDF("regexp_replace", UDFRegExpReplace.class, OperatorType.PREFIX, false);
    registerUDF("regexp_extract", UDFRegExpExtract.class, OperatorType.PREFIX, false);
    registerUDF("parse_url", UDFParseUrl.class, OperatorType.PREFIX, false);
    registerGenericUDF("split", GenericUDFSplit.class);

    registerUDF("positive", UDFOPPositive.class, OperatorType.PREFIX, true, "+");
    registerUDF("negative", UDFOPNegative.class, OperatorType.PREFIX, true, "-");

    registerUDF("day", UDFDayOfMonth.class, OperatorType.PREFIX, false);
    registerUDF("dayofmonth", UDFDayOfMonth.class, OperatorType.PREFIX, false);
    registerUDF("month", UDFMonth.class, OperatorType.PREFIX, false);
    registerUDF("year", UDFYear.class, OperatorType.PREFIX, false);
    registerUDF("from_unixtime", UDFFromUnixTime.class, OperatorType.PREFIX, false);
    registerUDF("unix_timestamp", UDFUnixTimeStamp.class, OperatorType.PREFIX, false);
    registerUDF("to_date", UDFDate.class, OperatorType.PREFIX, false);

    registerUDF("date_add", UDFDateAdd.class, OperatorType.PREFIX, false);
    registerUDF("date_sub", UDFDateSub.class, OperatorType.PREFIX, false);
    registerUDF("datediff", UDFDateDiff.class, OperatorType.PREFIX, false);

    registerUDF("get_json_object", UDFJson.class, OperatorType.PREFIX, false);

    registerUDF("+", UDFOPPlus.class, OperatorType.INFIX, true);
    registerUDF("-", UDFOPMinus.class, OperatorType.INFIX, true);
    registerUDF("*", UDFOPMultiply.class, OperatorType.INFIX, true);
    registerUDF("/", UDFOPDivide.class, OperatorType.INFIX, true);
    registerUDF("%", UDFOPMod.class, OperatorType.INFIX, true);

    registerUDF("&", UDFOPBitAnd.class, OperatorType.INFIX, true);
    registerUDF("|", UDFOPBitOr.class, OperatorType.INFIX, true);
    registerUDF("^", UDFOPBitXor.class, OperatorType.INFIX, true);
    registerUDF("~", UDFOPBitNot.class, OperatorType.PREFIX, true);

    registerUDF("=", UDFOPEqual.class, OperatorType.INFIX, true);
    registerUDF("==", UDFOPEqual.class, OperatorType.INFIX, true, "=");
    registerUDF("<>", UDFOPNotEqual.class, OperatorType.INFIX, true);
    registerUDF("<", UDFOPLessThan.class, OperatorType.INFIX, true);
    registerUDF("<=", UDFOPEqualOrLessThan.class, OperatorType.INFIX, true);
    registerUDF(">", UDFOPGreaterThan.class, OperatorType.INFIX, true);
    registerUDF(">=", UDFOPEqualOrGreaterThan.class, OperatorType.INFIX, true);

    registerUDF("and", UDFOPAnd.class, OperatorType.INFIX, true);
    registerUDF("&&", UDFOPAnd.class, OperatorType.INFIX, true, "and");
    registerUDF("or", UDFOPOr.class, OperatorType.INFIX, true);
    registerUDF("||", UDFOPOr.class, OperatorType.INFIX, true, "or");
    registerUDF("not", UDFOPNot.class, OperatorType.PREFIX, true);
    registerUDF("!", UDFOPNot.class, OperatorType.PREFIX, true, "not");

    registerGenericUDF("isnull", GenericUDFOPNull.class);
    registerGenericUDF("isnotnull", GenericUDFOPNotNull.class);

    registerGenericUDF("if", GenericUDFIf.class);

    // Aliases for Java Class Names
    // These are used in getImplicitConvertUDFMethod
    registerUDF(Constants.BOOLEAN_TYPE_NAME, UDFToBoolean.class, OperatorType.PREFIX, false,
                UDFToBoolean.class.getSimpleName());
    registerUDF(Constants.TINYINT_TYPE_NAME, UDFToByte.class, OperatorType.PREFIX, false,
                UDFToByte.class.getSimpleName());
    registerUDF(Constants.SMALLINT_TYPE_NAME, UDFToShort.class, OperatorType.PREFIX, false,
                UDFToShort.class.getSimpleName());
    registerUDF(Constants.INT_TYPE_NAME, UDFToInteger.class, OperatorType.PREFIX, false,
                UDFToInteger.class.getSimpleName());
    registerUDF(Constants.BIGINT_TYPE_NAME, UDFToLong.class, OperatorType.PREFIX, false,
                UDFToLong.class.getSimpleName());
    registerUDF(Constants.FLOAT_TYPE_NAME, UDFToFloat.class, OperatorType.PREFIX, false,
                UDFToFloat.class.getSimpleName());
    registerUDF(Constants.DOUBLE_TYPE_NAME, UDFToDouble.class, OperatorType.PREFIX, false,
                UDFToDouble.class.getSimpleName());
    registerUDF(Constants.STRING_TYPE_NAME, UDFToString.class, OperatorType.PREFIX, false,
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
    registerGenericUDF("case", GenericUDFCase.class);
    registerGenericUDF("when", GenericUDFWhen.class);
    registerGenericUDF("hash", GenericUDFHash.class);
    registerGenericUDF("coalesce", GenericUDFCoalesce.class);
    registerGenericUDF("index", GenericUDFIndex.class);
    registerGenericUDF("instr", GenericUDFInstr.class);
    registerGenericUDF("locate", GenericUDFLocate.class);
    registerGenericUDF("elt", GenericUDFElt.class);
  }

  public static FunctionInfo getUDFInfo(Class<?> fClass) {
    for(Map.Entry<String, FunctionInfo> ent: mFunctions.entrySet()) {
      FunctionInfo val = ent.getValue();
      if (val.getUDFClass() == fClass) {
        return val;
      }
    }

    return null;
  }

  public static void registerUDF(String functionName, Class<? extends UDF> UDFClass,
                                 FunctionInfo.OperatorType opt, boolean isOperator) {
    if (UDF.class.isAssignableFrom(UDFClass)) {
      FunctionInfo fI = new FunctionInfo(functionName.toLowerCase(), UDFClass, null);
      fI.setIsOperator(isOperator);
      fI.setOpType(opt);
      mFunctions.put(functionName.toLowerCase(), fI);
    } else {
      throw new RuntimeException("Registering UDF Class " + UDFClass + " which does not extends " + UDF.class);
    }
  }

  public static void registerUDF(String functionName, Class<? extends UDF> UDFClass,
                                 FunctionInfo.OperatorType opt, boolean isOperator,
                                 String displayName) {
    if (UDF.class.isAssignableFrom(UDFClass)) {
      FunctionInfo fI = new FunctionInfo(displayName, UDFClass, null);
      fI.setIsOperator(isOperator);
      fI.setOpType(opt);
      mFunctions.put(functionName.toLowerCase(), fI);
    } else {
      throw new RuntimeException("Registering UDF Class " + UDFClass + " which does not extends " + UDF.class);
    }
  }

  public static void registerGenericUDF(String functionName, Class<? extends GenericUDF> genericUDFClass) {
    if (GenericUDF.class.isAssignableFrom(genericUDFClass)) {
      FunctionInfo fI = new FunctionInfo(functionName, null, genericUDFClass);
      mFunctions.put(functionName.toLowerCase(), fI);
    } else {
      throw new RuntimeException("Registering GenericUDF Class " + genericUDFClass
          + " which does not extends " + GenericUDF.class);
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

  public static Class<? extends UDF> getUDFClass(String functionName) {
    LOG.debug("Looking up: " + functionName);
    FunctionInfo finfo = mFunctions.get(functionName.toLowerCase());
    if (finfo == null) {
      return null;
    }
    Class<? extends UDF> result = finfo.getUDFClass();
    return result;
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
   * Get the UDF method for the name and argumentClasses.
   * @param name the name of the UDF
   * @param argumentTypeInfos
   * @return The UDF method
   */
  public static Method getUDFMethod(String name, List<TypeInfo> argumentTypeInfos) {
    Class<? extends UDF> udf = getUDFClass(name);
    if (udf == null) return null;
    Method udfMethod = null;
    try {
      udfMethod = udf.newInstance().getResolver().getEvalMethod(argumentTypeInfos);
    }
    catch (AmbiguousMethodException e) {
    }
    catch (Exception e) {
      throw new RuntimeException("Cannot get UDF for " + name + " " + argumentTypeInfos, e);
    }
    return udfMethod;
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
  public static <T> Method getMethodInternal(Class<? extends T> udfClass, String methodName, boolean exact, List<TypeInfo> argumentClasses) {

    ArrayList<Method> mlist = new ArrayList<Method>();

    for(Method m: Arrays.asList(udfClass.getMethods())) {
      if (m.getName().equals(methodName)) {
        mlist.add(m);
      }
    }

    return getMethodInternal(mlist, exact, argumentClasses);
  }

  public static Method getUDFMethod(String name, TypeInfo ... argumentClasses) {
    return getUDFMethod(name, Arrays.asList(argumentClasses));
  }

  public static void registerGenericUDAF(String functionName, GenericUDAFResolver genericUDAFResolver) {
    mFunctions.put(functionName.toLowerCase(), 
        new FunctionInfo(functionName.toLowerCase(), genericUDAFResolver));
  }

  public static void registerUDAF(String functionName, Class<? extends UDAF> udafClass) {
    mFunctions.put(functionName.toLowerCase(), 
        new FunctionInfo(functionName.toLowerCase(), 
            new GenericUDAFBridge((UDAF)ReflectionUtils.newInstance(udafClass, null))));
  }

  public static void unregisterUDF(String functionName) {
    mFunctions.remove(functionName.toLowerCase());
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

      e.printStackTrace();
      throw new HiveException("Unable to execute method " + m + " "
          + " on object " + thisObjectString
          + " with arguments " + argumentString.toString()
          + ":" + e.getMessage());
    }
    return o;
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
    int leastImplicitConversions = Integer.MAX_VALUE;
    Method udfMethod = null;

    for(Method m: mlist) {
      List<TypeInfo> argumentsAccepted = TypeInfoUtils.getParameterTypeInfos(m);
      
      boolean match = (argumentsAccepted.size() == argumentsPassed.size());
      int implicitConversions = 0;

      for(int i=0; i<argumentsPassed.size() && match; i++) {
        TypeInfo argumentPassed = argumentsPassed.get(i);
        TypeInfo argumentAccepted = argumentsAccepted.get(i);
        if (argumentPassed.equals(TypeInfoFactory.voidTypeInfo)) {
          // passing null matches everything
          continue;
        }
        if (argumentAccepted.equals(TypeInfoFactory.unknownTypeInfo)) {
          // accepting Object means accepting everything
          continue;
        }
        if (argumentPassed.getCategory().equals(Category.LIST) 
            && argumentAccepted.equals(TypeInfoFactory.unknownListTypeInfo)) {
          // accepting List means accepting List of everything
          continue;
        }
        if (argumentPassed.getCategory().equals(Category.MAP) 
            && argumentAccepted.equals(TypeInfoFactory.unknownMapTypeInfo)) {
          // accepting Map means accepting Map of everything
          continue;
        }
        TypeInfo accepted = argumentsAccepted.get(i);
        if (accepted.equals(argumentsPassed.get(i))) {
          // do nothing if match
        } else if (!exact && implicitConvertable(argumentsPassed.get(i), accepted)) {
          implicitConversions ++;
        } else {
          match = false;
        }
      }

      if (match) {
        // Always choose the function with least implicit conversions.
        if (implicitConversions < leastImplicitConversions) {
          udfMethod = m;
          leastImplicitConversions = implicitConversions;
          // Found an exact match
          if (leastImplicitConversions == 0) break;
        } else if (implicitConversions == leastImplicitConversions){
          // Ambiguous call: two methods with the same number of implicit conversions
          udfMethod = null;
        } else {
          // do nothing if implicitConversions > leastImplicitConversions
        }
      }
    }
    return udfMethod;
  }
  
  /**
   * A shortcut to get the index GenericUDFClass.
   * This is used for getting elements out of array and getting values out of map.
   */
  public static Class<? extends GenericUDF> getGenericUDFClassForIndex() {
    return FunctionRegistry.getFunctionInfo("index").getGenericUDFClass();
  }
}
