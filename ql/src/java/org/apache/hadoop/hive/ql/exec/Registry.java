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

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.ql.exec.FunctionInfo.FunctionResource;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMacro;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionResolver;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

// Extracted from FunctionRegistry
public class Registry {

  private static final Log LOG = LogFactory.getLog(FunctionRegistry.class);

  // prefix for window functions, to discern LEAD/LAG UDFs from window functions with the same name
  private static final String WINDOW_FUNC_PREFIX = "@_";

  /**
   * The mapping from expression function names to expression classes.
   */
  private final Map<String, FunctionInfo> mFunctions = new LinkedHashMap<String, FunctionInfo>();
  private final Set<Class<?>> builtIns = Collections.synchronizedSet(new HashSet<Class<?>>());
  private final Set<ClassLoader> mSessionUDFLoaders = new LinkedHashSet<ClassLoader>();

  private final boolean isNative;

  Registry(boolean isNative) {
    this.isNative = isNative;
  }

  public Registry() {
    this(false);
  }

  /**
   * Registers the appropriate kind of temporary function based on a class's
   * type.
   *
   * @param functionName name under which to register function
   * @param udfClass     class implementing UD[A|T]F
   * @return true if udfClass's type was recognized (so registration
   *         succeeded); false otherwise
   */
  @SuppressWarnings("unchecked")
  public FunctionInfo registerFunction(
      String functionName, Class<?> udfClass, FunctionResource... resources) {

    FunctionUtils.UDFClassType udfClassType = FunctionUtils.getUDFClassType(udfClass);
    switch (udfClassType) {
      case UDF:
        return registerUDF(
            functionName, (Class<? extends UDF>) udfClass, false, resources);
      case GENERIC_UDF:
        return registerGenericUDF(
            functionName, (Class<? extends GenericUDF>) udfClass, resources);
      case GENERIC_UDTF:
        return registerGenericUDTF(
            functionName, (Class<? extends GenericUDTF>) udfClass, resources);
      case UDAF:
        return registerUDAF(
            functionName, (Class<? extends UDAF>) udfClass, resources);
      case GENERIC_UDAF_RESOLVER:
        return registerGenericUDAF(
            functionName, (GenericUDAFResolver)
            ReflectionUtils.newInstance(udfClass, null), resources);
      case TABLE_FUNCTION_RESOLVER:
        // native or not would be decided by annotation. need to evaluate that first
        return registerTableFunction(functionName,
            (Class<? extends TableFunctionResolver>) udfClass, resources);
    }
    return null;

  }

  public FunctionInfo registerUDF(String functionName,
      Class<? extends UDF> UDFClass, boolean isOperator, FunctionResource... resources) {
    return registerUDF(functionName, UDFClass, isOperator, functionName.toLowerCase(), resources);
  }

  public FunctionInfo registerUDF(String functionName,
      Class<? extends UDF> UDFClass, boolean isOperator, String displayName,
      FunctionResource... resources) {
    validateClass(UDFClass, UDF.class);
    FunctionInfo fI = new FunctionInfo(isNative, displayName,
        new GenericUDFBridge(displayName, isOperator, UDFClass.getName()), resources);
    addFunction(functionName, fI);
    return fI;
  }

  public FunctionInfo registerGenericUDF(String functionName,
      Class<? extends GenericUDF> genericUDFClass, FunctionResource... resources) {
    validateClass(genericUDFClass, GenericUDF.class);
    FunctionInfo fI = new FunctionInfo(isNative, functionName,
        ReflectionUtils.newInstance(genericUDFClass, null), resources);
    addFunction(functionName, fI);
    return fI;
  }

  public FunctionInfo registerGenericUDTF(String functionName,
      Class<? extends GenericUDTF> genericUDTFClass, FunctionResource... resources) {
    validateClass(genericUDTFClass, GenericUDTF.class);
    FunctionInfo fI = new FunctionInfo(isNative, functionName,
        ReflectionUtils.newInstance(genericUDTFClass, null), resources);
    addFunction(functionName, fI);
    return fI;
  }

  public FunctionInfo registerGenericUDAF(String functionName,
      GenericUDAFResolver genericUDAFResolver, FunctionResource... resources) {
    FunctionInfo function =
        new WindowFunctionInfo(isNative, functionName, genericUDAFResolver, resources);
    addFunction(functionName, function);
    addFunction(WINDOW_FUNC_PREFIX + functionName, function);
    return function;
  }

  public FunctionInfo registerUDAF(String functionName,
      Class<? extends UDAF> udafClass, FunctionResource... resources) {
    validateClass(udafClass, UDAF.class);
    FunctionInfo function = new WindowFunctionInfo(isNative, functionName,
        new GenericUDAFBridge(ReflectionUtils.newInstance(udafClass, null)), resources);
    addFunction(functionName, function);
    addFunction(WINDOW_FUNC_PREFIX + functionName, function);
    return function;
  }

  public FunctionInfo registerTableFunction(String functionName,
      Class<? extends TableFunctionResolver> tFnCls, FunctionResource... resources) {
    validateClass(tFnCls, TableFunctionResolver.class);
    FunctionInfo function = new FunctionInfo(isNative, functionName, tFnCls, resources);
    addFunction(functionName, function);
    return function;
  }

  public FunctionInfo registerMacro(String macroName, ExprNodeDesc body,
      List<String> colNames, List<TypeInfo> colTypes) {
    return registerMacro(macroName, body, colNames, colTypes, null);
  }

  public FunctionInfo registerMacro(String macroName, ExprNodeDesc body,
      List<String> colNames, List<TypeInfo> colTypes, FunctionResource... resources) {
    GenericUDFMacro macro = new GenericUDFMacro(macroName, body, colNames, colTypes);
    FunctionInfo fI = new FunctionInfo(isNative, macroName, macro, resources);
    addFunction(macroName, fI);
    return fI;
  }

  public FunctionInfo registerPermanentFunction(String functionName,
      String className, boolean registerToSession, FunctionResource... resources) {
    FunctionInfo function = new FunctionInfo(functionName, className, resources);
    // register to session first for backward compatibility
    if (registerToSession) {
      String qualifiedName = FunctionUtils.qualifyFunctionName(
          functionName, SessionState.get().getCurrentDatabase().toLowerCase());
      if (registerToSessionRegistry(qualifiedName, function) != null) {
        addFunction(functionName, function);
        return function;
      }
    }
    addFunction(functionName, function);
    return null;
  }

  /**
   * Typically a WindowFunction is the same as a UDAF. The only exceptions are Lead & Lag UDAFs. These
   * are not registered as regular UDAFs because
   * - we plan to support Lead & Lag as UDFs (usable only within argument expressions
   *   of UDAFs when windowing is involved). Since mFunctions holds both UDFs and UDAFs we cannot
   *   add both FunctionInfos to mFunctions.
   *
   * @param name
   * @param wFn
   */
  void registerWindowFunction(String name, GenericUDAFResolver wFn) {
    addFunction(WINDOW_FUNC_PREFIX + name, new WindowFunctionInfo(isNative, name, wFn, null));
  }

  private void validateClass(Class input, Class expected) {
    if (!expected.isAssignableFrom(input)) {
      throw new RuntimeException("Registering UDF Class " + input
          + " which does not extend " + expected);
    }
  }

  /**
   * Looks up the function name in the registry. If enabled, will attempt to search the metastore
   * for the function.
   * @param functionName
   * @return
   */
  public synchronized FunctionInfo getFunctionInfo(String functionName) throws SemanticException {
    functionName = functionName.toLowerCase();
    if (FunctionUtils.isQualifiedFunctionName(functionName)) {
      return getQualifiedFunctionInfo(functionName);
    }
    // First try without qualifiers - would resolve builtin/temp functions.
    // Otherwise try qualifying with current db name.
    FunctionInfo functionInfo = mFunctions.get(functionName);
    if (functionInfo != null && functionInfo.isBlockedFunction()) {
      throw new SemanticException ("UDF " + functionName + " is not allowed");
    }
    if (functionInfo == null) {
      String qualifiedName = FunctionUtils.qualifyFunctionName(
          functionName, SessionState.get().getCurrentDatabase().toLowerCase());
      functionInfo = getQualifiedFunctionInfo(qualifiedName);
    }
    return functionInfo;
  }

  public WindowFunctionInfo getWindowFunctionInfo(String functionName) throws SemanticException {
    FunctionInfo info = getFunctionInfo(WINDOW_FUNC_PREFIX + functionName);
    if (info instanceof WindowFunctionInfo) {
      return (WindowFunctionInfo) info;
    }
    return null;
  }

  /**
   * @param udfClass Function class.
   * @return True iff the fnExpr represents a hive built-in function.
   */
  public boolean isBuiltInFunc(Class<?> udfClass) {
    return udfClass != null && builtIns.contains(udfClass);
  }

  public synchronized Set<String> getCurrentFunctionNames() {
    return getFunctionNames((Pattern)null);
  }

  public synchronized Set<String> getFunctionNames(String funcPatternStr) {
    try {
      return getFunctionNames(Pattern.compile(funcPatternStr));
    } catch (PatternSyntaxException e) {
      return Collections.emptySet();
    }
  }

  /**
   * Returns a set of registered function names. This is used for the CLI
   * command "SHOW FUNCTIONS 'regular expression';" Returns an empty set when
   * the regular expression is not valid.
   *
   * @param funcPattern regular expression of the interested function names
   * @return set of strings contains function names
   */
  public synchronized Set<String> getFunctionNames(Pattern funcPattern) {
    Set<String> funcNames = new TreeSet<String>();
    for (String funcName : mFunctions.keySet()) {
      if (funcName.contains(WINDOW_FUNC_PREFIX)) {
        continue;
      }
      if (funcPattern == null || funcPattern.matcher(funcName).matches()) {
        funcNames.add(funcName);
      }
    }
    return funcNames;
  }

  /**
   * Adds to the set of synonyms of the supplied function.
   * @param funcName
   * @param funcInfo
   * @param synonyms
   */
  public synchronized void getFunctionSynonyms(
      String funcName, FunctionInfo funcInfo, Set<String> synonyms) throws SemanticException {
    Class<?> funcClass = funcInfo.getFunctionClass();
    for (Map.Entry<String, FunctionInfo> entry : mFunctions.entrySet()) {
      String name = entry.getKey();
      if (name.contains(WINDOW_FUNC_PREFIX) || name.equals(funcName)) {
        continue;
      }
      FunctionInfo function = entry.getValue();
      if (function.getFunctionClass() == funcClass) {
        synonyms.add(name);
      }
    }
  }

  /**
   * Get the GenericUDAF evaluator for the name and argumentClasses.
   *
   * @param name         the name of the UDAF
   * @param argumentOIs
   * @param isDistinct
   * @param isAllColumns
   * @return The UDAF evaluator
   */
  @SuppressWarnings("deprecation")
  public GenericUDAFEvaluator getGenericUDAFEvaluator(String name,
      List<ObjectInspector> argumentOIs, boolean isDistinct,
      boolean isAllColumns) throws SemanticException {

    GenericUDAFResolver udafResolver = getGenericUDAFResolver(name);
    if (udafResolver == null) {
      return null;
    }

    GenericUDAFEvaluator udafEvaluator;
    ObjectInspector args[] = new ObjectInspector[argumentOIs.size()];
    // Can't use toArray here because Java is dumb when it comes to
    // generics + arrays.
    for (int ii = 0; ii < argumentOIs.size(); ++ii) {
      args[ii] = argumentOIs.get(ii);
    }

    GenericUDAFParameterInfo paramInfo =
        new SimpleGenericUDAFParameterInfo(
            args, isDistinct, isAllColumns);
    if (udafResolver instanceof GenericUDAFResolver2) {
      udafEvaluator =
          ((GenericUDAFResolver2) udafResolver).getEvaluator(paramInfo);
    } else {
      udafEvaluator = udafResolver.getEvaluator(paramInfo.getParameters());
    }
    return udafEvaluator;
  }

  public GenericUDAFEvaluator getGenericWindowingEvaluator(String functionName,
      List<ObjectInspector> argumentOIs, boolean isDistinct, boolean isAllColumns)
      throws SemanticException {
    functionName = functionName.toLowerCase();
    WindowFunctionInfo info = getWindowFunctionInfo(functionName);
    if (info == null) {
      return null;
    }
    if (!functionName.equals(FunctionRegistry.LEAD_FUNC_NAME) &&
        !functionName.equals(FunctionRegistry.LAG_FUNC_NAME)) {
      return getGenericUDAFEvaluator(functionName, argumentOIs, isDistinct, isAllColumns);
    }

    // this must be lead/lag UDAF
    ObjectInspector args[] = new ObjectInspector[argumentOIs.size()];
    GenericUDAFResolver udafResolver = info.getGenericUDAFResolver();
    GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(
        argumentOIs.toArray(args), isDistinct, isAllColumns);
    return ((GenericUDAFResolver2) udafResolver).getEvaluator(paramInfo);
  }

  private synchronized void addFunction(String functionName, FunctionInfo function) {
    if (isNative ^ function.isNative()) {
      throw new RuntimeException("Function " + functionName + " is not for this registry");
    }
    functionName = functionName.toLowerCase();
    FunctionInfo prev = mFunctions.get(functionName);
    if (prev != null) {
      if (isBuiltInFunc(prev.getFunctionClass())) {
        throw new RuntimeException("Function " + functionName + " is hive builtin function, " +
            "which cannot be overriden.");
      }
      prev.discarded();
    }
    mFunctions.put(functionName, function);
    if (function.isBuiltIn()) {
      builtIns.add(function.getFunctionClass());
    }
  }

  public synchronized void unregisterFunction(String functionName) throws HiveException {
    functionName = functionName.toLowerCase();
    FunctionInfo fi = mFunctions.get(functionName);
    if (fi != null) {
      if (fi.isBuiltIn()) {
        throw new HiveException(ErrorMsg.DROP_NATIVE_FUNCTION.getMsg(functionName));
      }
      mFunctions.remove(functionName);
      fi.discarded();
    }
  }

  public GenericUDAFResolver getGenericUDAFResolver(String functionName) throws SemanticException {
    FunctionInfo info = getFunctionInfo(functionName);
    if (info != null) {
      return info.getGenericUDAFResolver();
    }
    return null;
  }

  private FunctionInfo getQualifiedFunctionInfo(String qualifiedName) throws SemanticException {
    FunctionInfo info = mFunctions.get(qualifiedName);
    if (info != null && info.isBlockedFunction()) {
      throw new SemanticException ("UDF " + qualifiedName + " is not allowed");
    }
    if (!isNative && info != null && info.isDiscarded()) {
      // the persistent function is discarded. try reload
      mFunctions.remove(qualifiedName);
      return null;
    }
    // HIVE-6672: In HiveServer2 the JARs for this UDF may have been loaded by a different thread,
    // and the current thread may not be able to resolve the UDF. Test for this condition
    // and if necessary load the JARs in this thread.
    if (isNative && info != null && info.isPersistent()) {
      return registerToSessionRegistry(qualifiedName, info);
    }
    return info;
  }

  // should be called after session registry is checked
  private FunctionInfo registerToSessionRegistry(String qualifiedName, FunctionInfo function) {
    FunctionInfo ret = null;
    ClassLoader prev = Utilities.getSessionSpecifiedClassLoader();
    try {
      // Found UDF in metastore - now add it to the function registry
      // At this point we should add any relevant jars that would be needed for the UDf.
      FunctionResource[] resources = function.getResources();
      try {
        FunctionTask.addFunctionResources(resources);
      } catch (Exception e) {
        LOG.error("Unable to load resources for " + qualifiedName + ":" + e, e);
        return null;
      }
      ClassLoader loader = Utilities.getSessionSpecifiedClassLoader();
      Class<?> udfClass = Class.forName(function.getClassName(), true, loader);

      ret = FunctionRegistry.registerTemporaryUDF(qualifiedName, udfClass, resources);
      if (ret == null) {
        LOG.error(function.getClassName() + " is not a valid UDF class and was not registered.");
      }
      if (SessionState.get().isHiveServerQuery()) {
        SessionState.getRegistryForWrite().addToUDFLoaders(loader);
      }
    } catch (ClassNotFoundException e) {
      // Lookup of UDf class failed
      LOG.error("Unable to load UDF class: " + e);
      Utilities.restoreSessionSpecifiedClassLoader(prev);
    }
    function.shareStateWith(ret);
    return ret;
  }

  private void checkFunctionClass(FunctionInfo cfi) throws ClassNotFoundException {
    // This call will fail for non-generic UDFs using GenericUDFBridge
    Class<?> udfClass = cfi.getFunctionClass();
    // Even if we have a reference to the class (which will be the case for GenericUDFs),
    // the classloader may not be able to resolve the class, which would mean reflection-based
    // methods would fail such as for plan deserialization. Make sure this works too.
    Class.forName(udfClass.getName(), true, Utilities.getSessionSpecifiedClassLoader());
  }

  public synchronized void clear() {
    if (isNative) {
      throw new IllegalStateException("System function registry cannot be cleared");
    }
    mFunctions.clear();
    builtIns.clear();
  }

  public synchronized void closeCUDFLoaders() {
    try {
      for(ClassLoader loader: mSessionUDFLoaders) {
        JavaUtils.closeClassLoader(loader);
      }
    } catch (IOException ie) {
        LOG.error("Error in close loader: " + ie);
    }
    mSessionUDFLoaders.clear();
  }

  public synchronized void addToUDFLoaders(ClassLoader loader) {
    mSessionUDFLoaders.add(loader);
  }
  public synchronized void removeFromUDFLoaders(ClassLoader loader) {
    mSessionUDFLoaders.remove(loader);
  }

  /**
   * Setup blocked flag for all builtin UDFs as per udf whitelist and blacklist
   * @param whiteListStr
   * @param blackListStr
   */
  public void setupPermissionsForUDFs(String whiteListStr, String blackListStr) {
    Set<String> whiteList = Sets.newHashSet(
        Splitter.on(",").trimResults().omitEmptyStrings().split(whiteListStr.toLowerCase()));
    Set<String> blackList = Sets.newHashSet(
        Splitter.on(",").trimResults().omitEmptyStrings().split(blackListStr.toLowerCase()));
    blackList.removeAll(FunctionRegistry.HIVE_OPERATORS);

    for (Map.Entry<String, FunctionInfo> funcEntry : mFunctions.entrySet()) {
      funcEntry.getValue().setBlockedFunction(
          isUdfBlocked(funcEntry.getKey(), whiteList, blackList));
    }
  }

  /**
   * Check if the function belongs to whitelist or blacklist
   * @param functionName
   * @param whiteList
   * @param blackList
   * @return true if the given udf is to be blocked
   */
  boolean isUdfBlocked(String functionName, Set<String> whiteList, Set<String> blackList) {
    functionName = functionName.toLowerCase();
    return blackList.contains(functionName) ||
        (!whiteList.isEmpty() && !whiteList.contains(functionName));
  }
}
