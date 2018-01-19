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

package org.apache.hadoop.hive.ql.exec;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.ql.exec.FunctionInfo.FunctionResource;
import org.apache.hadoop.hive.ql.exec.FunctionInfo.FunctionType;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.Hive;
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
import org.apache.hive.common.util.ReflectionUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

// Extracted from FunctionRegistry
public class Registry {

  private static final Logger LOG = LoggerFactory.getLogger(FunctionRegistry.class);

  // prefix for window functions, to discern LEAD/LAG UDFs from window functions with the same name
  private static final String WINDOW_FUNC_PREFIX = "@_";

  /**
   * The mapping from expression function names to expression classes.
   */
  private final Map<String, FunctionInfo> mFunctions = new LinkedHashMap<String, FunctionInfo>();
  private final Set<Class<?>> builtIns = Collections.synchronizedSet(new HashSet<Class<?>>());
  /**
   * Persistent map contains refcounts that are only modified in synchronized methods for now,
   * so there's no separate effort to make refcount operations thread-safe.
   */
  private final Map<Class<?>, Integer> persistent = new ConcurrentHashMap<>();
  private final Set<ClassLoader> mSessionUDFLoaders = new LinkedHashSet<ClassLoader>();

  private final boolean isNative;
  /**
   * The epic lock for the registry. This was added to replace the synchronized methods with
   * minimum disruption; the locking should really be made more granular here.
   */
  private final ReentrantLock lock = new ReentrantLock();

  public Registry(boolean isNative) {
    this.isNative = isNative;
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
  public FunctionInfo registerFunction(
      String functionName, Class<?> udfClass, FunctionResource... resources) {
    FunctionType functionType = isNative ? FunctionType.BUILTIN : FunctionType.TEMPORARY;
    return registerFunction(functionName, functionType, udfClass, resources);
  }

  @SuppressWarnings("unchecked")
  private FunctionInfo registerFunction(
      String functionName, FunctionType functionType, Class<?> udfClass, FunctionResource... resources) {

    FunctionUtils.UDFClassType udfClassType = FunctionUtils.getUDFClassType(udfClass);
    switch (udfClassType) {
      case UDF:
        return registerUDF(
            functionName, functionType, (Class<? extends UDF>) udfClass, false, functionName.toLowerCase(), resources);
      case GENERIC_UDF:
        return registerGenericUDF(
            functionName, functionType, (Class<? extends GenericUDF>) udfClass, resources);
      case GENERIC_UDTF:
        return registerGenericUDTF(
            functionName, functionType, (Class<? extends GenericUDTF>) udfClass, resources);
      case UDAF:
        return registerUDAF(
            functionName, functionType, (Class<? extends UDAF>) udfClass, resources);
      case GENERIC_UDAF_RESOLVER:
        return registerGenericUDAF(
            functionName, functionType,
            (GenericUDAFResolver) ReflectionUtil.newInstance(udfClass, null), resources);
      case TABLE_FUNCTION_RESOLVER:
        // native or not would be decided by annotation. need to evaluate that first
        return registerTableFunction(functionName, functionType,
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
    FunctionType functionType = isNative ? FunctionType.BUILTIN : FunctionType.TEMPORARY;
    return registerUDF(functionName, functionType, UDFClass, isOperator, displayName);
  }

  private FunctionInfo registerUDF(String functionName, FunctionType functionType,
      Class<? extends UDF> UDFClass, boolean isOperator, String displayName,
      FunctionResource... resources) {
    validateClass(UDFClass, UDF.class);
    FunctionInfo fI = new FunctionInfo(functionType, displayName,
        new GenericUDFBridge(displayName, isOperator, UDFClass.getName()), resources);
    addFunction(functionName, fI);
    return fI;
  }

  public FunctionInfo registerGenericUDF(String functionName,
      Class<? extends GenericUDF> genericUDFClass, FunctionResource... resources) {
    FunctionType functionType = isNative ? FunctionType.BUILTIN : FunctionType.TEMPORARY;
    return registerGenericUDF(functionName, functionType, genericUDFClass, resources);
  }

  private FunctionInfo registerGenericUDF(String functionName, FunctionType functionType,
      Class<? extends GenericUDF> genericUDFClass, FunctionResource... resources) {
    validateClass(genericUDFClass, GenericUDF.class);
    FunctionInfo fI = new FunctionInfo(functionType, functionName,
        ReflectionUtil.newInstance(genericUDFClass, null), resources);
    addFunction(functionName, fI);
    return fI;
  }

  /**
   * Registers the UDF class as a built-in function; used for dynamically created UDFs, like
   * GenericUDFOP*Minus/Plus.
   */
  public void registerHiddenBuiltIn(Class<? extends GenericUDF> functionClass) {
    lock.lock();
    try {
      if (!isNative) {
        throw new RuntimeException("Builtin is not for this registry");
      }
      builtIns.add(functionClass);
    } finally {
      lock.unlock();
    }
  }

  public FunctionInfo registerGenericUDTF(String functionName,
      Class<? extends GenericUDTF> genericUDTFClass, FunctionResource... resources) {
    FunctionType functionType = isNative ? FunctionType.BUILTIN : FunctionType.TEMPORARY;
    return registerGenericUDTF(functionName, functionType, genericUDTFClass, resources);
  }

  private FunctionInfo registerGenericUDTF(String functionName, FunctionType functionType,
      Class<? extends GenericUDTF> genericUDTFClass, FunctionResource... resources) {
    validateClass(genericUDTFClass, GenericUDTF.class);
    FunctionInfo fI = new FunctionInfo(functionType, functionName,
        ReflectionUtil.newInstance(genericUDTFClass, null), resources);
    addFunction(functionName, fI);
    return fI;
  }

  public FunctionInfo registerGenericUDAF(String functionName,
      GenericUDAFResolver genericUDAFResolver, FunctionResource... resources) {
    FunctionType functionType = isNative ? FunctionType.BUILTIN : FunctionType.TEMPORARY;
    return registerGenericUDAF(functionName, functionType, genericUDAFResolver, resources);
  }

  private FunctionInfo registerGenericUDAF(String functionName, FunctionType functionType,
      GenericUDAFResolver genericUDAFResolver, FunctionResource... resources) {
    FunctionInfo function =
        new WindowFunctionInfo(functionType, functionName, genericUDAFResolver, resources);
    addFunction(functionName, function);
    addFunction(WINDOW_FUNC_PREFIX + functionName, function);
    return function;
  }

  public FunctionInfo registerUDAF(String functionName,
      Class<? extends UDAF> udafClass, FunctionResource... resources) {
    FunctionType functionType = isNative ? FunctionType.BUILTIN : FunctionType.TEMPORARY;
    return registerUDAF(functionName, functionType, udafClass, resources);
  }

  private FunctionInfo registerUDAF(String functionName, FunctionType functionType,
      Class<? extends UDAF> udafClass, FunctionResource... resources) {
    validateClass(udafClass, UDAF.class);
    FunctionInfo function = new WindowFunctionInfo(functionType, functionName,
        new GenericUDAFBridge(ReflectionUtil.newInstance(udafClass, null)), resources);
    addFunction(functionName, function);
    addFunction(WINDOW_FUNC_PREFIX + functionName, function);
    return function;
  }

  public FunctionInfo registerTableFunction(String functionName,
      Class<? extends TableFunctionResolver> tFnCls, FunctionResource... resources) {
    FunctionType functionType = isNative ? FunctionType.BUILTIN : FunctionType.TEMPORARY;
    return registerTableFunction(functionName, functionType, tFnCls, resources);
  }

  private FunctionInfo registerTableFunction(String functionName, FunctionType functionType,
      Class<? extends TableFunctionResolver> tFnCls, FunctionResource... resources) {
    validateClass(tFnCls, TableFunctionResolver.class);
    FunctionInfo function = new FunctionInfo(functionType, functionName, tFnCls, resources);
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
    FunctionInfo fI = new FunctionInfo(FunctionType.TEMPORARY, macroName, macro, resources);
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
    } else {
        addFunction(functionName, function);
    }
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
    FunctionType functionType = isNative ? FunctionType.BUILTIN : FunctionType.TEMPORARY;
    addFunction(WINDOW_FUNC_PREFIX + name, new WindowFunctionInfo(functionType, name, wFn, null));
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
  public FunctionInfo getFunctionInfo(String functionName) throws SemanticException {
    lock.lock();
    try {
      functionName = functionName.toLowerCase();
      if (FunctionUtils.isQualifiedFunctionName(functionName)) {
        return getQualifiedFunctionInfoUnderLock(functionName);
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
        functionInfo = getQualifiedFunctionInfoUnderLock(qualifiedName);
      }
    return functionInfo;
    } finally {
      lock.unlock();
    }

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

  public boolean isPermanentFunc(Class<?> udfClass) {
    // Note that permanent functions can only be properly checked from the session registry.
    // If permanent functions are read from the metastore during Hive initialization,
    // the JARs are not loaded for the UDFs during that time and so Hive is unable to instantiate
    // the UDf classes to add to the persistent functions set.
    // Once a permanent UDF has been referenced in a session its FunctionInfo should be registered
    // in the session registry (and persistent set updated), so it can be looked up there.
    return udfClass != null && persistent.containsKey(udfClass);
  }

  public Set<String> getCurrentFunctionNames() {
    lock.lock();
    try {
      return getFunctionNames((Pattern)null);
    } finally {
      lock.unlock();
    }
  }

  public Set<String> getFunctionNames(String funcPatternStr) {
    lock.lock();
    try {
      return getFunctionNames(Pattern.compile(funcPatternStr));
    } catch (PatternSyntaxException e) {
      return Collections.emptySet();
    } finally {
      lock.unlock();
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
  public Set<String> getFunctionNames(Pattern funcPattern) {
    lock.lock();
    try {
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
    } finally {
      lock.unlock();
    }
  }

  /**
   * Adds to the set of synonyms of the supplied function.
   * @param funcName
   * @param funcInfo
   * @param synonyms
   */
  public void getFunctionSynonyms(
      String funcName, FunctionInfo funcInfo, Set<String> synonyms) throws SemanticException {
    lock.lock();
    try {
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
    } finally {
      lock.unlock();
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
      List<ObjectInspector> argumentOIs, boolean isWindowing, boolean isDistinct,
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
            args, isWindowing, isDistinct, isAllColumns);
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
      return getGenericUDAFEvaluator(functionName, argumentOIs, true, isDistinct, isAllColumns);
    }

    // this must be lead/lag UDAF
    ObjectInspector args[] = new ObjectInspector[argumentOIs.size()];
    GenericUDAFResolver udafResolver = info.getGenericUDAFResolver();
    GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(
        argumentOIs.toArray(args), true, isDistinct, isAllColumns);
    return ((GenericUDAFResolver2) udafResolver).getEvaluator(paramInfo);
  }

  private void addFunction(String functionName, FunctionInfo function) {
    lock.lock();
    try {
      // Built-in functions shouldn't go in the session registry,
      // and temp functions shouldn't go in the system registry.
      // Persistent functions can be in either registry.
      if ((!isNative && function.isBuiltIn()) || (isNative && !function.isNative())) {
        throw new RuntimeException("Function " + functionName + " is not for this registry");
      }
      functionName = functionName.toLowerCase();
      FunctionInfo prev = mFunctions.get(functionName);
      if (prev != null) {
        if (isBuiltInFunc(prev.getFunctionClass())) {
          throw new RuntimeException("Function " + functionName + " is hive builtin function, " +
              "which cannot be overridden.");
        }
        prev.discarded();
      }
      mFunctions.put(functionName, function);
      if (function.isBuiltIn()) {
        builtIns.add(function.getFunctionClass());
      } else if (function.isPersistent() && !isNative) {
        // System registry should not be used to check persistent functions - see isPermanentFunc()
        Class<?> functionClass = getPermanentUdfClass(function);
        Integer refCount = persistent.get(functionClass);
        persistent.put(functionClass, Integer.valueOf(refCount == null ? 1 : refCount + 1));
      }
    } finally {
      lock.unlock();
    }
  }

  private Class<?> getPermanentUdfClass(FunctionInfo function) {
    Class<?> functionClass = function.getFunctionClass();
    if (functionClass == null) {
      // Expected for permanent UDFs at this point.
      ClassLoader loader = Utilities.getSessionSpecifiedClassLoader();
      try {
        functionClass = Class.forName(function.getClassName(), true, loader);
      } catch (ClassNotFoundException ex) {
        throw new RuntimeException(ex);
      }
    }
    return functionClass;
  }

  public void unregisterFunction(String functionName) throws HiveException {
    lock.lock();
    try {
      functionName = functionName.toLowerCase();
      FunctionInfo fi = mFunctions.get(functionName);
      if (fi != null) {
        if (fi.isBuiltIn()) {
          throw new HiveException(ErrorMsg.DROP_NATIVE_FUNCTION.getMsg(functionName));
        }
        mFunctions.remove(functionName);
        fi.discarded();
        if (fi.isPersistent()) {
          removePersistentFunctionUnderLock(fi);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private void removePersistentFunctionUnderLock(FunctionInfo fi) {
    Class<?> functionClass = getPermanentUdfClass(fi);
    Integer refCount = persistent.get(functionClass);
    if (refCount != null) {
      if (refCount == 1) {
        persistent.remove(functionClass);
      } else {
        persistent.put(functionClass, Integer.valueOf(refCount - 1));
      }
    }
  }

  /**
   * Unregisters all the functions belonging to the specified database
   * @param dbName database name
   * @throws HiveException
   */
  public void unregisterFunctions(String dbName) throws HiveException {
    lock.lock();
    try {
      Set<String> funcNames = getFunctionNames(dbName.toLowerCase() + "\\..*");
      for (String funcName : funcNames) {
        unregisterFunction(funcName);
      }
    } finally {
      lock.unlock();
    }
  }

  public GenericUDAFResolver getGenericUDAFResolver(String functionName) throws SemanticException {
    FunctionInfo info = getFunctionInfo(functionName);
    if (info != null) {
      return info.getGenericUDAFResolver();
    }
    return null;
  }

  private FunctionInfo getQualifiedFunctionInfoUnderLock(String qualifiedName) throws SemanticException {
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
    if (info != null || !isNative) {
      return info; // We have the UDF, or we are in the session registry (or both).
    }
    // If we are in the system registry and this feature is enabled, try to get it from metastore.
    SessionState ss = SessionState.get();
    HiveConf conf = (ss == null) ? null : ss.getConf();
    if (conf == null || !HiveConf.getBoolVar(conf, ConfVars.HIVE_ALLOW_UDF_LOAD_ON_DEMAND)) {
      return null;
    }
    // This is a little bit weird. We'll do the MS call outside of the lock. Our caller calls us
    // under lock, so we'd preserve the lock state for them; their finally block will release the
    // lock correctly. See the comment on the lock field - the locking needs to be reworked.
    lock.unlock();
    try {
      return getFunctionInfoFromMetastoreNoLock(qualifiedName, conf);
    } finally {
      lock.lock();
    }
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

      // Make sure the FunctionInfo is listed as PERSISTENT (rather than TEMPORARY)
      // when it is registered to the system registry.
      ret = SessionState.getRegistryForWrite().registerFunction(
          qualifiedName, FunctionType.PERSISTENT, udfClass, resources);
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

  public void clear() {
    lock.lock();
    try {
      if (isNative) {
        throw new IllegalStateException("System function registry cannot be cleared");
      }
      mFunctions.clear();
      builtIns.clear();
      persistent.clear();
    } finally {
      lock.unlock();
    }
  }

  public void closeCUDFLoaders() {
    lock.lock();
    try {
      try {
        for(ClassLoader loader: mSessionUDFLoaders) {
          JavaUtils.closeClassLoader(loader);
        }
      } catch (IOException ie) {
          LOG.error("Error in close loader: " + ie);
      }
      mSessionUDFLoaders.clear();
    } finally {
      lock.unlock();
    }
  }

  public void addToUDFLoaders(ClassLoader loader) {
    lock.lock();
    try {
      mSessionUDFLoaders.add(loader);
    } finally {
      lock.unlock();
    }
  }

  public void removeFromUDFLoaders(ClassLoader loader) {
    lock.lock();
    try {
      mSessionUDFLoaders.remove(loader);
    } finally {
      lock.unlock();
    }
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

  /**
   * This is called outside of the lock. Some of the methods that are called transitively by
   * this (e.g. addFunction) will take the lock again and then release it, which is ok.
   */
  private FunctionInfo getFunctionInfoFromMetastoreNoLock(String functionName, HiveConf conf) {
    try {
      String[] parts = FunctionUtils.getQualifiedFunctionNameParts(functionName);
      Function func = Hive.get(conf).getFunction(parts[0].toLowerCase(), parts[1]);
      if (func == null) {
        return null;
      }
      // Found UDF in metastore - now add it to the function registry.
      FunctionInfo fi = registerPermanentFunction(functionName, func.getClassName(), true,
          FunctionTask.toFunctionResource(func.getResourceUris()));
      if (fi == null) {
        LOG.error(func.getClassName() + " is not a valid UDF class and was not registered");
        return null;
      }
      return fi;
    } catch (Throwable e) {
      LOG.info("Unable to look up " + functionName + " in metastore", e);
    }
    return null;
  }
}
