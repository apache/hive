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

import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionResolver;
import org.apache.hadoop.hive.ql.udf.ptf.WindowingTableFunction;
import org.apache.hive.common.util.AnnotationUtils;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * FunctionInfo.
 *
 */
public class FunctionInfo {

  public static enum FunctionType {
    BUILTIN, PERSISTENT, TEMPORARY;
  }

  private final FunctionType functionType;

  private final boolean isInternalTableFunction;

  private final String displayName;

  private final FunctionResource[] resources;

  private String className;

  protected GenericUDF genericUDF;

  protected GenericUDTF genericUDTF;

  private GenericUDAFResolver genericUDAFResolver;

  private Class<? extends TableFunctionResolver>  tableFunctionResolver;

  private boolean blockedFunction;

  // for persistent function
  // if the function is dropped, all functions registered to sessions are needed to be reloaded
  private AtomicBoolean discarded;

  public FunctionInfo(String displayName, String className, FunctionResource... resources) {
    this.functionType = FunctionType.PERSISTENT;
    this.displayName = displayName;
    this.className = className;
    this.isInternalTableFunction = false;
    this.resources = resources;
    this.discarded = new AtomicBoolean(false);  // shared to all session functions
  }

  public FunctionInfo(FunctionType functionType, String displayName,
      GenericUDF genericUDF, FunctionResource... resources) {
    this.functionType = functionType;
    this.displayName = displayName;
    this.genericUDF = genericUDF;
    this.isInternalTableFunction = false;
    this.resources = resources;
  }

  public FunctionInfo(FunctionType functionType, String displayName,
      GenericUDAFResolver genericUDAFResolver, FunctionResource... resources) {
    this.functionType = functionType;
    this.displayName = displayName;
    this.genericUDAFResolver = genericUDAFResolver;
    this.isInternalTableFunction = false;
    this.resources = resources;
  }

  public FunctionInfo(FunctionType functionType, String displayName,
      GenericUDTF genericUDTF, FunctionResource... resources) {
    this.functionType = functionType;
    this.displayName = displayName;
    this.genericUDTF = genericUDTF;
    this.isInternalTableFunction = false;
    this.resources = resources;
  }

  public FunctionInfo(FunctionType functionType, String displayName, Class<? extends TableFunctionResolver> tFnCls,
      FunctionResource... resources) {
    this.functionType = functionType;
    this.displayName = displayName;
    this.tableFunctionResolver = tFnCls;
    PartitionTableFunctionDescription def = AnnotationUtils.getAnnotation(
        tableFunctionResolver, PartitionTableFunctionDescription.class);
    this.isInternalTableFunction = def != null && def.isInternal();
    this.resources = resources;
  }

  public FunctionInfo(FunctionType functionType, String displayName, Class<? extends TableFunctionResolver> tFnCls,
      GenericUDF genericUDF, GenericUDTF genericUDTF, GenericUDAFResolver genericUDAFResolver,
      String className, FunctionResource... resources) {
    this.functionType = functionType;
    this.displayName = displayName;
    this.tableFunctionResolver = tFnCls;
    PartitionTableFunctionDescription def =
        (tableFunctionResolver != null)
	    ? AnnotationUtils.getAnnotation(
                tableFunctionResolver, PartitionTableFunctionDescription.class)
            : null;
    this.genericUDF = genericUDF;
    this.genericUDTF = genericUDTF;
    this.genericUDAFResolver = genericUDAFResolver;
    this.className = className;
    this.isInternalTableFunction = def != null && def.isInternal();
    this.resources = resources;
  }
  /**
   * Get a new GenericUDF object for the function.
   */
  public GenericUDF getGenericUDF() {
    // GenericUDF is stateful - we have to make a copy here
    if (genericUDF == null) {
      return null;
    }
    return FunctionRegistry.cloneGenericUDF(genericUDF);
  }

  /**
   * Get a new GenericUDTF object for the function.
   */
  public GenericUDTF getGenericUDTF() {
    // GenericUDTF is stateful too, copy
    if (genericUDTF == null) {
      return null;
    }
    return FunctionRegistry.cloneGenericUDTF(genericUDTF);
  }

  /**
   * Get the GenericUDAFResolver object for the function.
   */
  public GenericUDAFResolver getGenericUDAFResolver() {
    return genericUDAFResolver;
  }



  /**
   * Get the Class of the UDF.
   */
  public Class<?> getFunctionClass() {
    if (isGenericUDF()) {
      if (genericUDF instanceof GenericUDFBridge) {
        return ((GenericUDFBridge) genericUDF).getUdfClass();
      } else {
        return genericUDF.getClass();
      }
    } else if (isGenericUDAF()) {
      if (genericUDAFResolver instanceof GenericUDAFBridge) {
        return ((GenericUDAFBridge) genericUDAFResolver).getUDAFClass();
      } else {
        return genericUDAFResolver.getClass();
      }
    } else if (isGenericUDTF()) {
      return genericUDTF.getClass();
    }
    if(isTableFunction()) {
      return this.tableFunctionResolver;
    }
    return null;
  }

  /**
   * Get the display name for this function. This should be transferred into
   * exprNodeGenericUDFDesc, and will be used as the first parameter to
   * GenericUDF.getDisplayName() call, instead of hard-coding the function name.
   * This will solve the problem of displaying only one name when a udf is
   * registered under 2 names.
   */
  public String getDisplayName() {
    return displayName;
  }

  /**
   * Native functions cannot be unregistered.
   */
  public boolean isNative() {
    return functionType == FunctionType.BUILTIN || functionType == FunctionType.PERSISTENT;
  }

  /**
   * Internal table functions cannot be used in the language.
   * {@link WindowingTableFunction}
   */
  public boolean isInternalTableFunction() {
    return isInternalTableFunction;
  }

  /**
   * @return TRUE if the function is a GenericUDF
   */
  public boolean isGenericUDF() {
    return null != genericUDF;
  }

  /**
   * @return TRUE if the function is a GenericUDAF
   */
  public boolean isGenericUDAF() {
    return null != genericUDAFResolver;
  }

  /**
   * @return TRUE if the function is a GenericUDTF
   */
  public boolean isGenericUDTF() {
    return null != genericUDTF;
  }

  public Class<? extends TableFunctionResolver>  getTableFunctionResolver() {
    return tableFunctionResolver;
  }

  /**
   * @return TRUE if the function is a Table Function
   */
  public boolean isTableFunction() {
    return null != tableFunctionResolver;
  }

  public boolean isBlockedFunction() {
    return blockedFunction;
  }

  public void setBlockedFunction(boolean blockedFunction) {
    this.blockedFunction = blockedFunction;
  }

  public boolean isBuiltIn() {
    return functionType == FunctionType.BUILTIN;
  }

  public boolean isPersistent() {
    return functionType == FunctionType.PERSISTENT;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public FunctionResource[] getResources() {
    return resources;
  }

  public void discarded() {
    if (discarded != null) {
      discarded.set(true);
    }
  }

  // for persistent function
  public boolean isDiscarded() {
    return discarded != null && discarded.get();
  }

  // for persistent function
  public void shareStateWith(FunctionInfo function) {
    if (function != null) {
      function.discarded = discarded;
    }
  }

  public FunctionType getFunctionType() {
    return functionType;
  }

  public static class FunctionResource {
    private final SessionState.ResourceType resourceType;
    private final String resourceURI;
    public FunctionResource(SessionState.ResourceType resourceType, String resourceURI) {
      this.resourceType = resourceType;
      this.resourceURI = resourceURI;
    }
    public SessionState.ResourceType getResourceType() {
      return resourceType;
    }
    public String getResourceURI() {
      return resourceURI;
    }
    @Override
    public String toString() {
      return resourceType + ":" + resourceURI;
    }
  }
}
