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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.ql.exec.FunctionInfo.FunctionResource;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionResolver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Function related utilities.
 */
public final class FunctionUtils {
  private FunctionUtils() {
    throw new UnsupportedOperationException("FunctionUtils should not be instantiated");
  }

  /**
   * Extracts the UDAFE evaluators of the specified class from the provided aggregations.
   */
  public static <T extends GenericUDAFEvaluator> List<T> extractEvaluators(
      Collection<? extends AggregationDesc> aggregations, Class<T> clazz) {
    List<T> result = new ArrayList<>();
    for (AggregationDesc d : aggregations) {
      if (clazz.isInstance(d.getGenericUDAFEvaluator())) {
        @SuppressWarnings("unchecked")
        T t = (T) d.getGenericUDAFEvaluator();
        result.add(t);
      }
    }
    return result;
  }

  public static FunctionResource[] toFunctionResource(List<ResourceUri> resources) throws HiveException {
    if (resources == null) {
      return null;
    }

    FunctionResource[] converted = new FunctionResource[resources.size()];
    for (int i = 0; i < converted.length; i++) {
      ResourceUri resource = resources.get(i);
      SessionState.ResourceType type = getResourceType(resource.getResourceType());
      converted[i] = new FunctionResource(type, resource.getUri());
    }
    return converted;
  }

  public static void addFunctionResources(FunctionResource[] resources) throws HiveException {
    if (resources != null) {
      Multimap<SessionState.ResourceType, String> mappings = HashMultimap.create();
      for (FunctionResource res : resources) {
        mappings.put(res.getResourceType(), res.getResourceURI());
      }
      for (SessionState.ResourceType type : mappings.keys()) {
        SessionState.get().add_resources(type, mappings.get(type));
      }
    }
  }

  public static SessionState.ResourceType getResourceType(ResourceType rt) {
    switch (rt) {
    case JAR:
      return SessionState.ResourceType.JAR;
    case FILE:
      return SessionState.ResourceType.FILE;
    case ARCHIVE:
      return SessionState.ResourceType.ARCHIVE;
    default:
      throw new AssertionError("Unexpected resource type " + rt);
    }
  }

  public static boolean isQualifiedFunctionName(String functionName) {
    return functionName.indexOf('.') >= 0;
  }

  public static String qualifyFunctionName(String functionName, String dbName) {
    if (isQualifiedFunctionName(functionName)) {
      return functionName;
    }
    return dbName + "." + functionName;
  }

  /**
   * Splits a qualified function name into an array containing the database name and function name.
   * If the name is not qualified, the database name is null.
   * If there is more than one '.', an exception will be thrown.
   * @param functionName Function name, which may or may not be qualified
   * @return
   */
  public static String[] splitQualifiedFunctionName(String functionName) throws HiveException {
    String[] names = functionName.split("\\.");
    if (names.length == 1) {
      String[] retval = { null, functionName };
      return retval;
    } else if (names.length > 2) {
      throw new HiveException("Function name does not have correct format: " + functionName);
    }
    // Remove the WINDOW_FUNC_PREFIX prefix, as that is not part of the database name.
    if (names[0].startsWith(Registry.WINDOW_FUNC_PREFIX)) {
      names[0] = names[0].substring(Registry.WINDOW_FUNC_PREFIX.length());
    }
    return names;
  }

  public static String[] getQualifiedFunctionNameParts(String name) throws HiveException {
    if (isQualifiedFunctionName(name)) {
      return splitQualifiedFunctionName(name);
    }
    String dbName = SessionState.get().getCurrentDatabase();
    return new String[] { dbName, name };
  }

  /**
   * Function type, for permanent functions.
   * Currently just JAVA, though we could support Groovy later on.
   */
  public enum FunctionType {
    JAVA,
  }

  /**
   * Enum type to describe what kind of UDF implementation class
   */
  public enum UDFClassType {
    UNKNOWN,
    UDF,
    GENERIC_UDF,
    GENERIC_UDTF,
    UDAF,
    GENERIC_UDAF_RESOLVER,
    TABLE_FUNCTION_RESOLVER,
  }

  /**
   * Determine the UDF class type of the class
   * @param udfClass
   * @return UDFClassType enum corresponding to the class type of the UDF
   */
  public static UDFClassType getUDFClassType(Class<?> udfClass) {
    if (UDF.class.isAssignableFrom(udfClass)) {
      return UDFClassType.UDF;
    } else if (GenericUDF.class.isAssignableFrom(udfClass)) {
      return UDFClassType.GENERIC_UDF;
    } else if (GenericUDTF.class.isAssignableFrom(udfClass)) {
      return UDFClassType.GENERIC_UDTF;
    } else if (UDAF.class.isAssignableFrom(udfClass)) {
      return UDFClassType.UDAF;
    } else if (GenericUDAFResolver.class.isAssignableFrom(udfClass)) {
      return UDFClassType.GENERIC_UDAF_RESOLVER;
    } else if(TableFunctionResolver.class.isAssignableFrom(udfClass)) {
      return UDFClassType.TABLE_FUNCTION_RESOLVER;
    } else {
      return UDFClassType.UNKNOWN;
    }
  }
}
