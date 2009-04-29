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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Util functions for GenericUDF classes.
 */
public class GenericUDFUtils {

  private static Log LOG = LogFactory.getLog(GenericUDFUtils.class.getName());


  /**
   * This class helps to find the return ObjectInspector for a GenericUDF.
   * 
   * In many cases like CASE and IF, the GenericUDF is returning a value out
   * of several possibilities.  However these possibilities may not always 
   * have the same ObjectInspector, although they should have the same 
   * TypeInfo.
   * 
   * This class will help detect whether all possibilities have exactly the
   * same ObjectInspector.  If not, then we need to convert the Objects to
   * the same ObjectInspector.
   * 
   * A special case is when some values are constant NULL. In this case we 
   * can use the same ObjectInspector.
   */
  public static class ReturnObjectInspectorResolver {
    boolean valueInspectorsAreTheSame;
    ObjectInspector returnObjectInspector;
    
    ReturnObjectInspectorResolver() {
      valueInspectorsAreTheSame = true;
    }
    /**
     * Update returnObjectInspector and valueInspectorsAreTheSame based on the
     * ObjectInspector seen.
     * @return false if there is a type mismatch
     */
    public boolean update(ObjectInspector oi)
        throws UDFArgumentTypeException {
      if (!(oi instanceof VoidObjectInspector)) {
        if (returnObjectInspector == null) {
          returnObjectInspector = oi;
        } else if (TypeInfoUtils.getTypeInfoFromObjectInspector(oi)
            != TypeInfoUtils.getTypeInfoFromObjectInspector(returnObjectInspector)) {
          System.out.println(TypeInfoUtils.getTypeInfoFromObjectInspector(oi).getTypeName());
          System.out.println(TypeInfoUtils.getTypeInfoFromObjectInspector(returnObjectInspector).getTypeName());
          return false;
        } else {
          valueInspectorsAreTheSame = valueInspectorsAreTheSame &&
              oi == returnObjectInspector;
        }
      }
      return true;
    }
    
    /**
     * Returns the ObjectInspector of the return value.
     */
    public ObjectInspector get() {
      return valueInspectorsAreTheSame
          ? returnObjectInspector
          : ObjectInspectorUtils.getStandardObjectInspector(returnObjectInspector,
              ObjectInspectorCopyOption.WRITABLE);
    }
    
    /**
     * Convert the return Object if necessary (when the ObjectInspectors of
     * different possibilities are not all the same).
     */
    public Object convertIfNecessary(Object o, ObjectInspector oi) {
      if (valueInspectorsAreTheSame || oi instanceof VoidObjectInspector) {
        return o;
      } else {
        return ObjectInspectorUtils.copyToStandardObject(
            o, oi, ObjectInspectorCopyOption.WRITABLE);
      }   
    }
    
  }
  
  /**
   * This class helps to make sure the TypeInfo of different possibilities
   * of the return values are all the same. 
   */
  public static class ReturnTypeInfoResolver {
    
    TypeInfo returnTypeInfo = null;
    /**
     * Update the return TypeInfo based on the new value TypeInfo.
     * @return  false if there is a type mismatch
     */
    public boolean updateReturnTypeInfo(TypeInfo newValueTypeInfo) 
        throws UDFArgumentTypeException {
      if (newValueTypeInfo == TypeInfoFactory.voidTypeInfo) {
        // do nothing
      } else if (returnTypeInfo == null) {
        returnTypeInfo = newValueTypeInfo;
      } else if (returnTypeInfo != newValueTypeInfo) {
        return false;
      } else {
        // do nothing
      }
      return true;
    }
    
    public TypeInfo getReturnTypeInfo() {
      return returnTypeInfo;
    }
    
  }
  
  
  
}
