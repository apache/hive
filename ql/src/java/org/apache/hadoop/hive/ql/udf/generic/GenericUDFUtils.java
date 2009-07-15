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

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Util functions for GenericUDF classes.
 */
public class GenericUDFUtils {

  private static Log LOG = LogFactory.getLog(GenericUDFUtils.class.getName());

  /**
   * Checks if b is the first byte of a UTF-8 character.
   * 
   */
  public static boolean isUtfStartByte(byte b) {
    return (b & 0xC0) != 0x80;
  }
  
  /**
   * This class helps to find the return ObjectInspector for a GenericUDF.
   * 
   * In many cases like CASE and IF, the GenericUDF is returning a value out
   * of several possibilities.  However these possibilities may not always 
   * have the same ObjectInspector.
   * 
   * This class will help detect whether all possibilities have exactly the
   * same ObjectInspector.  If not, then we need to convert the Objects to
   * the same ObjectInspector.
   * 
   * A special case is when some values are constant NULL. In this case we 
   * can use the same ObjectInspector.
   */
  public static class ReturnObjectInspectorResolver {

    boolean allowTypeConversion;
    ObjectInspector returnObjectInspector;
    
    // We create converters beforehand, so that the converters can reuse the 
    // same object for returning conversion results. 
    HashMap<ObjectInspector, Converter> converters;
    
    public ReturnObjectInspectorResolver() {
      this(false);
    }

    public ReturnObjectInspectorResolver(boolean allowTypeConversion) {
      this.allowTypeConversion = allowTypeConversion;
    }
    /**
     * Update returnObjectInspector and valueInspectorsAreTheSame based on the
     * ObjectInspector seen.
     * @return false if there is a type mismatch
     */
    public boolean update(ObjectInspector oi)
        throws UDFArgumentTypeException {
      if (oi instanceof VoidObjectInspector) {
        return true;
      }
      
      if (returnObjectInspector == null) {
        // The first argument, just set it.
        returnObjectInspector = oi;
        return true;
      }
      
      if (returnObjectInspector == oi) {
        // The new ObjectInspector is the same as the old one, directly return true
        return true;
      }
      
      TypeInfo oiTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(oi);
      TypeInfo rTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(returnObjectInspector); 
      if (oiTypeInfo == rTypeInfo) {
        // Convert everything to writable, if types of arguments are the same,
        // but ObjectInspectors are different.
        returnObjectInspector = ObjectInspectorUtils.getStandardObjectInspector(returnObjectInspector,
            ObjectInspectorCopyOption.WRITABLE);
        return true;
      }
      
      if (!allowTypeConversion) {
        return false;
      }
      
      // Types are different, we need to check whether we can convert them to 
      // a common base class or not.
      TypeInfo commonTypeInfo = FunctionRegistry.getCommonClass(oiTypeInfo, rTypeInfo);
      if (commonTypeInfo == null) {
        return false;
      }

      returnObjectInspector = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(commonTypeInfo);
      
      return true;
    }
    
    /**
     * Returns the ObjectInspector of the return value.
     */
    public ObjectInspector get() {
      return returnObjectInspector;
    }
    
    /**
     * Convert the return Object if necessary (when the ObjectInspectors of
     * different possibilities are not all the same).
     */
    public Object convertIfNecessary(Object o, ObjectInspector oi) {
      Object converted = null;
      if (oi == returnObjectInspector) {
        converted = o;
      } else {

        if (o == null) {
          return null;
        }
        
        if (converters == null) {
          converters = new HashMap<ObjectInspector, Converter>();
        }
        
        Converter converter = converters.get(oi);
        if (converter == null) {
          converter = ObjectInspectorConverters.getConverter(oi, returnObjectInspector);
          converters.put(oi, converter);
        }
        converted = converter.convert(o);
      }
      return converted;
    }
    
  }
  
  /**
   * Convert primitive parameters between Java and Writable when needed. 
   */
  public static class PrimitiveConversionHelper {

    private Method m;
    private ObjectInspector[] parameters;
    
    Converter[] converters; 
    Object[] convertedParameters;
    
    /**
     * Create a PrimitiveConversionHelper for Method m.  The ObjectInspector's
     * input parameters are specified in parameters.
     */
    public PrimitiveConversionHelper(Method m, ObjectInspector[] parameters) {
      this.m = m;
      this.parameters = parameters;
      
      Type[] acceptedParameters = m.getGenericParameterTypes();
      assert(parameters.length == acceptedParameters.length);
      
      for (int i = 0; i < parameters.length; i++) {
        ObjectInspector acceptedParameterOI = PrimitiveObjectInspectorFactory
            .getPrimitiveObjectInspectorFromClass((Class<?>)acceptedParameters[i]);
        Converter pc = ObjectInspectorConverters
            .getConverter(parameters[i], acceptedParameterOI);
        // Conversion is needed?
        if (pc != null) {
          if (converters == null) {
            // init converters only if needed.
            converters = new Converter[parameters.length];
            convertedParameters = new Object[parameters.length];
          }
          converters[i] = pc;
        }
      }
    }
    
    public Object[] convertIfNecessary(Object... parameters) {
      if (converters == null) {
        return parameters;
      } else {
        assert(parameters.length == convertedParameters.length);
        for(int i = 0; i < parameters.length; i++) {
          convertedParameters[i] =
            converters[i] == null 
            ? parameters[i]
            : converters[i].convert(parameters[i]); 
        }
        return convertedParameters;
      }
    }
  };
  
}
