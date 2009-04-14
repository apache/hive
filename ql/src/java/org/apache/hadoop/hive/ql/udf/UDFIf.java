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

package org.apache.hadoop.hive.ql.udf;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.AmbiguousMethodException;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFMethodResolver;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * UDF Class for SQL construct "IF".
 */
public class UDFIf extends UDF {

  private static Log LOG = LogFactory.getLog(UDFIf.class.getName());

  
  public UDFIf() {
    super(null);
    setResolver(new UDFIfMethodResolver(this.getClass()));
  }

  /**
   * Method Resolver for SQL construct "IF".
   * This method resolver follows the type determination process:
   * 
   * 1. If valueTrue or valueFalse is a String, then result is String
   * 2. If valueTrue or valueFalse is a Double, then result is Double
   * 3. If valueTrue or valueFalse is a Long, then result is a Long
   * 4. If valueTrue or valueFalse is a Integer, then result is a Integer
   * 5. If valueTrue or valueFalse is a Short, then result is a Short
   * 6. If valueTrue or valueFalse is a Byte, then result is a Byte
   * 7. If valueTrue or valueFalse is a Boolean, then result is a Boolean
   * 
   * This mimics the process from MySQL http://dev.mysql.com/doc/refman/5.0/en/control-flow-functions.html#function_if
   */  
  public static class UDFIfMethodResolver implements UDFMethodResolver {

    /**
     * The udfclass for which resolution is needed.
     */
    Class<? extends UDF> udfClass;
    
    static TypeInfo[] classPriority = {
      TypeInfoFactory.stringTypeInfo, 
      TypeInfoFactory.doubleTypeInfo, 
      TypeInfoFactory.floatTypeInfo,
      TypeInfoFactory.longTypeInfo,
      TypeInfoFactory.intTypeInfo,
      TypeInfoFactory.shortTypeInfo,
      TypeInfoFactory.byteTypeInfo,
      TypeInfoFactory.booleanTypeInfo,
      TypeInfoFactory.voidTypeInfo};
    
    /**
     * Constuctor.
     */
    public UDFIfMethodResolver(Class<? extends UDF> udfClass) {
      this.udfClass = udfClass;
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.hive.ql.exec.UDFMethodResolver#getEvalMethod(java.util.List)
     */
    @Override
    public Method getEvalMethod(List<TypeInfo> argTypeInfos)
    throws AmbiguousMethodException {
      
      if (argTypeInfos.size() != 3) {
        return null;
      }
      
      List<TypeInfo> pClasses = new ArrayList<TypeInfo>(3);
      pClasses.add(TypeInfoFactory.booleanTypeInfo);

      for(int i=0; i<classPriority.length; i++) {
        if (argTypeInfos.get(1).equals(classPriority[i]) ||
            argTypeInfos.get(2).equals(classPriority[i])) {
          pClasses.add(classPriority[i]);
          pClasses.add(classPriority[i]);
          break;
        }
      }
      if (pClasses.size() != 3) {
        return null;
      }
      
      Method udfMethod = null;

      for(Method m: Arrays.asList(udfClass.getMethods())) {
        if (m.getName().equals("evaluate")) {

          List<TypeInfo> acceptedTypeInfos = TypeInfoUtils.getParameterTypeInfos(m);

          boolean match = (acceptedTypeInfos.size() == pClasses.size());

          for(int i=0; i<pClasses.size() && match; i++) {
            TypeInfo accepted = acceptedTypeInfos.get(i);
            if (!accepted.equals(pClasses.get(i))) {
              match = false;
            }
          }

          if (match) {
            if (udfMethod != null) {
              throw new AmbiguousMethodException(udfClass, argTypeInfos);
            }
            else {
              udfMethod = m;
            }
          }
        }
      }
      return udfMethod;      
    }
  }
  
  /**
   * Method for SQL construct "IF(test,valueTrue,valueFalse)"
   */
  public Text evaluate(BooleanWritable test, Text valueTrue, Text valueFalse)  {
    if (test != null && test.get()) {
      return valueTrue;
    } else {
      return valueFalse;
    }
  }
  
  /**
   * Method for SQL construct "IF(test,valueTrue,valueFalse)"
   */
  public DoubleWritable evaluate(BooleanWritable test, DoubleWritable valueTrue, DoubleWritable valueFalse)  {
    if (test != null && test.get()) {
      return valueTrue;
    } else {
      return valueFalse;
    }
  }
  
  /**
   * Method for SQL construct "IF(test,valueTrue,valueFalse)"
   */
  public FloatWritable evaluate(BooleanWritable test, FloatWritable valueTrue, FloatWritable valueFalse)  {
    if (test != null && test.get()) {
      return valueTrue;
    } else {
      return valueFalse;
    }
  }
  
  /**
   * Method for SQL construct "IF(test,valueTrue,valueFalse)"
   */
  public ByteWritable evaluate(BooleanWritable test, ByteWritable valueTrue, ByteWritable valueFalse)  {
    if (test != null && test.get()) {
      return valueTrue;
    } else {
      return valueFalse;
    }
  }

  /**
   * Method for SQL construct "IF(test,valueTrue,valueFalse)"
   */
  public ShortWritable evaluate(BooleanWritable test, ShortWritable valueTrue, ShortWritable valueFalse)  {
    if (test != null && test.get()) {
      return valueTrue;
    } else {
      return valueFalse;
    }
  }

  /**
   * Method for SQL construct "IF(test,valueTrue,valueFalse)"
   */
  public IntWritable evaluate(BooleanWritable test, IntWritable valueTrue, IntWritable valueFalse)  {
    if (test != null && test.get()) {
      return valueTrue;
    } else {
      return valueFalse;
    }
  }
  
  /**
   * Method for SQL construct "IF(test,valueTrue,valueFalse)"
   */
  public LongWritable evaluate(BooleanWritable test, LongWritable valueTrue, LongWritable valueFalse)  {
    if (test != null && test.get()) {
      return valueTrue;
    } else {
      return valueFalse;
    }
  }

  /**
   * Method for SQL construct "IF(test,valueTrue,valueFalse)"
   */
  public BooleanWritable evaluate(BooleanWritable test, BooleanWritable valueTrue, BooleanWritable valueFalse)  {
    if (test != null && test.get()) {
      return valueTrue;
    } else {
      return valueFalse;
    }
  }

  /**
   * Method for SQL construct "IF(test,valueTrue,valueFalse)"
   */
  public NullWritable evaluate(BooleanWritable test, NullWritable valueTrue, NullWritable valueFalse)  {
    if (test != null && test.get()) {
      return valueTrue;
    } else {
      return valueFalse;
    }
  }
  
  
}
