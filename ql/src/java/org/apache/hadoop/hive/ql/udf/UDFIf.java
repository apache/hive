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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

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
    
    static Class[] classPriority = {String.class, Double.class, Long.class,
      Integer.class, Short.class, Byte.class, Boolean.class, Void.class};
    
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
    public Method getEvalMethod(List<Class<?>> argClasses)
    throws AmbiguousMethodException {
      
      if (argClasses.size() != 3) {
        return null;
      }
      
      List<Class<?>> pClasses = new ArrayList<Class<?>>(3);
      pClasses.add(Boolean.class);

      for(int i=0; i<classPriority.length; i++) {
        if (ObjectInspectorUtils.generalizePrimitive(argClasses.get(1)) == classPriority[i] ||
            ObjectInspectorUtils.generalizePrimitive(argClasses.get(2)) == classPriority[i]) {
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

          Class<?>[] argumentTypeInfos = m.getParameterTypes();

          boolean match = (argumentTypeInfos.length == pClasses.size());

          for(int i=0; i<pClasses.size() && match; i++) {
            Class<?> accepted = ObjectInspectorUtils.generalizePrimitive(argumentTypeInfos[i]);
            if (accepted != pClasses.get(i)) {
              match = false;
            }
          }

          if (match) {
            if (udfMethod != null) {
              throw new AmbiguousMethodException(udfClass, argClasses);
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
  public String evaluate(Boolean test, String valueTrue, String valueFalse)  {
    if (Boolean.TRUE.equals(test)) {
      return valueTrue;
    } else {
      return valueFalse;
    }
  }
  
  /**
   * Method for SQL construct "IF(test,valueTrue,valueFalse)"
   */
  public Double evaluate(Boolean test, Double valueTrue, Double valueFalse)  {
    if (Boolean.TRUE.equals(test)) {
      return valueTrue;
    } else {
      return valueFalse;
    }
  }
  
  /**
   * Method for SQL construct "IF(test,valueTrue,valueFalse)"
   */
  public Byte evaluate(Boolean test, Byte valueTrue, Byte valueFalse)  {
    if (Boolean.TRUE.equals(test)) {
      return valueTrue;
    } else {
      return valueFalse;
    }
  }

  /**
   * Method for SQL construct "IF(test,valueTrue,valueFalse)"
   */
  public Short evaluate(Boolean test, Short valueTrue, Short valueFalse)  {
    if (Boolean.TRUE.equals(test)) {
      return valueTrue;
    } else {
      return valueFalse;
    }
  }

  /**
   * Method for SQL construct "IF(test,valueTrue,valueFalse)"
   */
  public Integer evaluate(Boolean test, Integer valueTrue, Integer valueFalse)  {
    if (Boolean.TRUE.equals(test)) {
      return valueTrue;
    } else {
      return valueFalse;
    }
  }
  
  /**
   * Method for SQL construct "IF(test,valueTrue,valueFalse)"
   */
  public Long evaluate(Boolean test, Long valueTrue, Long valueFalse)  {
    if (Boolean.TRUE.equals(test)) {
      return valueTrue;
    } else {
      return valueFalse;
    }
  }

  /**
   * Method for SQL construct "IF(test,valueTrue,valueFalse)"
   */
  public Boolean evaluate(Boolean test, Boolean valueTrue, Boolean valueFalse)  {
    if (Boolean.TRUE.equals(test)) {
      return valueTrue;
    } else {
      return valueFalse;
    }
  }

  /**
   * Method for SQL construct "IF(test,valueTrue,valueFalse)"
   */
  public Void evaluate(Boolean test, Void valueTrue, Void valueFalse)  {
    if (Boolean.TRUE.equals(test)) {
      return valueTrue;
    } else {
      return valueFalse;
    }
  }
  
  
}
