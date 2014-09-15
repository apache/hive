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
package org.apache.hadoop.hive.serde2.objectinspector;


import java.lang.reflect.Method;
import java.lang.reflect.Type;

public class ThriftObjectInspectorUtils {

  /**
   * Returns generic type for a field in a Thrift class. The type is the return
   * type for the accessor method for the field (e.g. <code>isFieldName()</code>
   * for a boolean type or <code>getFieldName</code> for other types). The return
   * type works for both structs and unions. Reflecting directly based on
   * fields does not work for unions.
   *
   * @return generic {@link Type} of the thrift field.
   */
  public static Type getFieldType(Class<?> containingClass, String fieldName) {

    String suffix = // uppercase first letter
      fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);

    // look for getFieldName() or isFieldName()

    for(String prefix : new String[]{"get", "is"}) {
      try {
        Method method = containingClass.getDeclaredMethod(prefix + suffix);
        return method.getGenericReturnType();
      } catch (NoSuchMethodException e) {
      }
    }

    // look for bean style accessors get_fieldName and is_fieldName

    for(String prefix : new String[]{"get_", "is_"}) {
      try {
        Method method = containingClass.getDeclaredMethod(prefix + fieldName);
        return method.getGenericReturnType();
      } catch (NoSuchMethodException e) {
      }
    }

    throw new RuntimeException("Could not find type for " + fieldName +
                                       " in " + containingClass);
  }

}
