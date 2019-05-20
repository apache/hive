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
package org.apache.hadoop.hive;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.junit.*;

import static org.junit.Assert.*;

public class VersionTestBase {

  public String getParameterTypeName(Class<?> parameterType,
      Map<Class, String> versionedClassToNameMap) {
    if (versionedClassToNameMap.containsKey(parameterType)) {
      return versionedClassToNameMap.get(parameterType);
    } else {
      return parameterType.getSimpleName();
    }
  }

  public String getMethodKey(Method method, Map<Class, String> versionedClassToNameMap) {
    //-------------------------------------------------
    StringBuilder sb = new StringBuilder();

    int modifiers = method.getModifiers();
    if ((modifiers & Modifier.STATIC) != 0) {
      sb.append("static");
    } else {
      sb.append("non-static");
    }
    sb.append(" ");
    Class<?> returnType = method.getReturnType();
    sb.append(getParameterTypeName(returnType, versionedClassToNameMap));
    sb.append(" ");
    sb.append(method.getName());
    Class<?>[] parameterTypes = method.getParameterTypes();
    sb.append("(");
    boolean isFirst = true;
    for (Class<?> parameterType : parameterTypes) {
      if (!isFirst) {
        sb.append(", ");
      }
      sb.append(getParameterTypeName(parameterType, versionedClassToNameMap));
      isFirst = false;
    }
    sb.append(")");
    Class<?>[] exceptionsThrown = method.getExceptionTypes();
    if (exceptionsThrown.length > 0) {
      sb.append(" throws ");
      isFirst = true;
      for (Class<?> exceptionThrown : exceptionsThrown) {
        if (!isFirst) {
          sb.append(", ");
        }
        sb.append(exceptionThrown.getSimpleName());
        isFirst = false;
      }
    }

    return sb.toString();
    //-------------------------------------------------
  }

  public String getFieldKey(Field field, Map<Class, String> versionedClassToNameMap) throws IllegalAccessException {
    //-------------------------------------------------
    StringBuilder sb = new StringBuilder();

    int modifiers = field.getModifiers();
    if ((modifiers & Modifier.STATIC) != 0) {
      sb.append("static");
    } else {
      sb.append("non-static");
    }
    sb.append(" ");
    Class<?> fieldType = field.getType();
    sb.append(getParameterTypeName(fieldType, versionedClassToNameMap));
    sb.append(" ");
    sb.append(field.getName());
    if ((modifiers & Modifier.STATIC) != 0) {
      sb.append(" ");
      sb.append(field.get(null));
    }

    return sb.toString();
    //-------------------------------------------------
  }

  public Method[] onlyPublicMethods(Method[] methods) {
    List<Method> resultList = new ArrayList<Method>();
    for (Method method : methods) {
      if ((method.getModifiers() & Modifier.PUBLIC) != 0) {
        resultList.add(method);
      }
    }
    return resultList.toArray(new Method[0]);
  }

  public Field[] onlyPublicFields(Field[] fields) {
    List<Field> resultList = new ArrayList<Field>();
    for (Field field : fields) {
      if ((field.getModifiers() & Modifier.PUBLIC) != 0) {
        resultList.add(field);
      }
    }
    return resultList.toArray(new Field[0]);
  }

  public TreeSet<String> getMethodKeySetForAnnotation(Method[] methods, Class annotationClass,
      Map<Class, String> versionedClassToNameMap)
     throws IllegalAccessException {
    TreeSet<String> result = new TreeSet<String>();

    for (Method method : methods) {
      Annotation[] declaredAnnotations = method.getDeclaredAnnotations();
      boolean isFound = false;
      for (Annotation declaredAnnotation : declaredAnnotations) {
        if (declaredAnnotation.annotationType().equals(annotationClass)) {
          isFound = true;
          break;
        }
      }
      if (!isFound) {
        continue;
      }
      result.add(getMethodKey(method, versionedClassToNameMap));
    }
    return result;
  }

  public TreeSet<String> getMethodKeySetExcludingAnnotations(Method[] methods,
      List<Class> versionAnnotations, Map<Class, String> versionedClassToNameMap)
      throws IllegalAccessException {
     TreeSet<String> result = new TreeSet<String>();

     for (Method method : methods) {
       Annotation[] declaredAnnotations = method.getDeclaredAnnotations();
       boolean isFound = false;
       for (Annotation declaredAnnotation : declaredAnnotations) {
         for (Class versionAnnotation : versionAnnotations) {
           if (declaredAnnotation.annotationType().equals(versionAnnotation)) {
             isFound = true;
             break;
           }
         }
         if (isFound) {
           break;
         }
       }
       if (isFound) {
         continue;
       }
       String methodKey = getMethodKey(method, versionedClassToNameMap);
       if (!methodKey.equals("non-static int compareTo(Object)")) {
         result.add(methodKey);
       }
     }
     return result;
   }

  public TreeSet<String> getFieldKeySetForAnnotation(Field[] fields,
      Class annotationClass, Map<Class, String> versionedClassToNameMap)
     throws IllegalAccessException {
    TreeSet<String> result = new TreeSet<String>();

    for (Field field : fields) {
      Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
      boolean isFound = false;
      for (Annotation declaredAnnotation : declaredAnnotations) {
        if (declaredAnnotation.annotationType().equals(annotationClass)) {
          isFound = true;
          break;
        }
      }
      if (!isFound) {
        continue;
      }
      result.add(getFieldKey(field, versionedClassToNameMap));
    }
    return result;
  }

  public TreeSet<String> getFieldKeySetExcludingAnnotations(Field[] fields,
      List<Class> versionAnnotations, Map<Class, String> versionedClassToNameMap)
      throws IllegalAccessException {
     TreeSet<String> result = new TreeSet<String>();

     for (Field field : fields) {
       Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
       boolean isFound = false;
       for (Annotation declaredAnnotation : declaredAnnotations) {
         for (Class versionAnnotation : versionAnnotations) {
           if (declaredAnnotation.annotationType().equals(versionAnnotation)) {
             isFound = true;
             break;
           }
         }
         if (isFound) {
           break;
         }
       }
       if (isFound) {
         continue;
       }
       result.add(getFieldKey(field, versionedClassToNameMap));
     }
     return result;
   }

  // For now, olderClass has 1 version and newerClass 2 versions...
  public void  doVerifyVersions(
      Class olderClass, Class olderVersionClass,
      Class newerClass, Class newerVersionClass,
      Map<Class, String> versionedClassToNameMap) throws IllegalAccessException {

    List<Class> olderVersionClasses = new ArrayList<Class>();
    olderVersionClasses.add(olderVersionClass);

    List<Class> newerVersionClasses = new ArrayList<Class>();
    newerVersionClasses.add(olderVersionClass);
    newerVersionClasses.add(newerVersionClass);

    //----------------------------------------------------------------------------------------------
    Method[] olderMethods = onlyPublicMethods(olderClass.getDeclaredMethods());
    TreeSet<String> olderMethodSet =
        getMethodKeySetForAnnotation(olderMethods, olderVersionClass, versionedClassToNameMap);

    TreeSet<String> olderNoMethodAnnotationsSet =
        getMethodKeySetExcludingAnnotations(olderMethods, olderVersionClasses, versionedClassToNameMap);

    Field[] olderFields = onlyPublicFields(olderClass.getFields());
    TreeSet<String> olderFieldSet =
        getFieldKeySetForAnnotation(olderFields, olderVersionClass, versionedClassToNameMap);

    TreeSet<String> olderNoFieldAnnotationsSet =
        getFieldKeySetExcludingAnnotations(olderFields, olderVersionClasses, versionedClassToNameMap);
    //----------------------------------------------------------------------------------------------

    Method[] newerMethods = onlyPublicMethods(newerClass.getDeclaredMethods());
    TreeSet<String> newerMethodSetV1 =
        getMethodKeySetForAnnotation(newerMethods, olderVersionClass, versionedClassToNameMap);
    TreeSet<String> newerMethodSetV2 =
        getMethodKeySetForAnnotation(newerMethods, newerVersionClass, versionedClassToNameMap);

    TreeSet<String> newerNoMethodAnnotationsSetV1andV2 =
        getMethodKeySetExcludingAnnotations(newerMethods, newerVersionClasses, versionedClassToNameMap);

    Field[] newerFields = onlyPublicFields(newerClass.getFields());
    // doDisplayFields(newerFields, newerClass);
    TreeSet<String> newerFieldSetV1 =
        getFieldKeySetForAnnotation(newerFields, olderVersionClass, versionedClassToNameMap);
    TreeSet<String> newerFieldSetV2 =
        getFieldKeySetForAnnotation(newerFields, newerVersionClass, versionedClassToNameMap);

    TreeSet<String> newerNoFieldAnnotationsSetV1andV2 =
        getFieldKeySetExcludingAnnotations(newerFields, newerVersionClasses, versionedClassToNameMap);

    //----------------------------------------------------------------------------------------------
    // VALIDATION
    //----------------------------------------------------------------------------------------------

    // No version annotation?
    if (olderNoMethodAnnotationsSet.size() != 0) {
      Assert.assertTrue("Class " + olderClass.getSimpleName() + " has 1 or more public methods without a version V1 annotation " +
          olderNoMethodAnnotationsSet.toString(), false);
    }
    if (olderNoFieldAnnotationsSet.size() != 0) {
      Assert.assertTrue("Class " + olderClass.getSimpleName() + " has 1 or more public fields without a version V1 annotation " +
          olderNoFieldAnnotationsSet.toString(), false);
    }
    if (newerNoMethodAnnotationsSetV1andV2.size() != 0) {
      Assert.assertTrue("Class " + newerClass.getSimpleName() + " has 1 or more public methods without a version V1 or V2 annotation " +
          newerNoMethodAnnotationsSetV1andV2.toString(), false);
    }
    if (newerNoFieldAnnotationsSetV1andV2.size() != 0) {
      Assert.assertTrue("Class " + newerClass.getSimpleName() + " has 1 or more public methods without a version V1 or V2 annotation " +
          newerNoFieldAnnotationsSetV1andV2.toString(), false);
    }

    // Do the V1 methods of older and newer match?
    if (!olderMethodSet.equals(newerMethodSetV1)) {
      TreeSet<String> leftCopy = new TreeSet<String>(olderMethodSet);
      leftCopy.removeAll(newerMethodSetV1);
      TreeSet<String> rightCopy = new TreeSet<String>(newerMethodSetV1);
      rightCopy.removeAll(olderMethodSet);
      Assert.assertTrue("Class " + olderClass.getSimpleName() + " and class " + newerClass.getSimpleName() + " methods are different for V1 " +
          leftCopy.toString() + " " + rightCopy.toString(), false);
    }

    // Do the V1 fields of older and newer match?
    if (!olderFieldSet.equals(newerFieldSetV1)) {
      TreeSet<String> leftCopy = new TreeSet<String>(olderFieldSet);
      leftCopy.removeAll(newerFieldSetV1);
      TreeSet<String> rightCopy = new TreeSet<String>(newerFieldSetV1);
      rightCopy.removeAll(olderFieldSet);
      Assert.assertTrue("Class " + olderClass.getSimpleName() + " and class " + newerClass.getSimpleName() + " fields are different for V1 " +
          leftCopy.toString() + " " + rightCopy.toString(), false);
    }
  }
}
