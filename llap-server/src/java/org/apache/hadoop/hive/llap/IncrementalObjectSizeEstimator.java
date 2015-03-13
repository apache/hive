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
package org.apache.hadoop.hive.llap;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.llap.cache.LlapCacheableBuffer;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.datanucleus.store.types.backed.LinkedList;

import com.google.common.collect.Lists;
import com.google.protobuf.UnknownFieldSet;

/**
 * Creates size estimators for java objects. The estimators attempt to do most of the reflection
 * work at initialization time, and also take some shortcuts, to minimize the amount of work done
 * during the actual estimation. TODO: clean up
 */
public class IncrementalObjectSizeEstimator {
  private static final JavaDataModel memoryModel = JavaDataModel.get();
  private enum FieldType { PRIMITIVE_ARRAY, OBJECT_ARRAY, COLLECTION, MAP, OTHER };

  public static HashMap<Class<?>, ObjectEstimator> createEstimator(Object rootObj) {
    HashMap<Class<?>, ObjectEstimator> byType = new HashMap<>();
    addHardcodedEstimators(byType);
    createEstimator(rootObj, byType);
    return byType;
  }

  public static void createEstimator(Object rootObj, HashMap<Class<?>, ObjectEstimator> byType) {
    // Code initially inspired by Google ObjectExplorer.
    Deque<Object> stack = createWorkStack(rootObj, byType);

    while (!stack.isEmpty()) {
      Object obj = stack.pop();
      Class<?> clazz;
      if (obj instanceof Class<?>) {
        clazz = (Class<?>)obj;
        obj = null;
      } else {
        clazz = obj.getClass();
      }
      ObjectEstimator estimator = byType.get(clazz);
      assert estimator != null;
      if (!estimator.isFromClass && obj == null) {
        // The object was added later for the same class; see addToProcessing.
        continue;
      }
      if (estimator.isProcessed()) continue;
      estimator.init();
      for (Field field : getAllFields(clazz, LlapCacheableBuffer.class)) {
        Class<?> fieldClass = field.getType();
        if (Modifier.isStatic(field.getModifiers())) continue;
        if (Class.class.isAssignableFrom(fieldClass)) continue;
        if (fieldClass.isPrimitive()) {
          estimator.addPrimitive(fieldClass);
          continue;
        }
        if (Enum.class.isAssignableFrom(fieldClass)) {
          estimator.addEnum();
          continue;
        }
        boolean isArray = fieldClass.isArray();
        if (isArray && fieldClass.getComponentType().isPrimitive()) {
          estimator.addField(FieldType.PRIMITIVE_ARRAY, field);
          continue;
        }
        Object fieldObj = null;
        if (obj != null) {
          fieldObj = extractFieldObj(obj, field);
          fieldClass = determineRealClass(byType, stack, field, fieldClass, fieldObj);
        }
        if (isArray) {
          estimator.addField(FieldType.OBJECT_ARRAY, field);
          addArrayEstimator(byType, stack, field, fieldObj);
        } else if (Collection.class.isAssignableFrom(fieldClass)) {
          estimator.addField(FieldType.COLLECTION, field);
          addCollectionEstimator(byType, stack, field, fieldClass, fieldObj);
        } else if (Map.class.isAssignableFrom(fieldClass)) {
          estimator.addField(FieldType.MAP, field);
          addMapEstimator(byType, stack, field, fieldClass, fieldObj);
        } else {
          estimator.addField(FieldType.OTHER, field);
          addToProcessing(byType, stack, fieldObj, fieldClass);
        }
      }
      estimator.directSize = JavaDataModel.alignUp(
          estimator.directSize, memoryModel.memoryAlign());
    }
  }

  private static Deque<Object> createWorkStack(Object rootObj,
      HashMap<Class<?>, ObjectEstimator> byType) {
    Deque<Object> stack = new ArrayDeque<Object>(32);
    Class<?> rootClass = rootObj.getClass();
    if (Class.class.equals(rootClass)) {
      rootClass = (Class<?>)rootObj;
      rootObj = null;
    }
    addToProcessing(byType, stack, rootObj, rootClass);
    return stack;
  }

  private static void addHardcodedEstimators(
      HashMap<Class<?>, ObjectEstimator> byType) {
    // Add hacks for well-known collections and maps to avoid estimating them.
    byType.put(ArrayList.class, new CollectionEstimator(memoryModel.arrayList(), 0));
    byType.put(LinkedList.class, new CollectionEstimator(
          memoryModel.linkedListBase(), memoryModel.linkedListEntry()));
    byType.put(HashSet.class, new CollectionEstimator(
        memoryModel.hashSetBase(), memoryModel.hashSetEntry()));
    byType.put(HashMap.class, new CollectionEstimator(
        memoryModel.hashMapBase(), memoryModel.hashMapEntry()));
    // Add a hack for UnknownFieldSet because we assume it will never have anything (TODO: clear?)
    ObjectEstimator ufsEstimator = new ObjectEstimator(false);
    ufsEstimator.directSize = memoryModel.object() * 2 + memoryModel.ref();
    byType.put(UnknownFieldSet.class, ufsEstimator);
    // TODO: 1-field hack for UnmodifiableCollection for protobuf too
  }

  private static Object extractFieldObj(Object obj, Field field) {
    try {
      return field.get(obj);
    } catch (IllegalAccessException e) {
      throw new AssertionError("IAE: " + field + "; " + e.getMessage());
    }
  }

  private static Class<?> determineRealClass(HashMap<Class<?>, ObjectEstimator> byType,
      Deque<Object> stack, Field field, Class<?> fieldClass, Object fieldObj) {
    if (fieldObj == null) return fieldClass;
    Class<?> realFieldClass = fieldObj.getClass();
    if (!fieldClass.equals(realFieldClass)) {
      addToProcessing(byType, stack, null, fieldClass);
      return realFieldClass;
    }
    return fieldClass;
  }

  private static void addCollectionEstimator(HashMap<Class<?>, ObjectEstimator> byType,
      Deque<Object> stack, Field field, Class<?> fieldClass, Object fieldObj) {
    Collection<?> fieldCol = null;
    if (fieldObj != null) {
      fieldCol = (Collection<?>)fieldObj;
      if (fieldCol.size() == 0) {
        fieldCol = null;
        if (DebugUtils.isTraceEnabled()) {
          LlapIoImpl.LOG.info("Empty collection " + field);
        }
      }
    }
    if (fieldCol != null) {
      for (Object element : fieldCol) {
        if (element != null) {
          addToProcessing(byType, stack, element, element.getClass());
        }
      }
    }
    Class<?> collectionArg = getCollectionArg(field);
    if (collectionArg != null) {
      addToProcessing(byType, stack, null, collectionArg);
    }
    // TODO: there was code here to create guess-estimate for collection wrt how usage changes
    //       when removing elements. However it's too error-prone for anything involving
    //       pre-allocated capacity, so it was discarded.

    // We will estimate collection as an object. 
    addToProcessing(byType, stack, fieldObj, fieldClass);
  }

  private static void addMapEstimator(HashMap<Class<?>, ObjectEstimator> byType,
      Deque<Object> stack, Field field, Class<?> fieldClass, Object fieldObj) {
    Map<?, ?> fieldCol = null;
    if (fieldObj != null) {
      fieldCol = (Map<?, ?>)fieldObj;
      if (fieldCol.size() == 0) {
        fieldCol = null;
        if (DebugUtils.isTraceEnabled()) {
          LlapIoImpl.LOG.info("Empty map " + field);
        }
      }
    }
    if (fieldCol != null) {
      for (Map.Entry<?, ?> element : fieldCol.entrySet()) {
        Object k = element.getKey(), v = element.getValue();
        if (k != null) {
          addToProcessing(byType, stack, k, k.getClass());
        }
        if (v != null) {
          addToProcessing(byType, stack, v, v.getClass());
        }
      }
    }
    Class<?>[] mapArgs = getMapArgs(field);
    if (mapArgs != null) {
      for (Class<?> mapArg : mapArgs) {
        addToProcessing(byType, stack, null, mapArg);
      }
    }
    // We will estimate map as an object.
    addToProcessing(byType, stack, fieldObj, fieldClass);
  }

  private static Class<?>[] getMapArgs(Field field) {
    // TODO: this makes many assumptions, e.g. on how generic args are done
    Type genericType = field.getGenericType();
    if (genericType instanceof ParameterizedType) {
      Type[] types = ((ParameterizedType)genericType).getActualTypeArguments();
      if (types.length == 2 && types[0] instanceof Class<?> && types[1] instanceof Class<?>) {
        return new Class<?>[] { (Class<?>)types[0], (Class<?>)types[1] };
      } else {
        // TODO: we could try to get the declaring object and infer argument... stupid Java.
        if (DebugUtils.isTraceEnabled()) {
          LlapIoImpl.LOG.info("Cannot determine map type: " + field);
        }
      }
    } else {
      // TODO: we could try to get superclass or generic interfaces.
      if (DebugUtils.isTraceEnabled()) {
        LlapIoImpl.LOG.info("Non-parametrized map type: " + field);
      }
    }
    return null;
  }

  private static Class<?> getCollectionArg(Field field) {
    // TODO: this makes many assumptions, e.g. on how generic args are done
    Type genericType = field.getGenericType();
    if (genericType instanceof ParameterizedType) {
      Type type = ((ParameterizedType)genericType).getActualTypeArguments()[0];
      if (type instanceof Class<?>) {
        return (Class<?>)type;
      } else {
        // TODO: we could try to get the declaring object and infer argument... stupid Java.
        if (DebugUtils.isTraceEnabled()) {
          LlapIoImpl.LOG.info("Cannot determine collection type: " + field);
        }
      }
    } else {
      // TODO: we could try to get superclass or generic interfaces.
      if (DebugUtils.isTraceEnabled()) {
        LlapIoImpl.LOG.info("Non-parametrized collection type: " + field);
      }
    }
    return null;
  }

  private static void addArrayEstimator(
      HashMap<Class<?>, ObjectEstimator> byType, Deque<Object> stack,
      Field field, Object fieldObj) {
    int arrayLen = 0;
    if (fieldObj != null) {
      arrayLen = Array.getLength(fieldObj);
      if (arrayLen == 0) {
        if (DebugUtils.isTraceEnabled()) {
          LlapIoImpl.LOG.info("Empty array " + field);
        }
      }
    }
    for (int i = 0; i < arrayLen; ++i) {
      Object element = Array.get(fieldObj, i);
      if (element != null) {
        addToProcessing(byType, stack, element, element.getClass());
      }
    }
    Class<?> elementClass = field.getType().getComponentType();
    addToProcessing(byType, stack, null, elementClass);
  }

  private static void addToProcessing(HashMap<Class<?>, ObjectEstimator> byType,
      Deque<Object> stack, Object element, Class<?> elementClass) {
    ObjectEstimator existing = byType.get(elementClass);
    if (existing != null && (!existing.isFromClass || (element == null))) return;
    if (elementClass.isInterface()) {
      if (element == null) return;
      elementClass = element.getClass();
    }
    byType.put(elementClass, new ObjectEstimator(element == null));
    stack.push(element == null ? elementClass : element);
  }

  private static int getPrimitiveSize(Class<?> fieldClass) {
     if (fieldClass == long.class || fieldClass == double.class) return 8;
     if (fieldClass == int.class || fieldClass == float.class) return 4;
     if (fieldClass == short.class || fieldClass == char.class) return 2;
     if (fieldClass == byte.class || fieldClass == boolean.class) return 1;
     throw new AssertionError("Unrecognized primitive " + fieldClass.getName());
  }

  private static Iterable<Field> getAllFields(Class<?> clazz, Class<?> topClass) {
    List<Field> fields = Lists.newArrayListWithCapacity(8);
    while (clazz != null) {
      fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
      clazz = clazz.getSuperclass();
      if (clazz == topClass) break;
    }

    //all together so there is only one security check
    AccessibleObject.setAccessible(fields.toArray(new AccessibleObject[fields.size()]), true);
    return fields;
  }

  private static class FieldAndType {
    public FieldAndType(Field field, FieldType type) {
      this.field = field;
      this.type = type;
    }
    public Field field;
    public FieldType type;
  }

  public static class ObjectEstimator {
    private List<FieldAndType> fields;
    protected int directSize = -1;
    private boolean isFromClass;

    public ObjectEstimator(boolean isFromClass) {
      this.isFromClass = isFromClass;
    }

    boolean isProcessed() {
      return directSize >= 0;
    }

    private void init() {
      assert directSize == -1;
      directSize = memoryModel.object();
    }

    private void addPrimitive(Class<?> clazz) {
      directSize += getPrimitiveSize(clazz);
    }

    private void addEnum() {
      directSize += memoryModel.ref();
    }

    private void addField(FieldType type, Field field) {
      if (fields == null) {
        fields = new ArrayList<>();
      }
      directSize += memoryModel.ref();
      fields.add(new FieldAndType(field, type));
    }

    public int estimate(
        Object obj, HashMap<Class<?>, ObjectEstimator> parent) {
      // TODO: track unique objects?
      // TODO: maybe use stack of est+obj pairs instead of recursion
      if (fields == null) return directSize;
      int referencedSize = 0;
      for (FieldAndType e : fields) {
        Object fieldObj;
        try {
          fieldObj = e.field.get(obj);
        } catch (IllegalAccessException ex) {
          throw new AssertionError("IAE: " + ex.getMessage());
        }
        // reference is already accounted for in the directSize.
        if (fieldObj == null) continue;
        referencedSize += memoryModel.object();
        switch (e.type) {
        case COLLECTION: {
          Collection<?> c = (Collection<?>)fieldObj;
          ObjectEstimator collEstimator = parent.get(fieldObj.getClass());
          if (collEstimator == null) {
            // We have no estimator for this type... assume low overhead and hope for the best.
            if (DebugUtils.isTraceEnabled()) {
              LlapIoImpl.LOG.info("Approximate estimation for collection "
                  + fieldObj.getClass().getName() + " from " + e.field);
            }
            referencedSize += estimateCollectionElements(parent, c, e.field);
            referencedSize += memoryModel.array() + c.size() * memoryModel.ref();
          } else if (collEstimator instanceof CollectionEstimator) {
            referencedSize += estimateCollectionElements(parent, c, e.field);
            referencedSize += collEstimator.estimate(c, parent);
          } else {
            // We decided to treat this collection as regular object.
            if (DebugUtils.isTraceEnabled()) {
              LlapIoImpl.LOG.info("Verbose estimation for collection "
                  + fieldObj.getClass().getName() + " from " + e.field);
            }
            referencedSize += collEstimator.estimate(c, parent);
          }
          break;
        }
        case MAP: {
          Map<?, ?> m = (Map<?, ?>)fieldObj;
          ObjectEstimator collEstimator = parent.get(fieldObj.getClass());
          if (collEstimator == null) {
            // We have no estimator for this type... assume low overhead and hope for the best.
            if (DebugUtils.isTraceEnabled()) {
              LlapIoImpl.LOG.info("Approximate estimation for map "
                  + fieldObj.getClass().getName() + " from " + e.field);
            }
            referencedSize += estimateMapElements(parent, m, e.field);
            referencedSize += memoryModel.array() + m.size()
                * (memoryModel.ref() * 2 + memoryModel.object());
          } else if (collEstimator instanceof CollectionEstimator) {
            referencedSize += estimateMapElements(parent, m, e.field);
            referencedSize += collEstimator.estimate(m, parent);
          } else {
            // We decided to treat this map as regular object.
            if (DebugUtils.isTraceEnabled()) {
              LlapIoImpl.LOG.info("Verbose estimation for map "
                  + fieldObj.getClass().getName() + " from " + e.field);
            }
            referencedSize += collEstimator.estimate(m, parent);
          }
          break;
        }
        case OBJECT_ARRAY: {
          int len = Array.getLength(fieldObj);
          referencedSize += JavaDataModel.alignUp(
              memoryModel.array() + len * memoryModel.ref(), memoryModel.memoryAlign());
          if (len == 0) continue;
          referencedSize += estimateArrayElements(parent, e, fieldObj, len);
          break;
        }
        case PRIMITIVE_ARRAY: {
          int arraySize = memoryModel.array();
          int len = Array.getLength(fieldObj);
          if (len != 0) {
            int elementSize = getPrimitiveSize(e.field.getType().getComponentType());
            arraySize += elementSize * len;
            arraySize = JavaDataModel.alignUp(arraySize, memoryModel.memoryAlign());
          }
          referencedSize += arraySize;
          break;
        }
        case OTHER: {
          ObjectEstimator fieldEstimator = parent.get(fieldObj.getClass());
          if (fieldEstimator == null) {
            // TODO: use reflection?
            throw new AssertionError("Don't know how to measure "
                + fieldObj.getClass().getName() + " from " + e.field);
          }
          referencedSize += fieldEstimator.estimate(fieldObj, parent);
          break;
        }
        default: throw new AssertionError("Unknown type " + e.type);
        }
      }
      return directSize + referencedSize;
    }

    private int estimateArrayElements(HashMap<Class<?>, ObjectEstimator> parent,
        FieldAndType e, Object fieldObj, int len) {
      int result = 0;
      Class<?> lastClass = e.field.getType().getComponentType();
      ObjectEstimator lastEstimator = parent.get(lastClass);
      for (int i = 0; i < len; ++i) {
        Object element = Array.get(fieldObj, i);
        if (element == null) continue;
        Class<?> elementClass = element.getClass();
        if (lastClass != elementClass) {
          lastClass = elementClass;
          lastEstimator = parent.get(lastClass);
          if (lastEstimator == null) {
            // TODO: use reflection?
            throw new AssertionError("Don't know how to measure element "
                + lastClass.getName() + " from " + e.field);
          }
        }
        result += lastEstimator.estimate(element, parent);
      }
      return result;
    }

    private int estimateCollectionElements(
        HashMap<Class<?>, ObjectEstimator> parent, Collection<?> c, Field field) {
      ObjectEstimator lastEstimator = null;
      Class<?> lastClass = null;
      int result = 0;
      for (Object element : c) {
        if (element == null) continue;
        Class<?> elementClass = element.getClass();
        if (lastClass != elementClass) {
          lastClass = elementClass;
          lastEstimator = parent.get(lastClass);
          if (lastEstimator == null) {
            // TODO: use reflection?
            throw new AssertionError("Don't know how to measure element "
                + lastClass.getName() + " from " + field);
          }
        }
        result += lastEstimator.estimate(element, parent);
      }
      return result;
    }

    private int estimateMapElements(
        HashMap<Class<?>, ObjectEstimator> parent, Map<?, ?> m, Field field) {
      ObjectEstimator keyEstimator = null, valueEstimator = null;
      Class<?> lastKeyClass = null, lastValueClass = null;
      int result = 0;
      for (Map.Entry<?, ?> element : m.entrySet()) {
        Object key = element.getKey(), value = element.getValue();
        Class<?> keyClass = key.getClass();
        if (lastKeyClass != keyClass) {
          lastKeyClass = keyClass;
          keyEstimator = parent.get(lastKeyClass);
          if (keyEstimator == null) {
            // TODO: use reflection?
            throw new AssertionError("Don't know how to measure key "
                + lastKeyClass.getName() + " from " + field);
          }
        }
        result += keyEstimator.estimate(element, parent);
        if (value == null) continue;
        Class<?> valueClass = value.getClass();
        if (lastValueClass != valueClass) {
          lastValueClass = valueClass;
          valueEstimator = parent.get(lastValueClass);
          if (valueEstimator == null) {
            // TODO: use reflection?
            throw new AssertionError("Don't know how to measure value "
                + lastValueClass.getName() + " from " + field);
          }
        }
        result += valueEstimator.estimate(element, parent);
      }
      return result;
    }
  }

  private static class CollectionEstimator extends ObjectEstimator {
    private int perEntryOverhead;

    public CollectionEstimator(int base, int perElement) {
      super(false);
      directSize = base;
      perEntryOverhead = perElement;
    }

    @Override
    public int estimate(Object obj,
        HashMap<Class<?>, ObjectEstimator> parent) {
// TODO: this is bad design, it does shallow estimate and caller checks type... we should do better
      if (obj instanceof Collection<?>) {
        return directSize + perEntryOverhead * ((Collection<?>)obj).size();
      } else if (obj instanceof Map<?, ?>) {
        return directSize + perEntryOverhead * ((Map<?, ?>)obj).size();
      } else {
        throw new AssertionError(obj);
      }
    }
  }
}
