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

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.thrift.TUnion;

/**
 * ObjectInspectorFactory is the primary way to create new ObjectInspector
 * instances.
 *
 * SerDe classes should call the static functions in this library to create an
 * ObjectInspector to return to the caller of SerDe2.getObjectInspector().
 *
 * The reason of having caches here is that ObjectInspector is because
 * ObjectInspectors do not have an internal state - so ObjectInspectors with the
 * same construction parameters should result in exactly the same
 * ObjectInspector.
 */
public final class ObjectInspectorFactory {

  /**
   * ObjectInspectorOptions describes what ObjectInspector to use. JAVA is to
   * use pure JAVA reflection. THRIFT is to use JAVA reflection and filter out
   * __isset fields, PROTOCOL_BUFFERS filters out has*.
   * New ObjectInspectorOptions can be added here when available.
   *
   * We choose to use a single HashMap objectInspectorCache to cache all
   * situations for efficiency and code simplicity. And we don't expect a case
   * that a user need to create 2 or more different types of ObjectInspectors
   * for the same Java type.
   */
  public enum ObjectInspectorOptions {
    JAVA, THRIFT, PROTOCOL_BUFFERS, AVRO
  };

  private static ConcurrentHashMap<Type, ObjectInspector> objectInspectorCache = new ConcurrentHashMap<Type, ObjectInspector>();

  public static ObjectInspector getReflectionObjectInspector(Type t,
      ObjectInspectorOptions options) {
    ObjectInspector oi = objectInspectorCache.get(t);
    if (oi == null) {
      oi = getReflectionObjectInspectorNoCache(t, options);
      objectInspectorCache.put(t, oi);
    }
    verifyObjectInspector(options, oi, ObjectInspectorOptions.JAVA, new Class[]{ThriftStructObjectInspector.class,
      ProtocolBuffersStructObjectInspector.class});
    verifyObjectInspector(options, oi, ObjectInspectorOptions.THRIFT, new Class[]{ReflectionStructObjectInspector.class,
        ProtocolBuffersStructObjectInspector.class});
    verifyObjectInspector(options, oi, ObjectInspectorOptions.PROTOCOL_BUFFERS, new Class[]{ThriftStructObjectInspector.class,
        ReflectionStructObjectInspector.class});

    return oi;
  }

  /**
   * Verify that we don't have an unexpected type of object inspector.
   * @param option The option to verify
   * @param oi The ObjectInspector to verify
   * @param checkOption We're only interested in this option type
   * @param classes ObjectInspector should not be of these types
   */
  private static void verifyObjectInspector(ObjectInspectorOptions option, ObjectInspector oi,
      ObjectInspectorOptions checkOption, Class[] classes) {

    if (option.equals(checkOption)) {
      for (Class checkClass : classes) {
        if (oi.getClass().equals(checkClass)) {
          throw new RuntimeException(
            "Cannot call getObjectInspectorByReflection with more then one of " +
            Arrays.toString(ObjectInspectorOptions.values()) + "!");
        }
      }
    }
  }

  private static ObjectInspector getReflectionObjectInspectorNoCache(Type t,
      ObjectInspectorOptions options) {
    if (t instanceof GenericArrayType) {
      GenericArrayType at = (GenericArrayType) t;
      return getStandardListObjectInspector(getReflectionObjectInspector(at
          .getGenericComponentType(), options));
    }

    if (t instanceof ParameterizedType) {
      ParameterizedType pt = (ParameterizedType) t;
      // List?
      if (List.class.isAssignableFrom((Class<?>) pt.getRawType()) ||
          Set.class.isAssignableFrom((Class<?>) pt.getRawType())) {
        return getStandardListObjectInspector(getReflectionObjectInspector(pt
            .getActualTypeArguments()[0], options));
      }
      // Map?
      if (Map.class.isAssignableFrom((Class<?>) pt.getRawType())) {
        return getStandardMapObjectInspector(getReflectionObjectInspector(pt
            .getActualTypeArguments()[0], options),
            getReflectionObjectInspector(pt.getActualTypeArguments()[1],
            options));
      }
      // Otherwise convert t to RawType so we will fall into the following if
      // block.
      t = pt.getRawType();
    }

    // Must be a class.
    if (!(t instanceof Class)) {
      throw new RuntimeException(ObjectInspectorFactory.class.getName()
          + " internal error:" + t);
    }
    Class<?> c = (Class<?>) t;

    // Java Primitive Type?
    if (PrimitiveObjectInspectorUtils.isPrimitiveJavaType(c)) {
      return PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(PrimitiveObjectInspectorUtils
          .getTypeEntryFromPrimitiveJavaType(c).primitiveCategory);
    }

    // Java Primitive Class?
    if (PrimitiveObjectInspectorUtils.isPrimitiveJavaClass(c)) {
      return PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(PrimitiveObjectInspectorUtils
          .getTypeEntryFromPrimitiveJavaClass(c).primitiveCategory);
    }

    // Primitive Writable class?
    if (PrimitiveObjectInspectorUtils.isPrimitiveWritableClass(c)) {
      return PrimitiveObjectInspectorFactory
          .getPrimitiveWritableObjectInspector(PrimitiveObjectInspectorUtils
          .getTypeEntryFromPrimitiveWritableClass(c).primitiveCategory);
    }

    // Enum class?
    if (Enum.class.isAssignableFrom(c)) {
      return PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
    }

    // Must be struct because List and Map need to be ParameterizedType
    assert (!List.class.isAssignableFrom(c));
    assert (!Map.class.isAssignableFrom(c));

    // Create StructObjectInspector
    ReflectionStructObjectInspector oi;
    switch (options) {
    case JAVA:
      oi = new ReflectionStructObjectInspector();
      break;
    case THRIFT:
      oi = TUnion.class.isAssignableFrom(c) ? new ThriftUnionObjectInspector() : new ThriftStructObjectInspector();
      break;
    case PROTOCOL_BUFFERS:
      oi = new ProtocolBuffersStructObjectInspector();
      break;
    default:
      throw new RuntimeException(ObjectInspectorFactory.class.getName()
          + ": internal error.");
    }

    // put it into the cache BEFORE it is initialized to make sure we can catch
    // recursive types.
    objectInspectorCache.put(t, oi);
    oi.init(c, options);
    return oi;

  }

  static ConcurrentHashMap<ObjectInspector, StandardListObjectInspector> cachedStandardListObjectInspector =
      new ConcurrentHashMap<ObjectInspector, StandardListObjectInspector>();

  public static StandardListObjectInspector getStandardListObjectInspector(
      ObjectInspector listElementObjectInspector) {
    StandardListObjectInspector result = cachedStandardListObjectInspector
        .get(listElementObjectInspector);
    if (result == null) {
      result = new StandardListObjectInspector(listElementObjectInspector);
      StandardListObjectInspector prev =
        cachedStandardListObjectInspector.putIfAbsent(listElementObjectInspector, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  public static StandardConstantListObjectInspector getStandardConstantListObjectInspector(
      ObjectInspector listElementObjectInspector, List<?> constantValue) {
    return new StandardConstantListObjectInspector(listElementObjectInspector, constantValue);
  }

  static ConcurrentHashMap<List<ObjectInspector>, StandardMapObjectInspector> cachedStandardMapObjectInspector =
      new ConcurrentHashMap<List<ObjectInspector>, StandardMapObjectInspector>();

  public static StandardMapObjectInspector getStandardMapObjectInspector(
      ObjectInspector mapKeyObjectInspector,
      ObjectInspector mapValueObjectInspector) {
    ArrayList<ObjectInspector> signature = new ArrayList<ObjectInspector>(2);
    signature.add(mapKeyObjectInspector);
    signature.add(mapValueObjectInspector);
    StandardMapObjectInspector result = cachedStandardMapObjectInspector
        .get(signature);
    if (result == null) {
      result = new StandardMapObjectInspector(mapKeyObjectInspector,
          mapValueObjectInspector);
      StandardMapObjectInspector prev =
        cachedStandardMapObjectInspector.putIfAbsent(signature, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  public static StandardConstantMapObjectInspector getStandardConstantMapObjectInspector(
      ObjectInspector mapKeyObjectInspector,
      ObjectInspector mapValueObjectInspector,
      Map<?, ?> constantValue) {
    return new StandardConstantMapObjectInspector(mapKeyObjectInspector,
          mapValueObjectInspector, constantValue);
  }

  static ConcurrentHashMap<List<ObjectInspector>, StandardUnionObjectInspector>
    cachedStandardUnionObjectInspector =
      new ConcurrentHashMap<List<ObjectInspector>, StandardUnionObjectInspector>();

  public static StandardUnionObjectInspector getStandardUnionObjectInspector(
      List<ObjectInspector> unionObjectInspectors) {
    StandardUnionObjectInspector result = cachedStandardUnionObjectInspector
        .get(unionObjectInspectors);
    if (result == null) {
      result = new StandardUnionObjectInspector(unionObjectInspectors);
      StandardUnionObjectInspector prev =
        cachedStandardUnionObjectInspector.putIfAbsent(unionObjectInspectors, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  static ConcurrentHashMap<ArrayList<List<?>>, StandardStructObjectInspector> cachedStandardStructObjectInspector =
      new ConcurrentHashMap<ArrayList<List<?>>, StandardStructObjectInspector>();

  public static StandardStructObjectInspector getStandardStructObjectInspector(
      List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors) {
    return getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors, null);
  }

  public static StandardStructObjectInspector getStandardStructObjectInspector(
      List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors,
      List<String> structComments) {
    ArrayList<List<?>> signature = new ArrayList<List<?>>(3);
    signature.add(structFieldNames);
    signature.add(structFieldObjectInspectors);
    if(structComments != null) {
      signature.add(structComments);
    }
    StandardStructObjectInspector result = cachedStandardStructObjectInspector.get(signature);
    if(result == null) {
      result = new StandardStructObjectInspector(structFieldNames, structFieldObjectInspectors, structComments);
      StandardStructObjectInspector prev =
        cachedStandardStructObjectInspector.putIfAbsent(signature, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  public static StandardConstantStructObjectInspector getStandardConstantStructObjectInspector(
    List<String> structFieldNames,
    List<ObjectInspector> structFieldObjectInspectors,  List<?> value) {
    return new StandardConstantStructObjectInspector(structFieldNames, structFieldObjectInspectors, value);
  }

  static ConcurrentHashMap<List<StructObjectInspector>, UnionStructObjectInspector> cachedUnionStructObjectInspector =
      new ConcurrentHashMap<List<StructObjectInspector>, UnionStructObjectInspector>();

  public static UnionStructObjectInspector getUnionStructObjectInspector(
      List<StructObjectInspector> structObjectInspectors) {
    UnionStructObjectInspector result = cachedUnionStructObjectInspector
        .get(structObjectInspectors);
    if (result == null) {
      result = new UnionStructObjectInspector(structObjectInspectors);
      UnionStructObjectInspector prev =
        cachedUnionStructObjectInspector.putIfAbsent(structObjectInspectors, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  static ConcurrentHashMap<ArrayList<Object>, ColumnarStructObjectInspector> cachedColumnarStructObjectInspector =
      new ConcurrentHashMap<ArrayList<Object>, ColumnarStructObjectInspector>();

  public static ColumnarStructObjectInspector getColumnarStructObjectInspector(
      List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors) {
    return getColumnarStructObjectInspector(structFieldNames, structFieldObjectInspectors, null);
  }

  public static ColumnarStructObjectInspector getColumnarStructObjectInspector(
      List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors, List<String> structFieldComments) {
    ArrayList<Object> signature = new ArrayList<Object>(3);
    signature.add(structFieldNames);
    signature.add(structFieldObjectInspectors);
    if(structFieldComments != null) {
      signature.add(structFieldComments);
    }
    ColumnarStructObjectInspector result = cachedColumnarStructObjectInspector
        .get(signature);
    if (result == null) {
      result = new ColumnarStructObjectInspector(structFieldNames,
          structFieldObjectInspectors, structFieldComments);
      ColumnarStructObjectInspector prev =
        cachedColumnarStructObjectInspector.putIfAbsent(signature, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  private ObjectInspectorFactory() {
    // prevent instantiation
  }

}
