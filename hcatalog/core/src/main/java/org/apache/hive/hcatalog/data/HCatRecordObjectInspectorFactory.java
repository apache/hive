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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.data;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ObjectInspectorFactory for HCatRecordObjectInspectors (and associated helper inspectors)
 */
public class HCatRecordObjectInspectorFactory {

  private final static Logger LOG = LoggerFactory.getLogger(HCatRecordObjectInspectorFactory.class);
  private static final int INITIAL_CACHE_CAPACITY = 1024;
  private static final int MAX_CACHE_CAPACITY = 10 * INITIAL_CACHE_CAPACITY;

  private static final CacheBuilder<Object, Object> boundedCache =
      CacheBuilder.newBuilder()
          .initialCapacity(INITIAL_CACHE_CAPACITY)
          .maximumSize(MAX_CACHE_CAPACITY)
          .concurrencyLevel(Runtime.getRuntime().availableProcessors())
          .expireAfterAccess(5, TimeUnit.MINUTES);

  private static final Cache<TypeInfo, HCatRecordObjectInspector> cachedHCatRecordObjectInspectors
      = boundedCache.build();
  private static final Cache<TypeInfo, ObjectInspector> cachedObjectInspectors
      = boundedCache.build();

  /**
   * Returns HCatRecordObjectInspector given a StructTypeInfo type definition for the record to look into
   * @param typeInfo Type definition for the record to look into
   * @return appropriate HCatRecordObjectInspector
   * @throws SerDeException
   */
  public static HCatRecordObjectInspector getHCatRecordObjectInspector(
    StructTypeInfo typeInfo) throws SerDeException {
    HCatRecordObjectInspector oi = cachedHCatRecordObjectInspectors.getIfPresent(typeInfo);
    if (oi == null) {

      LOG.debug("Got asked for OI for {} [{} ]", typeInfo.getCategory(), typeInfo.getTypeName());
      switch (typeInfo.getCategory()) {
      case STRUCT:
        StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        List<String> fieldNames = structTypeInfo.getAllStructFieldNames();
        List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
        List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(fieldTypeInfos.size());
        for (int i = 0; i < fieldTypeInfos.size(); i++) {
          fieldObjectInspectors.add(getStandardObjectInspectorFromTypeInfo(fieldTypeInfos.get(i)));
        }
        oi = new HCatRecordObjectInspector(fieldNames, fieldObjectInspectors);

        break;
      default:
        // Hmm.. not good,
        // the only type expected here is STRUCT, which maps to HCatRecord
        // - anything else is an error. Return null as the inspector.
        throw new SerDeException("TypeInfo [" + typeInfo.getTypeName()
          + "] was not of struct type - HCatRecord expected struct type, got ["
          + typeInfo.getCategory().toString() + "]");
      }
      cachedHCatRecordObjectInspectors.put(typeInfo, oi);
    }
    return oi;
  }

  public static ObjectInspector getStandardObjectInspectorFromTypeInfo(TypeInfo typeInfo) {


    ObjectInspector oi = cachedObjectInspectors.getIfPresent(typeInfo);
    if (oi == null) {

      LOG.debug("Got asked for OI for {}, [{}]", typeInfo.getCategory(), typeInfo.getTypeName());
      switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        oi = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
            (PrimitiveTypeInfo) typeInfo);
        break;
      case STRUCT:
        StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        List<String> fieldNames = structTypeInfo.getAllStructFieldNames();
        List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
        List<ObjectInspector> fieldObjectInspectors =
          new ArrayList<ObjectInspector>(fieldTypeInfos.size());
        for (int i = 0; i < fieldTypeInfos.size(); i++) {
          fieldObjectInspectors.add(getStandardObjectInspectorFromTypeInfo(fieldTypeInfos.get(i)));
        }
        oi = ObjectInspectorFactory.getStandardStructObjectInspector(
          fieldNames, fieldObjectInspectors
        );
        break;
      case LIST:
        ObjectInspector elementObjectInspector = getStandardObjectInspectorFromTypeInfo(
          ((ListTypeInfo) typeInfo).getListElementTypeInfo());
        oi = ObjectInspectorFactory.getStandardListObjectInspector(elementObjectInspector);
        break;
      case MAP:
        ObjectInspector keyObjectInspector = getStandardObjectInspectorFromTypeInfo(
          ((MapTypeInfo) typeInfo).getMapKeyTypeInfo());
        ObjectInspector valueObjectInspector = getStandardObjectInspectorFromTypeInfo(
          ((MapTypeInfo) typeInfo).getMapValueTypeInfo());
        oi = ObjectInspectorFactory.getStandardMapObjectInspector(keyObjectInspector, valueObjectInspector);
        break;
      default:
        oi = null;
      }
      cachedObjectInspectors.asMap().put(typeInfo, oi);
    }
    return oi;
  }


}
