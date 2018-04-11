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

package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.DynamicValueRegistry;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.sarg.LiteralDelegate;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.io.Serializable;


public class DynamicValue implements LiteralDelegate, Serializable {

  private static final long serialVersionUID = 1L;

  public static final String DYNAMIC_VALUE_REGISTRY_CACHE_KEY = "DynamicValueRegistry";

  protected transient Configuration conf;

  protected String id;
  TypeInfo typeInfo;
  PrimitiveObjectInspector objectInspector;

  transient protected Object val;
  transient boolean initialized = false;

  public DynamicValue(String id, TypeInfo typeInfo) {
    this.id = id;
    this.typeInfo = typeInfo;
    this.objectInspector = (PrimitiveObjectInspector) TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeInfo);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  public TypeInfo getTypeInfo() {
    return typeInfo;
  }

  public void setTypeInfo(TypeInfo typeInfo) {
    this.typeInfo = typeInfo;
  }

  public PrimitiveObjectInspector getObjectInspector() {
    return objectInspector;
  }

  public void setObjectInspector(PrimitiveObjectInspector objectInspector) {
    this.objectInspector = objectInspector;
  }

  @Override
  public String getId() { return id;}

  public void setId(String id) {
    this.id = id;
  }

  @Override
  public Object getLiteral() {
    return getJavaValue();
  }

  public Object getJavaValue() {
    return objectInspector.getPrimitiveJavaObject(getValue());
  }

  public Object getWritableValue() {
    return objectInspector.getPrimitiveWritableObject(getValue());
  }

  /**
   * An exception that indicates that the dynamic values are (intentionally)
   * not available in this context.
   */
  public static class NoDynamicValuesException extends RuntimeException {
    public NoDynamicValuesException(String message) {
      super(message);
    }
  }

  public Object getValue() {
    if (initialized) {
      return val;
    }

    if (conf == null) {
      throw new NoDynamicValuesException("Cannot retrieve dynamic value " + id + " - no conf set");
    }

    try {
      // Get object cache
      String queryId = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYID);
      ObjectCache cache = ObjectCacheFactory.getCache(conf, queryId, false, true);

      if (cache == null) {
        return null;
      }

      // Get the registry
      DynamicValueRegistry valueRegistry = cache.retrieve(DYNAMIC_VALUE_REGISTRY_CACHE_KEY);
      if (valueRegistry == null) {
        throw new NoDynamicValuesException("DynamicValueRegistry not available");
      }
      val = valueRegistry.getValue(id);
      initialized = true;
    } catch (NoDynamicValuesException err) {
      throw err;
    } catch (Exception err) {
      throw new IllegalStateException("Failed to retrieve dynamic value for " + id, err);
    }

    return val;
  }

  @Override
  public String toString() {
    // If the id is a generated unique ID then this could affect .q file golden files for tests that run EXPLAIN queries.
    return "DynamicValue(" + id + ")";
  }
}
