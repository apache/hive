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
package org.apache.hadoop.hive.metastore;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class implementation to create instances of IMetaStoreSchemaInfo
 * based on the provided configuration
 */
public class MetaStoreSchemaInfoFactory {
  public static final Logger LOG = LoggerFactory.getLogger(MetaStoreSchemaInfoFactory.class);

  public static IMetaStoreSchemaInfo get(Configuration conf) {
    String hiveHome = System.getenv("HIVE_HOME");
    if (hiveHome == null) {
      LOG.debug("HIVE_HOME is not set. Using current directory instead");
      hiveHome = ".";
    }
    return get(conf, hiveHome, null);
  }

  public static IMetaStoreSchemaInfo get(Configuration conf, String hiveHome, String dbType) {
    String className = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.SCHEMA_INFO_CLASS);
    Class<?> clasz;
    try {
      clasz = conf.getClassByName(className);
    } catch (ClassNotFoundException e) {
      LOG.error("Unable to load class " + className, e);
      throw new IllegalArgumentException(e);
    }
    Constructor<?> constructor;
    try {
      constructor = clasz.getConstructor(String.class, String.class);
      constructor.setAccessible(true);
      return (IMetaStoreSchemaInfo) constructor.newInstance(hiveHome, dbType);
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException
        | IllegalArgumentException | InvocationTargetException e) {
      LOG.error("Unable to create instance of class " + className, e);
      throw new IllegalArgumentException(e);
    }
  }
}
