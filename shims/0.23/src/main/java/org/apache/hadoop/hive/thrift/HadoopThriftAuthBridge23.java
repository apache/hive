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
package org.apache.hadoop.hive.thrift;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SaslRpcServer;

/**
 * Functions that bridge Thrift's SASL transports to Hadoop's SASL callback
 * handlers and authentication classes.
 *
 * This is a 0.23/2.x specific implementation
 */
public class HadoopThriftAuthBridge23 extends HadoopThriftAuthBridge {

  private static Field SASL_PROPS_FIELD;
  private static Class<?> SASL_PROPERTIES_RESOLVER_CLASS;
  private static Method RES_GET_INSTANCE_METHOD;
  private static Method GET_DEFAULT_PROP_METHOD;
  static {
    SASL_PROPERTIES_RESOLVER_CLASS = null;
    SASL_PROPS_FIELD = null;
    final String SASL_PROP_RES_CLASSNAME = "org.apache.hadoop.security.SaslPropertiesResolver";
    try {
      SASL_PROPERTIES_RESOLVER_CLASS = Class.forName(SASL_PROP_RES_CLASSNAME);

    } catch (ClassNotFoundException e) {
    }

    if (SASL_PROPERTIES_RESOLVER_CLASS != null) {
      // found the class, so this would be hadoop version 2.4 or newer (See
      // HADOOP-10221, HADOOP-10451)
      try {
        RES_GET_INSTANCE_METHOD = SASL_PROPERTIES_RESOLVER_CLASS.getMethod("getInstance",
            Configuration.class);
        GET_DEFAULT_PROP_METHOD = SASL_PROPERTIES_RESOLVER_CLASS.getMethod("getDefaultProperties");
      } catch (Exception e) {
        // this must be hadoop 2.4 , where getDefaultProperties was protected
      }
    }

    if (SASL_PROPERTIES_RESOLVER_CLASS == null || GET_DEFAULT_PROP_METHOD == null) {
      // this must be a hadoop 2.4 version or earlier.
      // Resorting to the earlier method of getting the properties, which uses SASL_PROPS field
      try {
        SASL_PROPS_FIELD = SaslRpcServer.class.getField("SASL_PROPS");
      } catch (NoSuchFieldException e) {
        // Older version of hadoop should have had this field
        throw new IllegalStateException("Error finding hadoop SASL_PROPS field in "
            + SaslRpcServer.class.getSimpleName(), e);
      }
    }
  }

  /**
   * Read and return Hadoop SASL configuration which can be configured using
   * "hadoop.rpc.protection"
   *
   * @param conf
   * @return Hadoop SASL configuration
   */
  @SuppressWarnings("unchecked")
  @Override
  public Map<String, String> getHadoopSaslProperties(Configuration conf) {
    if (SASL_PROPS_FIELD != null) {
      // hadoop 2.4 and earlier way of finding the sasl property settings
      // Initialize the SaslRpcServer to ensure QOP parameters are read from
      // conf
      SaslRpcServer.init(conf);
      try {
        return (Map<String, String>) SASL_PROPS_FIELD.get(null);
      } catch (Exception e) {
        throw new IllegalStateException("Error finding hadoop SASL properties", e);
      }
    }
    // 2.5 and later way of finding sasl property
    try {
      Configurable saslPropertiesResolver = (Configurable) RES_GET_INSTANCE_METHOD.invoke(null,
          conf);
      saslPropertiesResolver.setConf(conf);
      return (Map<String, String>) GET_DEFAULT_PROP_METHOD.invoke(saslPropertiesResolver);
    } catch (Exception e) {
      throw new IllegalStateException("Error finding hadoop SASL properties", e);
    }
  }

}
