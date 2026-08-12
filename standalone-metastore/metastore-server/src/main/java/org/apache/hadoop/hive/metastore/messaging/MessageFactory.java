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

package org.apache.hadoop.hive.metastore.messaging;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageEncoder;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract Factory for the construction of HCatalog message instances.
 */
public abstract class MessageFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MessageFactory.class.getName());

  protected static final Configuration conf = MetastoreConf.newMetastoreConf();

  private static final Map<String, Method> registry = new HashMap<>();

  public static void register(String messageFormat, Class clazz) {
    Method method = requiredMethod(clazz);
    registry.put(messageFormat, method);
  }

  static {
    register(GzipJSONMessageEncoder.FORMAT, GzipJSONMessageEncoder.class);
    register(JSONMessageEncoder.FORMAT, JSONMessageEncoder.class);
    register(supportedCompressionFormats.GZIP.toString().toLowerCase(), GzipJSONMessageEncoder.class);
  }

  private static Method requiredMethod(Class clazz) {
    if (MessageEncoder.class.isAssignableFrom(clazz)) {
      try {
        Method methodInstance = clazz.getMethod("getInstance");
        if (MessageEncoder.class.isAssignableFrom(methodInstance.getReturnType())) {
          int modifiers = methodInstance.getModifiers();
          if (Modifier.isStatic(modifiers) && Modifier.isPublic(modifiers)) {
            return methodInstance;
          }
          throw new NoSuchMethodException(
              "modifier for getInstance() method is not 'public static' in " + clazz
                  .getCanonicalName());
        }
        throw new NoSuchMethodException(
            "return type is not assignable to " + MessageEncoder.class.getCanonicalName());
      } catch (NoSuchMethodException e) {
        String message = clazz.getCanonicalName()
            + " does not implement the required 'public static MessageEncoder getInstance()' method ";
        LOG.error(message, e);
        throw new IllegalArgumentException(message, e);
      }
    }
    String message = clazz.getCanonicalName() + " is not assignable to " + MessageEncoder.class
        .getCanonicalName();
    LOG.error(message);
    throw new IllegalArgumentException(message);
  }

  public static MessageEncoder getInstance(String compressionFormat)
      throws InvocationTargetException, IllegalAccessException {
    Method methodInstance = registry.get(compressionFormat.toLowerCase());
    if (methodInstance == null) {
      LOG.error("received incorrect CompressionFormat " + compressionFormat);
      throw new RuntimeException("compressionFormat: " + compressionFormat + " is not supported.");
    }
    return (MessageEncoder) methodInstance.invoke(null);
  }

  public static MessageEncoder getDefaultInstanceForReplMetrics(Configuration conf) {
    return getInstance(conf, MetastoreConf.ConfVars.REPL_MESSAGE_FACTORY.getVarname());
  }

  public static MessageEncoder getDefaultInstance(Configuration conf) {
    return getInstance(conf, MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getVarname());
  }

  public static MessageEncoder getInstance(Configuration conf, String config) {
    String clazz =
            MetastoreConf.get(conf, config);
    try {
      Class<?> clazzObject = MessageFactory.class.getClassLoader().loadClass(clazz);
      return (MessageEncoder) requiredMethod(clazzObject).invoke(null);
    } catch (Exception e) {
      String message = "could not load the configured class " + clazz;
      LOG.error(message, e);
      throw new IllegalStateException(message, e);
    }
  }

  public enum supportedCompressionFormats {
    /**
     * Currently supported compressionFormats for encoding and decoding the messages in backend RDBMS.
     */
    GZIP
  }
}
