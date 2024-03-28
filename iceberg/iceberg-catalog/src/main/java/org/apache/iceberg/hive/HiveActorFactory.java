/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Factory to create HiveActor instance.
 */
public class HiveActorFactory {
  private HiveActorFactory() {
    // non-instantiable
  }

  /**
   * Creates an actor using the {@see HiveConf.ConfVars.HIVE_ICEBERG_CATALOG_ACTOR_CLASS} class-name property.
   * @param name the actor name
   * @param conf the actor configuration
   * @return an actor instance
   * @throws RuntimeException if instantiation fails
   */
  public static HiveActor createActor(final String name, final Configuration conf) {
    return createActor(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_ICEBERG_CATALOG_ACTOR_CLASS), name, conf);

  }

  /**
   * The method to create an actor.
   * @param clazzName the actor fully qualified class name
   * @param name the actor name
   * @param conf the actor configuration
   * @return an actor instance
   * @throws RuntimeException if instantiation fails
   */
  public static HiveActor createActor(final String clazzName, final String name, final Configuration conf) {
    if (clazzName == null || HiveCatalogActor.class.getName().equals(clazzName)) {
      return new HiveCatalogActor(name, conf);
    }
    try {
      @SuppressWarnings("unchecked")
      Class<? extends HiveActor> clazz = (Class<? extends HiveActor>) Class.forName(clazzName);
      Constructor<? extends HiveActor> ctor = clazz.getConstructor(String.class, Configuration.class);
      return ctor.newInstance(name, conf);
    } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException |
             InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
