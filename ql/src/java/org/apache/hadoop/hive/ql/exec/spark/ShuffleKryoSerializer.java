/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.spark.client.SparkClientUtilities;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;


final class ShuffleKryoSerializer {

  private static final String HIVE_SHUFFLE_KRYO_SERIALIZER = "org.apache.hive.spark.NoHashCodeKryoSerializer";

  private static org.apache.spark.serializer.KryoSerializer INSTANCE;

  private ShuffleKryoSerializer() {
    // Don't create me
  }

  static org.apache.spark.serializer.KryoSerializer getInstance(JavaSparkContext sc,
                                                                Configuration conf) {
    if (INSTANCE == null) {
      synchronized (ShuffleKryoSerializer.class) {
        if (INSTANCE == null) {
          try {
            INSTANCE = (org.apache.spark.serializer.KryoSerializer) Thread.currentThread().getContextClassLoader().loadClass(
                    HIVE_SHUFFLE_KRYO_SERIALIZER).getConstructor(SparkConf.class).newInstance(
                    sc.getConf());
            return INSTANCE;
          } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException e) {
            throw new IllegalStateException(
                    "Unable to create kryo serializer for shuffle RDDs using " +
                            "class " + HIVE_SHUFFLE_KRYO_SERIALIZER, e);
          }
        } else {
          return INSTANCE;
        }
      }
    }
    return INSTANCE;
  }
}
