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

package org.apache.hadoop.hive.accumulo.serde;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.Utils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.log4j.Logger;

/**
 * {@link AccumuloRowIdFactory} designed for injection of the {@link AccumuloCompositeRowId} to be
 * used to generate the Accumulo rowId. Allows for custom {@link AccumuloCompositeRowId}s to be
 * specified without overriding the entire ObjectInspector for the Hive row.
 *
 * @param <T>
 */
public class CompositeAccumuloRowIdFactory<T extends AccumuloCompositeRowId> extends
    DefaultAccumuloRowIdFactory {

  public static final Logger log = Logger.getLogger(CompositeAccumuloRowIdFactory.class);

  private final Class<T> keyClass;
  private final Constructor<T> constructor;

  public CompositeAccumuloRowIdFactory(Class<T> keyClass) throws SecurityException,
      NoSuchMethodException {
    // see javadoc of AccumuloCompositeRowId
    this.keyClass = keyClass;
    this.constructor = keyClass.getDeclaredConstructor(LazySimpleStructObjectInspector.class,
        Properties.class, Configuration.class);
  }

  @Override
  public void addDependencyJars(Configuration jobConf) throws IOException {
    // Make sure the jar containing the custom CompositeRowId is included
    // in the mapreduce job's classpath (libjars)
    Utils.addDependencyJars(jobConf, keyClass);
  }

  @Override
  public T createRowId(ObjectInspector inspector) throws SerDeException {
    try {
      return (T) constructor.newInstance(inspector, this.properties,
          this.accumuloSerDeParams.getConf());
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }
}
