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

package org.apache.hadoop.hive.serde2;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

/**
 * Abstract class for implementing Deserializer. The abstract class has been created, so that
 * new methods can be added in the underlying interface, Deserializer, and only implementations
 * that need those methods overwrite it.
 */
public abstract class AbstractDeserializer implements Deserializer {

  /**
   * Initialize the HiveDeserializer.
   *
   * @param conf
   *          System properties
   * @param tbl
   *          table properties
   * @throws SerDeException
   */
  public abstract void initialize(Configuration conf, Properties tbl) throws SerDeException;

  /**
   * Deserialize an object out of a Writable blob. In most cases, the return
   * value of this function will be constant since the function will reuse the
   * returned object. If the client wants to keep a copy of the object, the
   * client needs to clone the returned value by calling
   * ObjectInspectorUtils.getStandardObject().
   *
   * @param blob
   *          The Writable object containing a serialized object
   * @return A Java object representing the contents in the blob.
   */
  public abstract Object deserialize(Writable blob) throws SerDeException;

  /**
   * Get the object inspector that can be used to navigate through the internal
   * structure of the Object returned from deserialize(...).
   */
  public abstract ObjectInspector getObjectInspector() throws SerDeException;

  /**
   * Returns statistics collected when serializing
   */
  public abstract SerDeStats getSerDeStats();
}
