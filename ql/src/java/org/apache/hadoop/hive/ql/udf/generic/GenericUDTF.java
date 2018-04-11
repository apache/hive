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

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.List;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * A Generic User-defined Table Generating Function (UDTF)
 *
 * Generates a variable number of output rows for a single input row. Useful for
 * explode(array)...
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class GenericUDTF {
  Collector collector = null;

  /**
   * Additionally setup GenericUDTF with MapredContext before initializing.
   * This is only called in runtime of MapRedTask.
   *
   * @param context context
   */
  public void configure(MapredContext mapredContext) {
  }

  public StructObjectInspector initialize(StructObjectInspector argOIs)
      throws UDFArgumentException {
    List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();
    ObjectInspector[] udtfInputOIs = new ObjectInspector[inputFields.size()];
    for (int i = 0; i < inputFields.size(); i++) {
      udtfInputOIs[i] = inputFields.get(i).getFieldObjectInspector();
    }
    return initialize(udtfInputOIs);
  }

  /**
   * Initialize this GenericUDTF. This will be called only once per instance.
   *
   * @param argOIs
   *          An array of ObjectInspectors for the arguments
   * @return A StructObjectInspector for output. The output struct represents a
   *         row of the table where the fields of the stuct are the columns. The
   *         field names are unimportant as they will be overridden by user
   *         supplied column aliases.
   */
  @Deprecated
  public StructObjectInspector initialize(ObjectInspector[] argOIs)
      throws UDFArgumentException {
    throw new IllegalStateException("Should not be called directly");
  }

  /**
   * Give a set of arguments for the UDTF to process.
   *
   * @param args
   *          object array of arguments
   */
  public abstract void process(Object[] args) throws HiveException;

  /**
   * Called to notify the UDTF that there are no more rows to process.
   * Clean up code or additional forward() calls can be made here.
   */
  public abstract void close() throws HiveException;

  /**
   * Associates a collector with this UDTF. Can't be specified in the
   * constructor as the UDTF may be initialized before the collector has been
   * constructed.
   *
   * @param collector
   */
  public final void setCollector(Collector collector) {
    this.collector = collector;
  }

  /**
   * Passes an output row to the collector.
   *
   * @param o
   * @throws HiveException
   */
  protected final void forward(Object o) throws HiveException {
    collector.collect(o);
  }

}
