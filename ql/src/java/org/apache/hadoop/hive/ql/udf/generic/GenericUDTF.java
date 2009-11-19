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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * A Generic User-defined Table Generating Function (UDTF)
 * 
 * Generates a variable number of output rows for a variable number of input
 * rows. Useful for explode(array()), histograms, etc
 */

public abstract class GenericUDTF {
  Collector collector = null;
  
  /**
   * Initialize this GenericUDTF. This will be called only once per
   * instance.
   * 
   * @param args    An array of ObjectInspectors for the arguments
   * @return        ObjectInspector for the output
   */
  public abstract ObjectInspector initialize(ObjectInspector [] argOIs) 
  throws UDFArgumentException;
  
  /**
   * Give a a set of arguments for the UDTF to process.
   * 
   * @param o       object array of arguments
   */
  public abstract void process(Object [] args) throws HiveException;
  
  /**
   * Notify the UDTF that there are no more rows to process.
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
   * Passes output data to collector
   * 
   * @param o
   * @throws HiveException
   */
  final void forward(Object o) throws HiveException {
    collector.collect(o);
  }

}
