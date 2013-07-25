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

package org.apache.hadoop.hive.ql.exec;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

/**
 * Runtime context of MapredTask providing additional information to GenericUDF
 */
public class MapredContext {

  private static final Log logger = LogFactory.getLog("MapredContext");
  private static final ThreadLocal<MapredContext> contexts = new ThreadLocal<MapredContext>();

  public static MapredContext get() {
    return contexts.get();
  }

  public static MapredContext init(boolean isMap, JobConf jobConf) {
    MapredContext context = new MapredContext(isMap, jobConf);
    contexts.set(context);
    return context;
  }

  public static void close() {
    MapredContext context = contexts.get();
    if (context != null) {
      context.closeAll();
    }
    contexts.remove();
  }

  private final boolean isMap;
  private final JobConf jobConf;
  private final List<Closeable> udfs;

  private Reporter reporter;

  private MapredContext(boolean isMap, JobConf jobConf) {
    this.isMap = isMap;
    this.jobConf = jobConf;
    this.udfs = new ArrayList<Closeable>();
  }

  /**
   * Returns whether the UDF is called from Map or Reduce task.
   */
  public boolean isMap() {
    return isMap;
  }

  /**
   * Returns Reporter, which is set right before reading the first row.
   */
  public Reporter getReporter() {
    return reporter;
  }

  /**
   * Returns JobConf.
   */
  public JobConf getJobConf() {
    return jobConf;
  }

  public void setReporter(Reporter reporter) {
    this.reporter = reporter;
  }

  private void registerCloseable(Closeable closeable) {
    udfs.add(closeable);
  }

  private void closeAll() {
    for (Closeable eval : udfs) {
      try {
        eval.close();
      } catch (IOException e) {
        logger.info("Hit error while closing udf " + eval);
      }
    }
    udfs.clear();
  }

  void setup(GenericUDF genericUDF) {
    if (needConfigure(genericUDF)) {
      genericUDF.configure(this);
    }
    if (needClose(genericUDF)) {
      registerCloseable(genericUDF);
    }
  }

  void setup(GenericUDAFEvaluator genericUDAF) {
    if (needConfigure(genericUDAF)) {
      genericUDAF.configure(this);
    }
    if (needClose(genericUDAF)) {
      registerCloseable(genericUDAF);
    }
  }

  void setup(GenericUDTF genericUDTF) {
    if (needConfigure(genericUDTF)) {
      genericUDTF.configure(this);
    }
    // close is called by UDTFOperator
  }

  private boolean needConfigure(Object func) {
    try {
      Method initMethod = func.getClass().getMethod("configure", MapredContext.class);
      return initMethod.getDeclaringClass() != GenericUDF.class &&
        initMethod.getDeclaringClass() != GenericUDAFEvaluator.class &&
        initMethod.getDeclaringClass() != GenericUDTF.class;
    } catch (Exception e) {
      return false;
    }
  }

  private boolean needClose(Closeable func) {
    try {
      Method closeMethod = func.getClass().getMethod("close");
      return closeMethod.getDeclaringClass() != GenericUDF.class &&
        closeMethod.getDeclaringClass() != GenericUDAFEvaluator.class;
    } catch (Exception e) {
      return false;
    }
  }
}
