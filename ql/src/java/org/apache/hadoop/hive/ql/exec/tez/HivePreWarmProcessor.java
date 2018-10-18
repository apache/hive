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

package org.apache.hadoop.hive.ql.exec.tez;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;

import java.net.URL;
import java.net.JarURLConnection;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;
import java.util.jar.JarEntry;

import javax.crypto.Mac;

/**
 * A simple sleep processor implementation that sleeps for the configured
 * time in milliseconds.
 *
 * @see Config for configuring the HivePreWarmProcessor
 */
public class HivePreWarmProcessor extends AbstractLogicalIOProcessor {

  private static boolean prewarmed = false;

  private static final Logger LOG = LoggerFactory.getLogger(HivePreWarmProcessor.class);

  private Configuration conf;

  public HivePreWarmProcessor(ProcessorContext context) {
    super(context);
  }

  @Override
  public void initialize() throws Exception {
    UserPayload userPayload = getContext().getUserPayload();
    this.conf = TezUtils.createConfFromUserPayload(userPayload);
  }

  @Override
  public void run(Map<String, LogicalInput> inputs,
                  Map<String, LogicalOutput> outputs) throws Exception {
    if(prewarmed) {
      /* container reuse */
      return;
    }
    for (LogicalInput input : inputs.values()) {
      input.start();
    }
    for (LogicalOutput output : outputs.values()) {
      output.start();
    }
    /* these are things that goes through singleton initialization on most queries */
    FileSystem fs = FileSystem.get(conf);
    Mac mac = Mac.getInstance("HmacSHA1");
    ReadaheadPool rpool = ReadaheadPool.getInstance();
    ShimLoader.getHadoopShims();

    URL hiveurl = new URL("jar:" + DagUtils.getInstance().getExecJarPathLocal(conf) + "!/");
    JarURLConnection hiveconn = (JarURLConnection)hiveurl.openConnection();
    JarFile hivejar = hiveconn.getJarFile();
    try {
      Enumeration<JarEntry> classes = hivejar.entries();
      while(classes.hasMoreElements()) {
        JarEntry je = classes.nextElement();
        if (je.getName().endsWith(".class")) {
          String klass = je.getName().replace(".class","").replaceAll("/","\\.");
          if(klass.indexOf("ql.exec") != -1 || klass.indexOf("ql.io") != -1) {
            /* several hive classes depend on the metastore APIs, which is not included
             * in hive-exec.jar. These are the relatively safe ones - operators & io classes.
             */
            if(klass.indexOf("vector") != -1 || klass.indexOf("Operator") != -1) {
              JavaUtils.loadClass(klass);
            }
          }
        }
      }
    } finally {
      hivejar.close();
    }
    prewarmed = true;
  }

  @Override
  public void handleEvents(List<Event> processorEvents) {
    // Nothing to do
  }

  @Override
  public void close() throws Exception {
    // Nothing to cleanup
  }
}
