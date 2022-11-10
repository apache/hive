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
package org.apache.hadoop.hive.ql.qoption;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.QTestUtil;

/**
 * Provides facilities to invoke {@link QTestOptionHandler}-s.
 *
 * Enables to dispatch option arguments to a specific option handler.
 * The option invocation format is '--! qt:&lt;optionName&gt;:&lt;optionArgs&gt;
 *
 * Please refer to specific implementations of {@link QTestOptionHandler} for more detailed information about them.
 */
public class QTestOptionDispatcher {

  Map<String, QTestOptionHandler> handlers = new LinkedHashMap<String, QTestOptionHandler>();

  public void register(String prefix, QTestOptionHandler datasetHandler) {
    if (handlers.containsKey(prefix)) {
      throw new RuntimeException();
    }
    handlers.put(prefix, datasetHandler);
  }

  public void process(File file) {
    synchronized (QTestUtil.class) {
      parse(file);
    }
  }

  public void parse(File file) {
    Pattern p = Pattern.compile(" *--! ?qt:([a-z]+):?(.*)");

    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        String l = line.trim();
        Matcher m = p.matcher(l);
        if (m.matches()) {
          String sub = m.group(1);
          String arguments = m.group(2);
          if (!handlers.containsKey(sub)) {
            throw new IOException("Don't know how to handle " + sub + "  line: " + l);
          }
          handlers.get(sub).processArguments(arguments);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error while processing file: " + file, e);
    }
  }

  public void beforeTest(QTestUtil qt) throws Exception {
    for (QTestOptionHandler h : handlers.values()) {
      h.beforeTest(qt);
    }
  }

  public void afterTest(QTestUtil qt) throws Exception {
    for (QTestOptionHandler h : handlers.values()) {
      h.afterTest(qt);
    }
  }

}
