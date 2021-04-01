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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.QTestUtil;

public class QTestConfHandler implements QTestOptionHandler {

  Map<String, String> confVals = new HashMap<>();

  @Override
  public void processArguments(String arguments) {
    Pattern p = Pattern.compile("([a-z0-9.]+)=?(.*)");
    Matcher m = p.matcher(arguments);
    if (!m.matches()) {
      throw new RuntimeException("invalid arguments!");
    }
    confVals.put(m.group(1), m.group(2));
  }

  @Override
  public void beforeTest(QTestUtil qt) throws Exception {
    for (Entry<String, String> e : confVals.entrySet()) {
      qt.getConf().set(e.getKey(), e.getValue());
    }
  }

  @Override
  public void afterTest(QTestUtil qt) throws Exception {
    confVals.clear();

  }
}

