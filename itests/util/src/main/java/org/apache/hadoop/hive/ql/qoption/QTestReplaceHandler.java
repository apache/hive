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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * QTest replacement directive handler
 *
 * Examples:
 *
 * --! qt:replace:/there/joe/
 * select 'hello there!
 * ===q.out
 * hello joe!
 *
 * standard java regex; placeholders also work:
 * --! qt:replace:/Hello (.*)!/$1 was here!/
 *
 * first char of regex pattern is used as separator; you may choose anything else than '/'
 * --! qt:replace:#this#that#
 */
public class QTestReplaceHandler implements QTestOptionHandler {
  private static final Logger LOG = LoggerFactory.getLogger(QTestReplaceHandler.class.getName());

  Map<Pattern, String> replacements = new HashMap<Pattern, String>();

  @Override
  public void processArguments(String arguments) {
    arguments=arguments.trim();
    if (arguments.length() < 2) {
      throw new RuntimeException("illegal replacement expr: " + arguments + " ; expected something like /this/that/");
    }
    String sep = arguments.substring(0, 1);
    String[] parts = arguments.split(Pattern.quote(sep));
    if (parts.length != 3) {
      throw new RuntimeException(
          "unexpected replacement expr: " + arguments + " ; expected something like /this/that/");
    }
    LOG.info("Enabling replacement of: {} => {}", parts[1], parts[2]);
    replacements.put(Pattern.compile(parts[1]), parts[2]);
  }

  @Override
  public void beforeTest(QTestUtil qt) throws Exception {
  }

  @Override
  public void afterTest(QTestUtil qt) throws Exception {
    replacements.clear();
  }

  public String processLine(String line) {
    for (Entry<Pattern, String> r : replacements.entrySet()) {
      Matcher m = r.getKey().matcher(line);
      if(m.find()) {
        line = m.replaceAll(r.getValue());
      }
    }
    return line;
  }

}
