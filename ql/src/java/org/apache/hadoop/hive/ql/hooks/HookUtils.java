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

package org.apache.hadoop.hive.ql.hooks;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.logging.log4j.util.Strings;

public class HookUtils {

  public static String redactLogString(HiveConf conf, String logString)
          throws InstantiationException, IllegalAccessException, ClassNotFoundException {

    String redactedString = logString;

    if (conf != null && logString != null) {
      List<Redactor> queryRedactors = readHooksFromConf(conf, ConfVars.QUERYREDACTORHOOKS);
      for (Redactor redactor : queryRedactors) {
        redactor.setConf(conf);
        redactedString = redactor.redactQuery(redactedString);
      }
    }
    return redactedString;
  }

  public static <T extends Hook> List<T> readHooksFromConf(HiveConf conf, HiveConf.ConfVars hookConfVar)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    String csHooks = conf.getVar(hookConfVar);
    List<T> hooks = new ArrayList<>();
    if (Strings.isBlank(csHooks)) {
      return hooks;
    }
    String[] hookClasses = csHooks.split(",");
    for (String hookClass : hookClasses) {
      T hook = (T) Class.forName(hookClass.trim(), true, Utilities.getSessionSpecifiedClassLoader()).newInstance();
      hooks.add(hook);
    }
    return hooks;
  }
}
