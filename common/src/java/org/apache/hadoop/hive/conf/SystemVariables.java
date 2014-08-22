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
package org.apache.hadoop.hive.conf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class SystemVariables {

  private static final Log l4j = LogFactory.getLog(SystemVariables.class);
  protected static Pattern varPat = Pattern.compile("\\$\\{[^\\}\\$\u0020]+\\}");

  public static final String ENV_PREFIX = "env:";
  public static final String SYSTEM_PREFIX = "system:";
  public static final String HIVECONF_PREFIX = "hiveconf:";
  public static final String HIVEVAR_PREFIX = "hivevar:";
  public static final String METACONF_PREFIX = "metaconf:";
  public static final String SET_COLUMN_NAME = "set";

  protected String getSubstitute(Configuration conf, String var) {
    String val = null;
    try {
      if (var.startsWith(SYSTEM_PREFIX)) {
        val = System.getProperty(var.substring(SYSTEM_PREFIX.length()));
      }
    } catch(SecurityException se) {
      l4j.warn("Unexpected SecurityException in Configuration", se);
    }
    if (val == null && var.startsWith(ENV_PREFIX)) {
      val = System.getenv(var.substring(ENV_PREFIX.length()));
    }
    if (val == null && conf != null && var.startsWith(HIVECONF_PREFIX)) {
      val = conf.get(var.substring(HIVECONF_PREFIX.length()));
    }
    return val;
  }

  public static boolean containsVar(String expr) {
    return expr != null && varPat.matcher(expr).find();
  }

  static String substitute(String expr) {
    return expr == null ? null : new SystemVariables().substitute(null, expr, 1);
  }

  static String substitute(Configuration conf, String expr) {
    return expr == null ? null : new SystemVariables().substitute(conf, expr, 1);
  }

  protected final String substitute(Configuration conf, String expr, int depth) {
    Matcher match = varPat.matcher("");
    String eval = expr;
    StringBuilder builder = new StringBuilder();
    int s = 0;
    for (; s <= depth; s++) {
      match.reset(eval);
      builder.setLength(0);
      int prev = 0;
      boolean found = false;
      while (match.find(prev)) {
        String group = match.group();
        String var = group.substring(2, group.length() - 1); // remove ${ .. }
        String substitute = getSubstitute(conf, var);
        if (substitute == null) {
          substitute = group;   // append as-is
        } else {
          found = true;
        }
        builder.append(eval.substring(prev, match.start())).append(substitute);
        prev = match.end();
      }
      if (!found) {
        return eval;
      }
      builder.append(eval.substring(prev));
      eval = builder.toString();
    }
    if (s > depth) {
      throw new IllegalStateException(
          "Variable substitution depth is deeper than " + depth + " for expression " + expr);
    }
    return eval;
  }
}
