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
package org.apache.hadoop.hive.conf;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.valcoersion.JavaIOTmpdirVariableCoercion;
import org.apache.hadoop.hive.conf.valcoersion.VariableCoercion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemVariables {

  private static final Logger l4j = LoggerFactory.getLogger(SystemVariables.class);
  protected static final Pattern varPat = Pattern.compile("\\$\\{[^\\}\\$\u0020]+\\}");
  private static final SystemVariables INSTANCE = new SystemVariables();
  private static final Map<String, VariableCoercion> COERCIONS =
      ImmutableMap.<String, VariableCoercion>builder()
          .put(JavaIOTmpdirVariableCoercion.INSTANCE.getName(), JavaIOTmpdirVariableCoercion.INSTANCE)
          .build();

  public static final String ENV_PREFIX = "env:";
  public static final String SYSTEM_PREFIX = "system:";
  public static final String HIVECONF_PREFIX = "hiveconf:";
  public static final String HIVEVAR_PREFIX = "hivevar:";
  public static final String METACONF_PREFIX = "metaconf:";
  public static final String SET_COLUMN_NAME = "set";

  protected String getSubstitute(Configuration conf, String variableName) {
    try {
      if (variableName.startsWith(SYSTEM_PREFIX)) {
        String propertyName = variableName.substring(SYSTEM_PREFIX.length());
        String originalValue = System.getProperty(propertyName);
        return applyCoercion(variableName, originalValue);
      }
    } catch(SecurityException se) {
      l4j.warn("Unexpected SecurityException in Configuration", se);
    }

    if (variableName.startsWith(ENV_PREFIX)) {
      return System.getenv(variableName.substring(ENV_PREFIX.length()));
    }

    if (conf != null && variableName.startsWith(HIVECONF_PREFIX)) {
      return conf.get(variableName.substring(HIVECONF_PREFIX.length()));
    }

    return null;
  }

  private String applyCoercion(String variableName, String originalValue) {
    if (COERCIONS.containsKey(variableName)) {
      return COERCIONS.get(variableName).getCoerced(originalValue);
    } else {
      return originalValue;
    }
  }

  public static boolean containsVar(String expr) {
    return expr != null && varPat.matcher(expr).find();
  }

  static String substitute(String expr) {
    return expr == null ? null : INSTANCE.substitute(null, expr, 1);
  }

  static String substitute(Configuration conf, String expr) {
    return expr == null ? null : INSTANCE.substitute(conf, expr, 1);
  }

  protected final String substitute(Configuration conf, String expr, int depth) {
    long maxLength = 0;
    if (conf != null) {
      maxLength = HiveConf.getSizeVar(conf, HiveConf.ConfVars.HIVE_QUERY_MAX_LENGTH);
    }
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
        if (maxLength > 0 && builder.length() > maxLength) {
          throw new IllegalStateException("Query length longer than hive.query.max.length ("+builder.length()+">"+maxLength+").");
        }
        prev = match.end();
      }
      if (!found) {
        return eval;
      }
      builder.append(eval.substring(prev));
      if (maxLength > 0 && builder.length() > maxLength) {
        throw new IllegalStateException("Query length longer than hive.query.max.length ("+builder.length()+">"+maxLength+").");
      }
      eval = builder.toString();
    }
    if (s > depth) {
      throw new IllegalStateException(
          "Variable substitution depth is deeper than " + depth + " for expression " + expr);
    }
    return eval;
  }
}
