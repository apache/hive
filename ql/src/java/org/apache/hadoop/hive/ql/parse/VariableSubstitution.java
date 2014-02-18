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
package org.apache.hadoop.hive.ql.parse;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.processors.SetProcessor;
import org.apache.hadoop.hive.ql.session.SessionState;

public class VariableSubstitution {

  private static final Log l4j = LogFactory.getLog(VariableSubstitution.class);
  protected static Pattern varPat = Pattern.compile("\\$\\{[^\\}\\$\u0020]+\\}");

  private String getSubstitute(HiveConf conf, String var) {
    String val = null;
    try {
      if (var.startsWith(SetProcessor.SYSTEM_PREFIX)) {
        val = System.getProperty(var.substring(SetProcessor.SYSTEM_PREFIX.length()));
      }
    } catch(SecurityException se) {
      l4j.warn("Unexpected SecurityException in Configuration", se);
    }
    if (val ==null){
      if (var.startsWith(SetProcessor.ENV_PREFIX)){
        val = System.getenv(var.substring(SetProcessor.ENV_PREFIX.length()));
      }
    }
    if (val == null) {
      if (var.startsWith(SetProcessor.HIVECONF_PREFIX)){
        val = conf.get(var.substring(SetProcessor.HIVECONF_PREFIX.length()));
      }
    }
    if (val ==null){
      if(var.startsWith(SetProcessor.HIVEVAR_PREFIX)){
        val =  SessionState.get().getHiveVariables().get(var.substring(SetProcessor.HIVEVAR_PREFIX.length()));
      } else {
        val = SessionState.get().getHiveVariables().get(var);
      }
    }
    return val;
  }

  public String substitute (HiveConf conf, String expr) {

    if (conf.getBoolVar(ConfVars.HIVEVARIABLESUBSTITUTE)){
      l4j.debug("Substitution is on: "+expr);
    } else {
      return expr;
    }
    if (expr == null) {
      return null;
    }
    Matcher match = varPat.matcher("");
    String eval = expr;
    for(int s=0;s<conf.getIntVar(ConfVars.HIVEVARIABLESUBSTITUTEDEPTH); s++) {
      match.reset(eval);
      if (!match.find()) {
        return eval;
      }
      String var = match.group();
      var = var.substring(2, var.length()-1); // remove ${ .. }
      String val = getSubstitute(conf, var);

      if (val == null) {
        l4j.debug("Interpolation result: "+eval);
        return eval; // return literal, no substitution found
      }
      // substitute
      eval = eval.substring(0, match.start())+val+eval.substring(match.end());
    }
    throw new IllegalStateException("Variable substitution depth too large: "
                                    + conf.getIntVar(ConfVars.HIVEVARIABLESUBSTITUTEDEPTH) + " " + expr);
  }
}
