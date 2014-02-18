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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.SystemVariables;

import java.util.Map;

public class VariableSubstitution extends SystemVariables {

  private static final Log l4j = LogFactory.getLog(VariableSubstitution.class);

  @Override
  protected String getSubstitute(HiveConf conf, String var) {
    String val = super.getSubstitute(conf, var);
    if (val == null) {
      if (var.startsWith(HIVECONF_PREFIX)) {
        val = conf.get(var.substring(HIVECONF_PREFIX.length()));
      }
    }
    if (val == null){
      Map<String,String> vars = SessionState.get().getHiveVariables();
      if (var.startsWith(HIVEVAR_PREFIX)) {
        val =  vars.get(var.substring(HIVEVAR_PREFIX.length()));
      } else {
        val = vars.get(var);
      }
    }
    return val;
  }

  @Override
  public String substitute (HiveConf conf, String expr) {
    if (conf.getBoolVar(ConfVars.HIVEVARIABLESUBSTITUTE)) {
      l4j.debug("Substitution is on: " + expr);
    } else {
      return expr;
    }
    if (expr == null) {
      return null;
    }
    return super.substitute(conf, expr);
  }
}
