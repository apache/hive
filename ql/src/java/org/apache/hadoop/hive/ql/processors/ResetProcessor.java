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

package org.apache.hadoop.hive.ql.processors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveVariableSource;
import org.apache.hadoop.hive.conf.SystemVariables;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.session.SessionState;


public class ResetProcessor implements CommandProcessor {

  private final static String DEFAULT_ARG = "-d";

  @Override
  public CommandProcessorResponse run(String command) {
    return run(SessionState.get(), command);
  }

  @VisibleForTesting
  CommandProcessorResponse run(SessionState ss, String command) {
    CommandProcessorResponse authErrResp =
        CommandUtil.authorizeCommand(ss, HiveOperationType.RESET, Arrays.asList(command));
    if (authErrResp != null) {
      // there was an authorization issue
      return authErrResp;
    }
    command = command.trim();
    if (StringUtils.isBlank(command)) {
      resetOverridesOnly(ss);
      return new CommandProcessorResponse(0);
    }
    String[] parts = command.split("\\s+");
    boolean isDefault = false;
    List<String> varnames = new ArrayList<>(parts.length);
    for (String part : parts) {
      if (part.isEmpty()) {
        continue;
      }
      if (DEFAULT_ARG.equals(part)) {
        isDefault = true;
      } else {
        varnames.add(part);
      }
    }
    if (varnames.isEmpty()) {
      return new CommandProcessorResponse(1, "No variable names specified", "42000");
    }
    String message = "";
    for (String varname : varnames) {
      if (isDefault) {
        if (!message.isEmpty()) {
          message += ", ";
        }
        message += varname;
        resetToDefault(ss, varname);
      } else {
        resetOverrideOnly(ss, varname);
      }
    }
    return new CommandProcessorResponse(0, isDefault
        ? Lists.newArrayList("Resetting " + message + " to default values") : null);
  }

  private static void resetOverridesOnly(SessionState ss) {
    if (ss.getOverriddenConfigurations().isEmpty()) {
      return;
    }
    HiveConf conf = new HiveConf();
    for (String key : ss.getOverriddenConfigurations().keySet()) {
      setSessionVariableFromConf(ss, key, conf);
    }
    ss.getOverriddenConfigurations().clear();
  }

  private static void resetOverrideOnly(SessionState ss, String varname) {
    if (!ss.getOverriddenConfigurations().containsKey(varname)) {
      return;
    }
    setSessionVariableFromConf(ss, varname, new HiveConf());
    ss.getOverriddenConfigurations().remove(varname);
  }

  private static void setSessionVariableFromConf(SessionState ss, String varname, HiveConf conf) {
    String value = conf.get(varname);
    if (value != null) {
      SetProcessor.setConf(ss, varname, varname, value, false);
    }
  }

  private static CommandProcessorResponse resetToDefault(SessionState ss, String varname) {
    varname = varname.trim();
    try {
      String nonErrorMessage = null;
      if (varname.startsWith(SystemVariables.HIVECONF_PREFIX)){
        String propName = varname.substring(SystemVariables.HIVECONF_PREFIX.length());
        nonErrorMessage = SetProcessor.setConf(
            varname, propName, getConfVar(propName).getDefaultValue(), false);
      } else if (varname.startsWith(SystemVariables.METACONF_PREFIX)) {
        String propName = varname.substring(SystemVariables.METACONF_PREFIX.length());
        HiveConf.ConfVars confVars = getConfVar(propName);
        Hive.get(ss.getConf()).setMetaConf(propName, new VariableSubstitution(new HiveVariableSource() {
          @Override
          public Map<String, String> getHiveVariable() {
            return SessionState.get().getHiveVariables();
          }
        }).substitute(ss.getConf(), confVars.getDefaultValue()));
      } else {
        String defaultVal = getConfVar(varname).getDefaultValue();
        nonErrorMessage = SetProcessor.setConf(varname, varname, defaultVal, true);
        if (varname.equals(HiveConf.ConfVars.HIVE_SESSION_HISTORY_ENABLED.toString())) {
          SessionState.get().updateHistory(Boolean.parseBoolean(defaultVal), ss);
        }
      }
      return nonErrorMessage == null ? new CommandProcessorResponse(0)
        : new CommandProcessorResponse(0, Lists.newArrayList(nonErrorMessage));
    } catch (Exception e) {
      return new CommandProcessorResponse(1, e.getMessage(), "42000",
          e instanceof IllegalArgumentException ? null : e);
    }
  }

  private static HiveConf.ConfVars getConfVar(String propName) {
    HiveConf.ConfVars confVars = HiveConf.getConfVars(propName);
    if (confVars == null) {
      throw new IllegalArgumentException(propName + " not found");
    }
    return confVars;
  }

  @Override
  public void close() throws Exception {
  }
}
