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
  public CommandProcessorResponse run(String command) throws CommandProcessorException {
    return run(SessionState.get(), command);
  }

  @VisibleForTesting
  CommandProcessorResponse run(SessionState ss, String command) throws CommandProcessorException {
    CommandProcessorResponse authErrResp =
        CommandUtil.authorizeCommand(ss, HiveOperationType.RESET, Arrays.asList(command));
    if (authErrResp != null) {
      // there was an authorization issue
      return authErrResp;
    }
    command = command.trim();
    if (StringUtils.isBlank(command)) {
      resetOverridesOnly(ss);
      return new CommandProcessorResponse();
    }
    String[] parts = command.split("\\s+");
    boolean isDefault = false;
    List<String> varNames = new ArrayList<>(parts.length);
    for (String part : parts) {
      if (part.isEmpty()) {
        continue;
      }
      if (DEFAULT_ARG.equals(part)) {
        isDefault = true;
      } else {
        varNames.add(part);
      }
    }
    if (varNames.isEmpty()) {
      throw new CommandProcessorException(1, -1, "No variable names specified", "42000", null);
    }
    String variableNames = "";
    for (String varName : varNames) {
      if (isDefault) {
        if (!variableNames.isEmpty()) {
          variableNames += ", ";
        }
        variableNames += varName;
        resetToDefault(ss, varName);
      } else {
        resetOverrideOnly(ss, varName);
      }
    }
    String message = isDefault ? "Resetting " + variableNames + " to default values" : null;
    return new CommandProcessorResponse(null, message);
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

  private static void resetOverrideOnly(SessionState ss, String varName) {
    if (!ss.getOverriddenConfigurations().containsKey(varName)) {
      return;
    }
    setSessionVariableFromConf(ss, varName, new HiveConf());
    ss.getOverriddenConfigurations().remove(varName);
  }

  private static void setSessionVariableFromConf(SessionState ss, String varName, HiveConf conf) {
    String value = conf.get(varName);
    if (value != null) {
      SetProcessor.setConf(ss, varName, varName, value, false);
    }
  }

  private static CommandProcessorResponse resetToDefault(SessionState ss, String varName)
      throws CommandProcessorException {
    varName = varName.trim();
    try {
      String nonErrorMessage = null;
      if (varName.startsWith(SystemVariables.HIVECONF_PREFIX)){
        String propName = varName.substring(SystemVariables.HIVECONF_PREFIX.length());
        nonErrorMessage = SetProcessor.setConf(
            varName, propName, getConfVar(propName).getDefaultValue(), false);
      } else if (varName.startsWith(SystemVariables.METACONF_PREFIX)) {
        String propName = varName.substring(SystemVariables.METACONF_PREFIX.length());
        HiveConf.ConfVars confVars = getConfVar(propName);
        Hive.get(ss.getConf()).setMetaConf(propName, new VariableSubstitution(new HiveVariableSource() {
          @Override
          public Map<String, String> getHiveVariable() {
            return SessionState.get().getHiveVariables();
          }
        }).substitute(ss.getConf(), confVars.getDefaultValue()));
      } else {
        String defaultVal = getConfVar(varName).getDefaultValue();
        nonErrorMessage = SetProcessor.setConf(varName, varName, defaultVal, true);
        if (varName.equals(HiveConf.ConfVars.HIVE_SESSION_HISTORY_ENABLED.toString())) {
          SessionState.get().updateHistory(Boolean.parseBoolean(defaultVal), ss);
        }
      }
      return new CommandProcessorResponse(null, nonErrorMessage);
    } catch (Exception e) {
      Throwable exception = e instanceof IllegalArgumentException ? null : e;
      throw new CommandProcessorException(1, -1, e.getMessage(), "42000", exception);
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
