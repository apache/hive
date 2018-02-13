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

import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.defaultNullString;

import static org.apache.hadoop.hive.conf.SystemVariables.*;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveVariableSource;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.orc.OrcConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * SetProcessor.
 *
 */
public class SetProcessor implements CommandProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(SetProcessor.class);

  private static final String prefix = "set: ";
  private static final Set<String> removedConfigs =
      Sets.newHashSet("hive.mapred.supports.subdirectories",
          "hive.enforce.sorting","hive.enforce.bucketing",
          "hive.outerjoin.supports.filters");
  // Allow the user to set the ORC properties without getting an error.
  static {
    for(OrcConf var: OrcConf.values()) {
      String name = var.getHiveConfName();
      if (name != null && name.startsWith("hive.")) {
        removedConfigs.add(name);
      }
    }
  }

  private static final String[] PASSWORD_STRINGS = new String[] {"password", "paswd", "pswd"};

  private static final Pattern TIME_ZONE_PATTERN =
      Pattern.compile("^time(\\s)+zone\\s", Pattern.CASE_INSENSITIVE);

  public static boolean getBoolean(String value) {
    if (value.equals("on") || value.equals("true")) {
      return true;
    }
    if (value.equals("off") || value.equals("false")) {
      return false;
    }
    throw new IllegalArgumentException(prefix + "'" + value
        + "' is not a boolean");
  }

  private void dumpOptions(Properties p) {
    SessionState ss = SessionState.get();
    SortedMap<String, String> sortedMap = new TreeMap<String, String>();
    sortedMap.put("silent", (ss.getIsSilent() ? "on" : "off"));
    for (Object one : p.keySet()) {
      String oneProp = (String) one;
      String oneValue = p.getProperty(oneProp);
      if (ss.getConf().isHiddenConfig(oneProp)) {
        continue;
      }
      sortedMap.put(oneProp, oneValue);
    }

    // Inserting hive variables
    for (String s : ss.getHiveVariables().keySet()) {
      sortedMap.put(HIVEVAR_PREFIX + s, ss.getHiveVariables().get(s));
    }

    for (Map.Entry<String, String> entries : sortedMap.entrySet()) {
      ss.out.println(entries.getKey() + "=" + entries.getValue());
    }

    for (Map.Entry<String, String> entry : mapToSortedMap(System.getenv()).entrySet()) {
      if(isHidden(entry.getKey())) {
        continue;
      }
      ss.out.println(ENV_PREFIX+entry.getKey() + "=" + entry.getValue());
    }

    for (Map.Entry<String, String> entry :
      propertiesToSortedMap(System.getProperties()).entrySet() ) {
      if(isHidden(entry.getKey())) {
        continue;
      }
      ss.out.println(SYSTEM_PREFIX+entry.getKey() + "=" + entry.getValue());
    }

  }

  /*
   * Checks if the value contains any of the PASSWORD_STRINGS and if yes
   * return true
   */
  private boolean isHidden(String key) {
    for(String p : PASSWORD_STRINGS) {
      if(key.toLowerCase().contains(p)) {
        return true;
      }
    }
    return false;
  }

  private void dumpOption(String s) {
    SessionState ss = SessionState.get();

    if (ss.getConf().isHiddenConfig(s)) {
      ss.out.println(s + " is a hidden config");
    } else if (ss.getConf().get(s) != null) {
      if (ss.getConf().isEncodedPar(s)) {
        ss.out.println(s + "=" + HiveConf.EncoderDecoderFactory.URL_ENCODER_DECODER
            .decode(ss.getConf().get(s)));
      } else {
        ss.out.println(s + "=" + ss.getConf().get(s));
      }
    } else if (ss.getHiveVariables().containsKey(s)) {
      ss.out.println(s + "=" + ss.getHiveVariables().get(s));
    } else {
      ss.out.println(s + " is undefined");
    }
  }

  public CommandProcessorResponse executeSetVariable(String varname, String varvalue) {
    try {
      return setVariable(varname, varvalue);
    } catch (Exception e) {
      return new CommandProcessorResponse(1, e.getMessage(), "42000",
          e instanceof IllegalArgumentException ? null : e);
    }
  }

  public static CommandProcessorResponse setVariable(
      String varname, String varvalue) throws Exception {
    SessionState ss = SessionState.get();
    if (varvalue.contains("\n")){
      ss.err.println("Warning: Value had a \\n character in it.");
    }
    varname = varname.trim();
    String nonErrorMessage = null;
    if (varname.startsWith(ENV_PREFIX)){
      ss.err.println("env:* variables can not be set.");
      return new CommandProcessorResponse(1); // Should we propagate the error message properly?
    } else if (varname.startsWith(SYSTEM_PREFIX)){
      String propName = varname.substring(SYSTEM_PREFIX.length());
      System.getProperties()
          .setProperty(propName, new VariableSubstitution(new HiveVariableSource() {
            @Override
            public Map<String, String> getHiveVariable() {
              return SessionState.get().getHiveVariables();
            }
          }).substitute(ss.getConf(), varvalue));
    } else if (varname.startsWith(HIVECONF_PREFIX)){
      String propName = varname.substring(HIVECONF_PREFIX.length());
      nonErrorMessage = setConf(varname, propName, varvalue, false);
    } else if (varname.startsWith(HIVEVAR_PREFIX)) {
      String propName = varname.substring(HIVEVAR_PREFIX.length());
      ss.getHiveVariables().put(propName, new VariableSubstitution(new HiveVariableSource() {
        @Override
        public Map<String, String> getHiveVariable() {
          return SessionState.get().getHiveVariables();
        }
      }).substitute(ss.getConf(), varvalue));
    } else if (varname.startsWith(METACONF_PREFIX)) {
      String propName = varname.substring(METACONF_PREFIX.length());
      Hive hive = Hive.get(ss.getConf());
      hive.setMetaConf(propName, new VariableSubstitution(new HiveVariableSource() {
        @Override
        public Map<String, String> getHiveVariable() {
          return SessionState.get().getHiveVariables();
        }
      }).substitute(ss.getConf(), varvalue));
    } else {
      nonErrorMessage = setConf(varname, varname, varvalue, true);
      if (varname.equals(HiveConf.ConfVars.HIVE_SESSION_HISTORY_ENABLED.toString())) {
        SessionState.get().updateHistory(Boolean.parseBoolean(varvalue), ss);
      }
    }
    return nonErrorMessage == null ? new CommandProcessorResponse(0)
      : new CommandProcessorResponse(0, Lists.newArrayList(nonErrorMessage));
  }

  static String setConf(String varname, String key, String varvalue, boolean register)
        throws IllegalArgumentException {
    return setConf(SessionState.get(), varname, key, varvalue, register);
  }

  /**
   * @return A console message that is not strong enough to fail the command (e.g. deprecation).
   */
  static String setConf(SessionState ss, String varname, String key, String varvalue, boolean register)
        throws IllegalArgumentException {
    String result = null;
    HiveConf conf = ss.getConf();
    String value = new VariableSubstitution(new HiveVariableSource() {
      @Override
      public Map<String, String> getHiveVariable() {
        return ss.getHiveVariables();
      }
    }).substitute(conf, varvalue);
    if (conf.getBoolVar(HiveConf.ConfVars.HIVECONFVALIDATION)) {
      HiveConf.ConfVars confVars = HiveConf.getConfVars(key);
      if (confVars != null) {
        if (!confVars.isType(value)) {
          StringBuilder message = new StringBuilder();
          message.append("'SET ").append(varname).append('=').append(varvalue);
          message.append("' FAILED because ").append(key).append(" expects ");
          message.append(confVars.typeString()).append(" type value.");
          throw new IllegalArgumentException(message.toString());
        }
        String fail = confVars.validate(value);
        if (fail != null) {
          StringBuilder message = new StringBuilder();
          message.append("'SET ").append(varname).append('=').append(varvalue);
          message.append("' FAILED in validation : ").append(fail).append('.');
          throw new IllegalArgumentException(message.toString());
        }
      } else if (!removedConfigs.contains(key) && key.startsWith("hive.")) {
        throw new IllegalArgumentException("hive configuration " + key + " does not exists.");
      }
    }
    conf.verifyAndSet(key, value);
    if (HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname.equals(key)) {
      if (!"spark".equals(value)) {
        ss.closeSparkSession();
      }
      if ("mr".equals(value)) {
        result = HiveConf.generateMrDeprecationWarning();
        LOG.warn(result);
      }
    }
    if (register) {
      ss.getOverriddenConfigurations().put(key, value);
    }
    return result;
  }

  private SortedMap<String,String> propertiesToSortedMap(Properties p){
    SortedMap<String,String> sortedPropMap = new TreeMap<String,String>();
    for (Map.Entry<Object, Object> entry : p.entrySet() ){
      sortedPropMap.put( (String) entry.getKey(), (String) entry.getValue());
    }
    return sortedPropMap;
  }

  private SortedMap<String,String> mapToSortedMap(Map<String,String> data){
    SortedMap<String,String> sortedEnvMap = new TreeMap<String,String>();
    sortedEnvMap.putAll( data );
    return sortedEnvMap;
  }

  private CommandProcessorResponse getVariable(String varname) throws Exception {
    SessionState ss = SessionState.get();
    if (varname.equals("silent")){
      ss.out.println("silent" + "=" + ss.getIsSilent());
      return createProcessorSuccessResponse();
    }
    if (varname.startsWith(SYSTEM_PREFIX)) {
      String propName = varname.substring(SYSTEM_PREFIX.length());
      String result = System.getProperty(propName);
      if (result != null) {
        if(isHidden(propName)) {
          ss.out.println(SYSTEM_PREFIX + propName + " is a hidden config");
        } else {
          ss.out.println(SYSTEM_PREFIX + propName + "=" + result);
        }
        return createProcessorSuccessResponse();
      } else {
        ss.out.println(propName + " is undefined as a system property");
        return new CommandProcessorResponse(1);
      }
    } else if (varname.indexOf(ENV_PREFIX) == 0) {
      String var = varname.substring(ENV_PREFIX.length());
      if (System.getenv(var) != null) {
        if(isHidden(var)) {
          ss.out.println(ENV_PREFIX + var + " is a hidden config");
        } else {
          ss.out.println(ENV_PREFIX + var + "=" + System.getenv(var));
        }
        return createProcessorSuccessResponse();
      } else {
        ss.out.println(varname + " is undefined as an environmental variable");
        return new CommandProcessorResponse(1);
      }
    } else if (varname.indexOf(HIVECONF_PREFIX) == 0) {
      String var = varname.substring(HIVECONF_PREFIX.length());
      if (ss.getConf().isHiddenConfig(var)) {
        ss.out.println(HIVECONF_PREFIX + var + " is a hidden config");
        return createProcessorSuccessResponse();
      } if (ss.getConf().get(var) != null) {
        ss.out.println(HIVECONF_PREFIX + var + "=" + ss.getConf().get(var));
        return createProcessorSuccessResponse();
      } else {
        ss.out.println(varname + " is undefined as a hive configuration variable");
        return new CommandProcessorResponse(1);
      }
    } else if (varname.indexOf(HIVEVAR_PREFIX) == 0) {
      String var = varname.substring(HIVEVAR_PREFIX.length());
      if (ss.getHiveVariables().get(var) != null) {
        ss.out.println(HIVEVAR_PREFIX + var + "=" + ss.getHiveVariables().get(var));
        return createProcessorSuccessResponse();
      } else {
        ss.out.println(varname + " is undefined as a hive variable");
        return new CommandProcessorResponse(1);
      }
    } else if (varname.indexOf(METACONF_PREFIX) == 0) {
      String var = varname.substring(METACONF_PREFIX.length());
      Hive hive = Hive.get(ss.getConf());
      String value = hive.getMetaConf(var);
      if (value != null) {
        ss.out.println(METACONF_PREFIX + var + "=" + value);
        return createProcessorSuccessResponse();
      } else {
        ss.out.println(varname + " is undefined as a hive meta variable");
        return new CommandProcessorResponse(1);
      }
    } else {
      dumpOption(varname);
      return createProcessorSuccessResponse();
    }
  }

  private CommandProcessorResponse createProcessorSuccessResponse() {
    return new CommandProcessorResponse(0, null, null, getSchema());
  }

  @Override
  public CommandProcessorResponse run(String command) {
    SessionState ss = SessionState.get();

    String nwcmd = command.trim();
    if (nwcmd.equals("")) {
      dumpOptions(ss.getConf().getChangedProperties());
      return createProcessorSuccessResponse();
    }

    if (nwcmd.equals("-v")) {
      Properties properties = null;
      if (ss.getConf().getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
        Class<?> clazz;
        try {
          clazz = Class.forName("org.apache.tez.dag.api.TezConfiguration");

          Configuration tezConf =
              (Configuration) clazz.getConstructor(Configuration.class).newInstance(ss.getConf());
          properties = HiveConf.getProperties(tezConf);
        } catch (Exception e) {
          return new CommandProcessorResponse(1, e.getMessage(), "42000", e);
        }
      } else {
        properties = ss.getConf().getAllProperties();
      }
      dumpOptions(properties);
      return createProcessorSuccessResponse();
    }

    // Special handling for time-zone
    Matcher matcher = TIME_ZONE_PATTERN.matcher(nwcmd);
    if (matcher.find()) {
      nwcmd = HiveConf.ConfVars.HIVE_LOCAL_TIME_ZONE.varname + "=" + nwcmd.substring(matcher.end());
    }

    String[] part = new String[2];
    int eqIndex = nwcmd.indexOf('=');

    if (nwcmd.contains("=")){
      if (eqIndex == nwcmd.length() - 1) { //x=
        part[0] = nwcmd.substring(0, nwcmd.length() - 1);
        part[1] = "";
      } else { //x=y
        part[0] = nwcmd.substring(0, eqIndex).trim();
        part[1] = nwcmd.substring(eqIndex + 1).trim();
      }
      if (part[0].equals("silent")) {
        ss.setIsSilent(getBoolean(part[1]));
        return new CommandProcessorResponse(0);
      }
      return executeSetVariable(part[0],part[1]);
    }
    try {
      return getVariable(nwcmd);
    } catch (Exception e) {
      return new CommandProcessorResponse(1, e.getMessage(), "42000", e);
    }
  }

// create a Schema object containing the give column
  private Schema getSchema() {
    Schema sch = new Schema();
    FieldSchema tmpFieldSchema = new FieldSchema();

    tmpFieldSchema.setName(SET_COLUMN_NAME);

    tmpFieldSchema.setType(STRING_TYPE_NAME);
    sch.putToProperties(SERIALIZATION_NULL_FORMAT, defaultNullString);
    sch.addToFieldSchemas(tmpFieldSchema);

    return sch;
  }

  @Override
  public void close() throws Exception {
  }
}
