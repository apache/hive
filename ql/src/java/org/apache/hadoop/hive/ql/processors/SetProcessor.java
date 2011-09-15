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

package org.apache.hadoop.hive.ql.processors;

import static org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.SERIALIZATION_NULL_FORMAT;
import static org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.defaultNullString;

import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.parse.VariableSubstitution;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * SetProcessor.
 *
 */
public class SetProcessor implements CommandProcessor {

  private static String prefix = "set: ";
  public static final String ENV_PREFIX = "env:";
  public static final String SYSTEM_PREFIX = "system:";
  public static final String HIVECONF_PREFIX = "hiveconf:";
  public static final String HIVEVAR_PREFIX = "hivevar:";
  public static final String SET_COLUMN_NAME = "set";

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
      sortedMap.put(oneProp, oneValue);
    }

    // Inserting hive variables
    for (String s : ss.getHiveVariables().keySet()) {
      sortedMap.put(SetProcessor.HIVEVAR_PREFIX + s, ss.getHiveVariables().get(s));
    }

    for (Map.Entry<String, String> entries : sortedMap.entrySet()) {
      ss.out.println(entries.getKey() + "=" + entries.getValue());
    }

    for (Map.Entry<String, String> entry : mapToSortedMap(System.getenv()).entrySet()) {
      ss.out.println(ENV_PREFIX+entry.getKey() + "=" + entry.getValue());
    }

    for (Map.Entry<String, String> entry :
      propertiesToSortedMap(System.getProperties()).entrySet() ) {
      ss.out.println(SYSTEM_PREFIX+entry.getKey() + "=" + entry.getValue());
    }

  }

  private void dumpOption(String s) {
    SessionState ss = SessionState.get();

    if (ss.getConf().get(s) != null) {
      ss.out.println(s + "=" + ss.getConf().get(s));
    } else if (ss.getHiveVariables().containsKey(s)) {
      ss.out.println(s + "=" + ss.getHiveVariables().get(s));
    } else {
      ss.out.println(s + " is undefined");
    }
  }

  public void init() {
  }

  private CommandProcessorResponse setVariable(String varname, String varvalue){
    SessionState ss = SessionState.get();
    if (varname.startsWith(SetProcessor.ENV_PREFIX)){
      ss.err.println("env:* variables can not be set.");
      return new CommandProcessorResponse(1);
    } else if (varname.startsWith(SetProcessor.SYSTEM_PREFIX)){
      String propName = varname.substring(SetProcessor.SYSTEM_PREFIX.length());
      System.getProperties().setProperty(propName, new VariableSubstitution().substitute(ss.getConf(),varvalue));
      return new CommandProcessorResponse(0);
    } else if (varname.startsWith(SetProcessor.HIVECONF_PREFIX)){
      String propName = varname.substring(SetProcessor.HIVECONF_PREFIX.length());
      ss.getConf().set(propName, new VariableSubstitution().substitute(ss.getConf(),varvalue));
      return new CommandProcessorResponse(0);
    } else if (varname.startsWith(SetProcessor.HIVEVAR_PREFIX)) {
      String propName = varname.substring(SetProcessor.HIVEVAR_PREFIX.length());
      ss.getHiveVariables().put(propName, new VariableSubstitution().substitute(ss.getConf(),varvalue));
      return new CommandProcessorResponse(0);
    } else {
      ss.getConf().set(varname, new VariableSubstitution().substitute(ss.getConf(),varvalue) );
      return new CommandProcessorResponse(0);
    }
  }

  private SortedMap<String,String> propertiesToSortedMap(Properties p){
    SortedMap<String,String> sortedPropMap = new TreeMap<String,String>();
    for (Map.Entry<Object, Object> entry :System.getProperties().entrySet() ){
      sortedPropMap.put( (String) entry.getKey(), (String) entry.getValue());
    }
    return sortedPropMap;
  }

  private SortedMap<String,String> mapToSortedMap(Map<String,String> data){
    SortedMap<String,String> sortedEnvMap = new TreeMap<String,String>();
    sortedEnvMap.putAll( data );
    return sortedEnvMap;
  }


  private CommandProcessorResponse getVariable(String varname){
    SessionState ss = SessionState.get();
    if (varname.equals("silent")){
      ss.out.println("silent" + "=" + ss.getIsSilent());
      return new CommandProcessorResponse(0);
    }
    if (varname.startsWith(SetProcessor.SYSTEM_PREFIX)){
      String propName = varname.substring(SetProcessor.SYSTEM_PREFIX.length());
      String result = System.getProperty(propName);
      if (result != null){
        ss.out.println(SetProcessor.SYSTEM_PREFIX+propName + "=" + result);
        return new CommandProcessorResponse(0);
      } else {
        ss.out.println( propName + " is undefined as a system property");
        return new CommandProcessorResponse(1);
      }
    } else if (varname.indexOf(SetProcessor.ENV_PREFIX)==0){
      String var = varname.substring(ENV_PREFIX.length());
      if (System.getenv(var)!=null){
        ss.out.println(SetProcessor.ENV_PREFIX+var + "=" + System.getenv(var));
        return new CommandProcessorResponse(0);
      } else {
        ss.out.println(varname + " is undefined as an environmental variable");
        return new CommandProcessorResponse(1);
      }
    } else if (varname.indexOf(SetProcessor.HIVECONF_PREFIX)==0) {
      String var = varname.substring(SetProcessor.HIVECONF_PREFIX.length());
      if (ss.getConf().get(var)!=null){
        ss.out.println(SetProcessor.HIVECONF_PREFIX+var + "=" + ss.getConf().get(var));
        return new CommandProcessorResponse(0);
      } else {
        ss.out.println(varname + " is undefined as a hive configuration variable");
        return new CommandProcessorResponse(1);
      }
    } else if (varname.indexOf(SetProcessor.HIVEVAR_PREFIX)==0) {
      String var = varname.substring(SetProcessor.HIVEVAR_PREFIX.length());
      if (ss.getHiveVariables().get(var)!=null){
        ss.out.println(SetProcessor.HIVEVAR_PREFIX+var + "=" + ss.getHiveVariables().get(var));
        return new CommandProcessorResponse(0);
      } else {
        ss.out.println(varname + " is undefined as a hive variable");
        return new CommandProcessorResponse(1);
      }
    } else {
      dumpOption(varname);
      return new CommandProcessorResponse(0);
    }
  }

  public CommandProcessorResponse run(String command) {
    SessionState ss = SessionState.get();
    Schema sch = getSchema();

    String nwcmd = command.trim();
    if (nwcmd.equals("")) {
      dumpOptions(ss.getConf().getChangedProperties());
      return new CommandProcessorResponse(0, null, null, sch);
    }

    if (nwcmd.equals("-v")) {
      dumpOptions(ss.getConf().getAllProperties());
      return new CommandProcessorResponse(0, null, null, sch);
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
      return setVariable(part[0],part[1]);
    } else {
      return getVariable(nwcmd);
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

}
