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

package org.apache.hadoop.hive.ql.udf.generic;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterStringColRegExpStringScalar;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
/**
 * UDF to extract a specific group identified by a java regex. Note that if a
 * regexp has a backslash ('\'), then need to specify '\\' For example,
 * regexp_extract('100-200', '(\\d+)-(\\d+)', 1) will return '100'
 */
@Description(name = "rlike,regexp",
    value = "str _FUNC_ regexp - Returns true if str matches regexp and "
    + "false otherwise", extended = "Example:\n"
    + "  > SELECT 'fb' _FUNC_ '.*' FROM src LIMIT 1;\n" + "  true")
@VectorizedExpressions({FilterStringColRegExpStringScalar.class})
public class GenericUDFRegExp extends GenericUDF {
  static final Logger LOG = LoggerFactory.getLogger(GenericUDFRegExp.class.getName());
  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[2];
  private transient Converter[] converters = new Converter[2];
  private final BooleanWritable output = new BooleanWritable();
  private transient boolean isRegexConst;
  private transient String regexConst;
  private transient boolean warned;
  private transient java.util.regex.Pattern patternConst;
  private transient com.google.re2j.Pattern patternConstR2j;
  private boolean useGoogleRegexEngine=false;

  @Override
  public void configure(MapredContext context) {
    if (context != null) {
      if(HiveConf.getBoolVar(context.getJobConf(), HiveConf.ConfVars.HIVEUSEGOOGLEREGEXENGINE)){
        this.useGoogleRegexEngine=true;
      }
    }

  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    SessionState ss = SessionState.get();
    if (ss != null) {
      this.useGoogleRegexEngine = ss.getConf().getBoolVar(HiveConf.ConfVars.HIVEUSEGOOGLEREGEXENGINE);
    }

    checkArgsSize(arguments, 2, 2);

    checkArgPrimitive(arguments, 0);
    checkArgPrimitive(arguments, 1);

    checkArgGroups(arguments, 0, inputTypes, STRING_GROUP);
    checkArgGroups(arguments, 1, inputTypes, STRING_GROUP);

    obtainStringConverter(arguments, 0, inputTypes, converters);
    obtainStringConverter(arguments, 1, inputTypes, converters);

    if (arguments[1] instanceof ConstantObjectInspector) {
      regexConst = getConstantStringValue(arguments, 1);
      if (regexConst != null) {
        if(!useGoogleRegexEngine){
          //if(!HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVEUSEGOOGLEREGEXENGINE)){
          patternConst = Pattern.compile(regexConst);
        }else{
          patternConstR2j = com.google.re2j.Pattern.compile(regexConst);
        }
      }
      isRegexConst = true;
    }

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    String s = getStringValue(arguments, 0, converters);
    if (s == null) {
      return null;
    }

    String regex;
    if (isRegexConst) {
      regex = regexConst;
    } else {
      regex = getStringValue(arguments, 1, converters);
    }
    if (regex == null) {
      return null;
    }

    if (regex.length() == 0) {
      if (!warned) {
        warned = true;
        LOG.warn(getClass().getSimpleName() + " regex is empty. Additional "
            + "warnings for an empty regex will be suppressed.");
      }
      output.set(false);
      return output;
    }

    if(!useGoogleRegexEngine){
      Pattern p;
      if (isRegexConst) {
        p = patternConst;
      } else {
        p = Pattern.compile(regex);
      }

      Matcher m = p.matcher(s);
      output.set(m.find(0));
      return output;
    }else{
      com.google.re2j.Pattern patternR2j;
      if (isRegexConst) {
        patternR2j = patternConstR2j;
      } else {
        patternR2j = com.google.re2j.Pattern.compile(regex);
      }

      com.google.re2j.Matcher m = patternR2j.matcher(s);
      output.set(m.find(0));
      return output;
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return children[0] + " regexp " + children[1];
  }

  @Override
  protected String getFuncName() {
    return "regexp";
  }
}
