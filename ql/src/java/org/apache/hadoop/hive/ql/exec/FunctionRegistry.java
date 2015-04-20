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

package org.apache.hadoop.hive.ql.exec;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FunctionInfo.FunctionResource;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.SettableUDF;
import org.apache.hadoop.hive.ql.udf.UDAFPercentile;
import org.apache.hadoop.hive.ql.udf.UDFAcos;
import org.apache.hadoop.hive.ql.udf.UDFAscii;
import org.apache.hadoop.hive.ql.udf.UDFAsin;
import org.apache.hadoop.hive.ql.udf.UDFAtan;
import org.apache.hadoop.hive.ql.udf.UDFBase64;
import org.apache.hadoop.hive.ql.udf.UDFBin;
import org.apache.hadoop.hive.ql.udf.UDFConv;
import org.apache.hadoop.hive.ql.udf.UDFCos;
import org.apache.hadoop.hive.ql.udf.UDFDayOfMonth;
import org.apache.hadoop.hive.ql.udf.UDFDegrees;
import org.apache.hadoop.hive.ql.udf.UDFE;
import org.apache.hadoop.hive.ql.udf.UDFExp;
import org.apache.hadoop.hive.ql.udf.UDFFindInSet;
import org.apache.hadoop.hive.ql.udf.UDFFromUnixTime;
import org.apache.hadoop.hive.ql.udf.UDFHex;
import org.apache.hadoop.hive.ql.udf.UDFHour;
import org.apache.hadoop.hive.ql.udf.UDFJson;
import org.apache.hadoop.hive.ql.udf.UDFLength;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.UDFLn;
import org.apache.hadoop.hive.ql.udf.UDFLog;
import org.apache.hadoop.hive.ql.udf.UDFLog10;
import org.apache.hadoop.hive.ql.udf.UDFLog2;
import org.apache.hadoop.hive.ql.udf.UDFMinute;
import org.apache.hadoop.hive.ql.udf.UDFMonth;
import org.apache.hadoop.hive.ql.udf.UDFOPBitAnd;
import org.apache.hadoop.hive.ql.udf.UDFOPBitShiftLeft;
import org.apache.hadoop.hive.ql.udf.UDFOPBitNot;
import org.apache.hadoop.hive.ql.udf.UDFOPBitOr;
import org.apache.hadoop.hive.ql.udf.UDFOPBitShiftRight;
import org.apache.hadoop.hive.ql.udf.UDFOPBitShiftRightUnsigned;
import org.apache.hadoop.hive.ql.udf.UDFOPBitXor;
import org.apache.hadoop.hive.ql.udf.UDFOPLongDivide;
import org.apache.hadoop.hive.ql.udf.UDFPI;
import org.apache.hadoop.hive.ql.udf.UDFParseUrl;
import org.apache.hadoop.hive.ql.udf.UDFRadians;
import org.apache.hadoop.hive.ql.udf.UDFRand;
import org.apache.hadoop.hive.ql.udf.UDFRegExp;
import org.apache.hadoop.hive.ql.udf.UDFRegExpExtract;
import org.apache.hadoop.hive.ql.udf.UDFRegExpReplace;
import org.apache.hadoop.hive.ql.udf.UDFRepeat;
import org.apache.hadoop.hive.ql.udf.UDFReverse;
import org.apache.hadoop.hive.ql.udf.UDFSecond;
import org.apache.hadoop.hive.ql.udf.UDFSign;
import org.apache.hadoop.hive.ql.udf.UDFSin;
import org.apache.hadoop.hive.ql.udf.UDFSpace;
import org.apache.hadoop.hive.ql.udf.UDFSqrt;
import org.apache.hadoop.hive.ql.udf.UDFSubstr;
import org.apache.hadoop.hive.ql.udf.UDFTan;
import org.apache.hadoop.hive.ql.udf.UDFToBoolean;
import org.apache.hadoop.hive.ql.udf.UDFToByte;
import org.apache.hadoop.hive.ql.udf.UDFToDouble;
import org.apache.hadoop.hive.ql.udf.UDFToFloat;
import org.apache.hadoop.hive.ql.udf.UDFToInteger;
import org.apache.hadoop.hive.ql.udf.UDFToLong;
import org.apache.hadoop.hive.ql.udf.UDFToShort;
import org.apache.hadoop.hive.ql.udf.UDFToString;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.UDFUnbase64;
import org.apache.hadoop.hive.ql.udf.UDFUnhex;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.apache.hadoop.hive.ql.udf.UDFYear;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.ql.udf.ptf.MatchPath.MatchPathResolver;
import org.apache.hadoop.hive.ql.udf.ptf.Noop.NoopResolver;
import org.apache.hadoop.hive.ql.udf.ptf.NoopStreaming.NoopStreamingResolver;
import org.apache.hadoop.hive.ql.udf.ptf.NoopWithMap.NoopWithMapResolver;
import org.apache.hadoop.hive.ql.udf.ptf.NoopWithMapStreaming.NoopWithMapStreamingResolver;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionResolver;
import org.apache.hadoop.hive.ql.udf.ptf.WindowingTableFunction.WindowingTableFunctionResolver;
import org.apache.hadoop.hive.ql.udf.xml.GenericUDFXPath;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathBoolean;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathDouble;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathFloat;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathInteger;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathLong;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathShort;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathString;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.common.util.AnnotationUtils;

/**
 * FunctionRegistry.
 */
public final class FunctionRegistry {

  private static final Log LOG = LogFactory.getLog(FunctionRegistry.class);

  /*
   * PTF variables
   * */

  public static final String LEAD_FUNC_NAME = "lead";
  public static final String LAG_FUNC_NAME = "lag";
  public static final String LAST_VALUE_FUNC_NAME = "last_value";

  public static final String UNARY_PLUS_FUNC_NAME = "positive";
  public static final String UNARY_MINUS_FUNC_NAME = "negative";

  public static final String WINDOWING_TABLE_FUNCTION = "windowingtablefunction";
  private static final String NOOP_TABLE_FUNCTION = "noop";
  private static final String NOOP_MAP_TABLE_FUNCTION = "noopwithmap";
  private static final String NOOP_STREAMING_TABLE_FUNCTION = "noopstreaming";
  private static final String NOOP_STREAMING_MAP_TABLE_FUNCTION = "noopwithmapstreaming";
  private static final String MATCH_PATH_TABLE_FUNCTION = "matchpath";

  public static final Set<String> HIVE_OPERATORS = new HashSet<String>();

  static {
    HIVE_OPERATORS.addAll(Arrays.asList(
        "+", "-", "*", "/", "%", "div", "&", "|", "^", "~",
        "and", "or", "not", "!",
        "=", "==", "<=>", "!=", "<>", "<", "<=", ">", ">=",
        "index"));
  }

  // registry for system functions
  private static final Registry system = new Registry(true);

  static {
    system.registerGenericUDF("concat", GenericUDFConcat.class);
    system.registerUDF("substr", UDFSubstr.class, false);
    system.registerUDF("substring", UDFSubstr.class, false);
    system.registerUDF("space", UDFSpace.class, false);
    system.registerUDF("repeat", UDFRepeat.class, false);
    system.registerUDF("ascii", UDFAscii.class, false);
    system.registerGenericUDF("lpad", GenericUDFLpad.class);
    system.registerGenericUDF("rpad", GenericUDFRpad.class);
    system.registerGenericUDF("levenshtein", GenericUDFLevenshtein.class);
    system.registerGenericUDF("soundex", GenericUDFSoundex.class);

    system.registerGenericUDF("size", GenericUDFSize.class);

    system.registerGenericUDF("round", GenericUDFRound.class);
    system.registerGenericUDF("floor", GenericUDFFloor.class);
    system.registerUDF("sqrt", UDFSqrt.class, false);
    system.registerGenericUDF("cbrt", GenericUDFCbrt.class);
    system.registerGenericUDF("ceil", GenericUDFCeil.class);
    system.registerGenericUDF("ceiling", GenericUDFCeil.class);
    system.registerUDF("rand", UDFRand.class, false);
    system.registerGenericUDF("abs", GenericUDFAbs.class);
    system.registerGenericUDF("pmod", GenericUDFPosMod.class);

    system.registerUDF("ln", UDFLn.class, false);
    system.registerUDF("log2", UDFLog2.class, false);
    system.registerUDF("sin", UDFSin.class, false);
    system.registerUDF("asin", UDFAsin.class, false);
    system.registerUDF("cos", UDFCos.class, false);
    system.registerUDF("acos", UDFAcos.class, false);
    system.registerUDF("log10", UDFLog10.class, false);
    system.registerUDF("log", UDFLog.class, false);
    system.registerUDF("exp", UDFExp.class, false);
    system.registerGenericUDF("power", GenericUDFPower.class);
    system.registerGenericUDF("pow", GenericUDFPower.class);
    system.registerUDF("sign", UDFSign.class, false);
    system.registerUDF("pi", UDFPI.class, false);
    system.registerUDF("degrees", UDFDegrees.class, false);
    system.registerUDF("radians", UDFRadians.class, false);
    system.registerUDF("atan", UDFAtan.class, false);
    system.registerUDF("tan", UDFTan.class, false);
    system.registerUDF("e", UDFE.class, false);
    system.registerGenericUDF("factorial", GenericUDFFactorial.class);

    system.registerUDF("conv", UDFConv.class, false);
    system.registerUDF("bin", UDFBin.class, false);
    system.registerUDF("hex", UDFHex.class, false);
    system.registerUDF("unhex", UDFUnhex.class, false);
    system.registerUDF("base64", UDFBase64.class, false);
    system.registerUDF("unbase64", UDFUnbase64.class, false);

    system.registerGenericUDF("encode", GenericUDFEncode.class);
    system.registerGenericUDF("decode", GenericUDFDecode.class);

    system.registerGenericUDF("upper", GenericUDFUpper.class);
    system.registerGenericUDF("lower", GenericUDFLower.class);
    system.registerGenericUDF("ucase", GenericUDFUpper.class);
    system.registerGenericUDF("lcase", GenericUDFLower.class);
    system.registerGenericUDF("trim", GenericUDFTrim.class);
    system.registerGenericUDF("ltrim", GenericUDFLTrim.class);
    system.registerGenericUDF("rtrim", GenericUDFRTrim.class);
    system.registerUDF("length", UDFLength.class, false);
    system.registerUDF("reverse", UDFReverse.class, false);
    system.registerGenericUDF("field", GenericUDFField.class);
    system.registerUDF("find_in_set", UDFFindInSet.class, false);
    system.registerGenericUDF("initcap", GenericUDFInitCap.class);

    system.registerUDF("like", UDFLike.class, true);
    system.registerUDF("rlike", UDFRegExp.class, true);
    system.registerUDF("regexp", UDFRegExp.class, true);
    system.registerUDF("regexp_replace", UDFRegExpReplace.class, false);
    system.registerUDF("regexp_extract", UDFRegExpExtract.class, false);
    system.registerUDF("parse_url", UDFParseUrl.class, false);
    system.registerGenericUDF("nvl", GenericUDFNvl.class);
    system.registerGenericUDF("split", GenericUDFSplit.class);
    system.registerGenericUDF("str_to_map", GenericUDFStringToMap.class);
    system.registerGenericUDF("translate", GenericUDFTranslate.class);

    system.registerGenericUDF(UNARY_PLUS_FUNC_NAME, GenericUDFOPPositive.class);
    system.registerGenericUDF(UNARY_MINUS_FUNC_NAME, GenericUDFOPNegative.class);

    system.registerUDF("day", UDFDayOfMonth.class, false);
    system.registerUDF("dayofmonth", UDFDayOfMonth.class, false);
    system.registerUDF("month", UDFMonth.class, false);
    system.registerUDF("year", UDFYear.class, false);
    system.registerUDF("hour", UDFHour.class, false);
    system.registerUDF("minute", UDFMinute.class, false);
    system.registerUDF("second", UDFSecond.class, false);
    system.registerUDF("from_unixtime", UDFFromUnixTime.class, false);
    system.registerGenericUDF("to_date", GenericUDFDate.class);
    system.registerUDF("weekofyear", UDFWeekOfYear.class, false);
    system.registerGenericUDF("last_day", GenericUDFLastDay.class);
    system.registerGenericUDF("next_day", GenericUDFNextDay.class);
    system.registerGenericUDF("trunc", GenericUDFTrunc.class);
    system.registerGenericUDF("date_format", GenericUDFDateFormat.class);

    system.registerGenericUDF("date_add", GenericUDFDateAdd.class);
    system.registerGenericUDF("date_sub", GenericUDFDateSub.class);
    system.registerGenericUDF("datediff", GenericUDFDateDiff.class);
    system.registerGenericUDF("add_months", GenericUDFAddMonths.class);
    system.registerGenericUDF("months_between", GenericUDFMonthsBetween.class);

    system.registerUDF("get_json_object", UDFJson.class, false);

    system.registerUDF("xpath_string", UDFXPathString.class, false);
    system.registerUDF("xpath_boolean", UDFXPathBoolean.class, false);
    system.registerUDF("xpath_number", UDFXPathDouble.class, false);
    system.registerUDF("xpath_double", UDFXPathDouble.class, false);
    system.registerUDF("xpath_float", UDFXPathFloat.class, false);
    system.registerUDF("xpath_long", UDFXPathLong.class, false);
    system.registerUDF("xpath_int", UDFXPathInteger.class, false);
    system.registerUDF("xpath_short", UDFXPathShort.class, false);
    system.registerGenericUDF("xpath", GenericUDFXPath.class);

    system.registerGenericUDF("+", GenericUDFOPPlus.class);
    system.registerGenericUDF("-", GenericUDFOPMinus.class);
    system.registerGenericUDF("*", GenericUDFOPMultiply.class);
    system.registerGenericUDF("/", GenericUDFOPDivide.class);
    system.registerGenericUDF("%", GenericUDFOPMod.class);
    system.registerUDF("div", UDFOPLongDivide.class, true);

    system.registerUDF("&", UDFOPBitAnd.class, true);
    system.registerUDF("|", UDFOPBitOr.class, true);
    system.registerUDF("^", UDFOPBitXor.class, true);
    system.registerUDF("~", UDFOPBitNot.class, true);
    system.registerUDF("shiftleft", UDFOPBitShiftLeft.class, true);
    system.registerUDF("shiftright", UDFOPBitShiftRight.class, true);
    system.registerUDF("shiftrightunsigned", UDFOPBitShiftRightUnsigned.class, true);

    system.registerGenericUDF("current_database", UDFCurrentDB.class);
    system.registerGenericUDF("current_date", GenericUDFCurrentDate.class);
    system.registerGenericUDF("current_timestamp", GenericUDFCurrentTimestamp.class);
    system.registerGenericUDF("current_user", GenericUDFCurrentUser.class);

    system.registerGenericUDF("isnull", GenericUDFOPNull.class);
    system.registerGenericUDF("isnotnull", GenericUDFOPNotNull.class);

    system.registerGenericUDF("if", GenericUDFIf.class);
    system.registerGenericUDF("in", GenericUDFIn.class);
    system.registerGenericUDF("and", GenericUDFOPAnd.class);
    system.registerGenericUDF("or", GenericUDFOPOr.class);
    system.registerGenericUDF("=", GenericUDFOPEqual.class);
    system.registerGenericUDF("==", GenericUDFOPEqual.class);
    system.registerGenericUDF("<=>", GenericUDFOPEqualNS.class);
    system.registerGenericUDF("!=", GenericUDFOPNotEqual.class);
    system.registerGenericUDF("<>", GenericUDFOPNotEqual.class);
    system.registerGenericUDF("<", GenericUDFOPLessThan.class);
    system.registerGenericUDF("<=", GenericUDFOPEqualOrLessThan.class);
    system.registerGenericUDF(">", GenericUDFOPGreaterThan.class);
    system.registerGenericUDF(">=", GenericUDFOPEqualOrGreaterThan.class);
    system.registerGenericUDF("not", GenericUDFOPNot.class);
    system.registerGenericUDF("!", GenericUDFOPNot.class);
    system.registerGenericUDF("between", GenericUDFBetween.class);

    system.registerGenericUDF("ewah_bitmap_and", GenericUDFEWAHBitmapAnd.class);
    system.registerGenericUDF("ewah_bitmap_or", GenericUDFEWAHBitmapOr.class);
    system.registerGenericUDF("ewah_bitmap_empty", GenericUDFEWAHBitmapEmpty.class);

    // Aliases for Java Class Names
    // These are used in getImplicitConvertUDFMethod
    system.registerUDF(serdeConstants.BOOLEAN_TYPE_NAME, UDFToBoolean.class, false, UDFToBoolean.class.getSimpleName());
    system.registerUDF(serdeConstants.TINYINT_TYPE_NAME, UDFToByte.class, false, UDFToByte.class.getSimpleName());
    system.registerUDF(serdeConstants.SMALLINT_TYPE_NAME, UDFToShort.class, false, UDFToShort.class.getSimpleName());
    system.registerUDF(serdeConstants.INT_TYPE_NAME, UDFToInteger.class, false, UDFToInteger.class.getSimpleName());
    system.registerUDF(serdeConstants.BIGINT_TYPE_NAME, UDFToLong.class, false, UDFToLong.class.getSimpleName());
    system.registerUDF(serdeConstants.FLOAT_TYPE_NAME, UDFToFloat.class, false, UDFToFloat.class.getSimpleName());
    system.registerUDF(serdeConstants.DOUBLE_TYPE_NAME, UDFToDouble.class, false, UDFToDouble.class.getSimpleName());
    system.registerUDF(serdeConstants.STRING_TYPE_NAME, UDFToString.class, false, UDFToString.class.getSimpleName());

    system.registerGenericUDF(serdeConstants.DATE_TYPE_NAME, GenericUDFToDate.class);
    system.registerGenericUDF(serdeConstants.TIMESTAMP_TYPE_NAME, GenericUDFTimestamp.class);
    system.registerGenericUDF(serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME, GenericUDFToIntervalYearMonth.class);
    system.registerGenericUDF(serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME, GenericUDFToIntervalDayTime.class);
    system.registerGenericUDF(serdeConstants.BINARY_TYPE_NAME, GenericUDFToBinary.class);
    system.registerGenericUDF(serdeConstants.DECIMAL_TYPE_NAME, GenericUDFToDecimal.class);
    system.registerGenericUDF(serdeConstants.VARCHAR_TYPE_NAME, GenericUDFToVarchar.class);
    system.registerGenericUDF(serdeConstants.CHAR_TYPE_NAME, GenericUDFToChar.class);

    // Aggregate functions
    system.registerGenericUDAF("max", new GenericUDAFMax());
    system.registerGenericUDAF("min", new GenericUDAFMin());

    system.registerGenericUDAF("sum", new GenericUDAFSum());
    system.registerGenericUDAF("count", new GenericUDAFCount());
    system.registerGenericUDAF("avg", new GenericUDAFAverage());
    system.registerGenericUDAF("std", new GenericUDAFStd());
    system.registerGenericUDAF("stddev", new GenericUDAFStd());
    system.registerGenericUDAF("stddev_pop", new GenericUDAFStd());
    system.registerGenericUDAF("stddev_samp", new GenericUDAFStdSample());
    system.registerGenericUDAF("variance", new GenericUDAFVariance());
    system.registerGenericUDAF("var_pop", new GenericUDAFVariance());
    system.registerGenericUDAF("var_samp", new GenericUDAFVarianceSample());
    system.registerGenericUDAF("covar_pop", new GenericUDAFCovariance());
    system.registerGenericUDAF("covar_samp", new GenericUDAFCovarianceSample());
    system.registerGenericUDAF("corr", new GenericUDAFCorrelation());
    system.registerGenericUDAF("histogram_numeric", new GenericUDAFHistogramNumeric());
    system.registerGenericUDAF("percentile_approx", new GenericUDAFPercentileApprox());
    system.registerGenericUDAF("collect_set", new GenericUDAFCollectSet());
    system.registerGenericUDAF("collect_list", new GenericUDAFCollectList());

    system.registerGenericUDAF("ngrams", new GenericUDAFnGrams());
    system.registerGenericUDAF("context_ngrams", new GenericUDAFContextNGrams());

    system.registerGenericUDAF("ewah_bitmap", new GenericUDAFEWAHBitmap());

    system.registerGenericUDAF("compute_stats", new GenericUDAFComputeStats());

    system.registerUDAF("percentile", UDAFPercentile.class);


    // Generic UDFs
    system.registerGenericUDF("reflect", GenericUDFReflect.class);
    system.registerGenericUDF("reflect2", GenericUDFReflect2.class);
    system.registerGenericUDF("java_method", GenericUDFReflect.class);

    system.registerGenericUDF("array", GenericUDFArray.class);
    system.registerGenericUDF("assert_true", GenericUDFAssertTrue.class);
    system.registerGenericUDF("map", GenericUDFMap.class);
    system.registerGenericUDF("struct", GenericUDFStruct.class);
    system.registerGenericUDF("named_struct", GenericUDFNamedStruct.class);
    system.registerGenericUDF("create_union", GenericUDFUnion.class);

    system.registerGenericUDF("case", GenericUDFCase.class);
    system.registerGenericUDF("when", GenericUDFWhen.class);
    system.registerGenericUDF("hash", GenericUDFHash.class);
    system.registerGenericUDF("coalesce", GenericUDFCoalesce.class);
    system.registerGenericUDF("index", GenericUDFIndex.class);
    system.registerGenericUDF("in_file", GenericUDFInFile.class);
    system.registerGenericUDF("instr", GenericUDFInstr.class);
    system.registerGenericUDF("locate", GenericUDFLocate.class);
    system.registerGenericUDF("elt", GenericUDFElt.class);
    system.registerGenericUDF("concat_ws", GenericUDFConcatWS.class);
    system.registerGenericUDF("sort_array", GenericUDFSortArray.class);
    system.registerGenericUDF("array_contains", GenericUDFArrayContains.class);
    system.registerGenericUDF("sentences", GenericUDFSentences.class);
    system.registerGenericUDF("map_keys", GenericUDFMapKeys.class);
    system.registerGenericUDF("map_values", GenericUDFMapValues.class);
    system.registerGenericUDF("format_number", GenericUDFFormatNumber.class);
    system.registerGenericUDF("printf", GenericUDFPrintf.class);
    system.registerGenericUDF("greatest", GenericUDFGreatest.class);
    system.registerGenericUDF("least", GenericUDFLeast.class);

    system.registerGenericUDF("from_utc_timestamp", GenericUDFFromUtcTimestamp.class);
    system.registerGenericUDF("to_utc_timestamp", GenericUDFToUtcTimestamp.class);

    system.registerGenericUDF("unix_timestamp", GenericUDFUnixTimeStamp.class);
    system.registerGenericUDF("to_unix_timestamp", GenericUDFToUnixTimeStamp.class);

    // Generic UDTF's
    system.registerGenericUDTF("explode", GenericUDTFExplode.class);
    system.registerGenericUDTF("inline", GenericUDTFInline.class);
    system.registerGenericUDTF("json_tuple", GenericUDTFJSONTuple.class);
    system.registerGenericUDTF("parse_url_tuple", GenericUDTFParseUrlTuple.class);
    system.registerGenericUDTF("posexplode", GenericUDTFPosExplode.class);
    system.registerGenericUDTF("stack", GenericUDTFStack.class);

    //PTF declarations
    system.registerGenericUDF(LEAD_FUNC_NAME, GenericUDFLead.class);
    system.registerGenericUDF(LAG_FUNC_NAME, GenericUDFLag.class);

    system.registerGenericUDAF("row_number", new GenericUDAFRowNumber());
    system.registerGenericUDAF("rank", new GenericUDAFRank());
    system.registerGenericUDAF("dense_rank", new GenericUDAFDenseRank());
    system.registerGenericUDAF("percent_rank", new GenericUDAFPercentRank());
    system.registerGenericUDAF("cume_dist", new GenericUDAFCumeDist());
    system.registerGenericUDAF("ntile", new GenericUDAFNTile());
    system.registerGenericUDAF("first_value", new GenericUDAFFirstValue());
    system.registerGenericUDAF("last_value", new GenericUDAFLastValue());
    system.registerWindowFunction(LEAD_FUNC_NAME, new GenericUDAFLead());
    system.registerWindowFunction(LAG_FUNC_NAME, new GenericUDAFLag());

    system.registerTableFunction(NOOP_TABLE_FUNCTION, NoopResolver.class);
    system.registerTableFunction(NOOP_MAP_TABLE_FUNCTION, NoopWithMapResolver.class);
    system.registerTableFunction(NOOP_STREAMING_TABLE_FUNCTION, NoopStreamingResolver.class);
    system.registerTableFunction(NOOP_STREAMING_MAP_TABLE_FUNCTION, NoopWithMapStreamingResolver.class);
    system.registerTableFunction(WINDOWING_TABLE_FUNCTION, WindowingTableFunctionResolver.class);
    system.registerTableFunction(MATCH_PATH_TABLE_FUNCTION, MatchPathResolver.class);
  }

  public static String getNormalizedFunctionName(String fn) throws SemanticException {
    // Does the same thing as getFunctionInfo, except for getting the function info.
    fn = fn.toLowerCase();
    return (FunctionUtils.isQualifiedFunctionName(fn) || getFunctionInfo(fn) != null) ? fn
        : FunctionUtils.qualifyFunctionName(
            fn, SessionState.get().getCurrentDatabase().toLowerCase());
  }

  public static FunctionInfo getFunctionInfo(String functionName) throws SemanticException {
    FunctionInfo info = getTemporaryFunctionInfo(functionName);
    return info != null ? info : system.getFunctionInfo(functionName);
  }

  public static FunctionInfo getTemporaryFunctionInfo(String functionName) throws SemanticException {
    Registry registry = SessionState.getRegistry();
    return registry == null ? null : registry.getFunctionInfo(functionName);
  }

  public static WindowFunctionInfo getWindowFunctionInfo(String functionName)
      throws SemanticException {
    Registry registry = SessionState.getRegistry();
    WindowFunctionInfo info =
        registry == null ? null : registry.getWindowFunctionInfo(functionName);
    return info != null ? info : system.getWindowFunctionInfo(functionName);
  }

  public static Set<String> getFunctionNames() {
    Set<String> names = new TreeSet<String>();
    names.addAll(system.getCurrentFunctionNames());
    if (SessionState.getRegistry() != null) {
      names.addAll(SessionState.getRegistry().getCurrentFunctionNames());
    }
    return names;
  }

  public static Set<String> getFunctionNames(String funcPatternStr) {
    Set<String> funcNames = new TreeSet<String>();
    funcNames.addAll(system.getFunctionNames(funcPatternStr));
    if (SessionState.getRegistry() != null) {
      funcNames.addAll(SessionState.getRegistry().getFunctionNames(funcPatternStr));
    }
    return funcNames;
  }

  /**
   * Returns a set of registered function names which matchs the given pattern.
   * This is used for the CLI command "SHOW FUNCTIONS LIKE 'regular expression';"
   *
   * @param funcPatternStr
   *          regular expression of the interested function names
   * @return set of strings contains function names
   */
  public static Set<String> getFunctionNamesByLikePattern(String funcPatternStr) {
    Set<String> funcNames = new TreeSet<String>();
    Set<String> allFuncs = getFunctionNames();
    String[] subpatterns = funcPatternStr.trim().split("\\|");
    for (String subpattern : subpatterns) {
      subpattern = "(?i)" + subpattern.replaceAll("\\*", ".*");
      try {
        Pattern patternObj = Pattern.compile(subpattern);
        for (String funcName : allFuncs) {
          if (patternObj.matcher(funcName).matches()) {
            funcNames.add(funcName);
          }
        }
      } catch (PatternSyntaxException e) {
        continue;
      }
    }
    return funcNames;
  }

  /**
   * Returns the set of synonyms of the supplied function.
   *
   * @param funcName the name of the function
   * @return Set of synonyms for funcName
   */
  public static Set<String> getFunctionSynonyms(String funcName) throws SemanticException {
    FunctionInfo funcInfo = getFunctionInfo(funcName);
    if (null == funcInfo) {
      return Collections.emptySet();
    }
    Set<String> synonyms = new LinkedHashSet<String>();
    system.getFunctionSynonyms(funcName, funcInfo, synonyms);
    if (SessionState.getRegistry() != null) {
      SessionState.getRegistry().getFunctionSynonyms(funcName, funcInfo, synonyms);
    }
    return synonyms;
  }

  // The ordering of types here is used to determine which numeric types
  // are common/convertible to one another. Probably better to rely on the
  // ordering explicitly defined here than to assume that the enum values
  // that were arbitrarily assigned in PrimitiveCategory work for our purposes.
  static EnumMap<PrimitiveCategory, Integer> numericTypes =
      new EnumMap<PrimitiveCategory, Integer>(PrimitiveCategory.class);
  static List<PrimitiveCategory> numericTypeList = new ArrayList<PrimitiveCategory>();

  static void registerNumericType(PrimitiveCategory primitiveCategory, int level) {
    numericTypeList.add(primitiveCategory);
    numericTypes.put(primitiveCategory, level);
  }

  static {
    registerNumericType(PrimitiveCategory.BYTE, 1);
    registerNumericType(PrimitiveCategory.SHORT, 2);
    registerNumericType(PrimitiveCategory.INT, 3);
    registerNumericType(PrimitiveCategory.LONG, 4);
    registerNumericType(PrimitiveCategory.FLOAT, 5);
    registerNumericType(PrimitiveCategory.DOUBLE, 6);
    registerNumericType(PrimitiveCategory.DECIMAL, 7);
    registerNumericType(PrimitiveCategory.STRING, 8);
  }

  /**
   * Check if the given type is numeric. String is considered numeric when used in
   * numeric operators.
   *
   * @param typeInfo
   * @return
   */
  public static boolean isNumericType(PrimitiveTypeInfo typeInfo) {
    switch (typeInfo.getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case DECIMAL:
    case FLOAT:
    case DOUBLE:
    case STRING: // String or string equivalent is considered numeric when used in arithmetic operator.
    case VARCHAR:
    case CHAR:
    case VOID: // NULL is considered numeric type for arithmetic operators.
      return true;
    default:
      return false;
    }
  }

  /**
   * Check if a type is exact (not approximate such as float and double). String is considered as
   * double, thus not exact.
   *
   * @param typeInfo
   * @return
   */
  public static boolean isExactNumericType(PrimitiveTypeInfo typeInfo) {
    switch (typeInfo.getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case DECIMAL:
      return true;
    default:
      return false;
    }
  }

  static int getCommonLength(int aLen, int bLen) {
    int maxLength;
    if (aLen < 0 || bLen < 0) {
      // negative length should take precedence over positive value?
      maxLength = -1;
    } else {
      maxLength = Math.max(aLen, bLen);
    }
    return maxLength;
  }

  /**
   * Given 2 TypeInfo types and the PrimitiveCategory selected as the common class between the two,
   * return a TypeInfo corresponding to the common PrimitiveCategory, and with type qualifiers
   * (if applicable) that match the 2 TypeInfo types.
   * Examples:
   *   varchar(10), varchar(20), primitive category varchar => varchar(20)
   *   date, string, primitive category string => string
   * @param a  TypeInfo of the first type
   * @param b  TypeInfo of the second type
   * @param typeCategory PrimitiveCategory of the designated common type between a and b
   * @return TypeInfo represented by the primitive category, with any applicable type qualifiers.
   */
  public static TypeInfo getTypeInfoForPrimitiveCategory(
      PrimitiveTypeInfo a, PrimitiveTypeInfo b, PrimitiveCategory typeCategory) {
    // For types with parameters (like varchar), we need to determine the type parameters
    // that should be added to this type, based on the original 2 TypeInfos.
    int maxLength;
    switch (typeCategory) {
      case CHAR:
        maxLength = getCommonLength(
            TypeInfoUtils.getCharacterLengthForType(a),
            TypeInfoUtils.getCharacterLengthForType(b));
        return TypeInfoFactory.getCharTypeInfo(maxLength);
      case VARCHAR:
        maxLength = getCommonLength(
            TypeInfoUtils.getCharacterLengthForType(a),
            TypeInfoUtils.getCharacterLengthForType(b));
        return TypeInfoFactory.getVarcharTypeInfo(maxLength);
      case DECIMAL:
      return HiveDecimalUtils.getDecimalTypeForPrimitiveCategories(a, b);
      default:
        // Type doesn't require any qualifiers.
        return TypeInfoFactory.getPrimitiveTypeInfo(
          PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveCategory(typeCategory).typeName);
    }
  }

  /**
   * Find a common class for union-all operator
   */
  public static TypeInfo getCommonClassForUnionAll(TypeInfo a, TypeInfo b) {
    if (a.equals(b)) {
      return a;
    }
    if (a.getCategory() != Category.PRIMITIVE || b.getCategory() != Category.PRIMITIVE) {
      return null;
    }
    PrimitiveCategory pcA = ((PrimitiveTypeInfo)a).getPrimitiveCategory();
    PrimitiveCategory pcB = ((PrimitiveTypeInfo)b).getPrimitiveCategory();

    if (pcA == pcB) {
      // Same primitive category but different qualifiers.
      return getTypeInfoForPrimitiveCategory((PrimitiveTypeInfo)a, (PrimitiveTypeInfo)b, pcA);
    }

    PrimitiveGrouping pgA = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(pcA);
    PrimitiveGrouping pgB = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(pcB);
    // handle string types properly
    if (pgA == PrimitiveGrouping.STRING_GROUP && pgB == PrimitiveGrouping.STRING_GROUP) {
      return getTypeInfoForPrimitiveCategory(
          (PrimitiveTypeInfo)a, (PrimitiveTypeInfo)b,PrimitiveCategory.STRING);
    }

    if (FunctionRegistry.implicitConvertible(a, b)) {
      return getTypeInfoForPrimitiveCategory((PrimitiveTypeInfo)a, (PrimitiveTypeInfo)b, pcB);
    }
    if (FunctionRegistry.implicitConvertible(b, a)) {
      return getTypeInfoForPrimitiveCategory((PrimitiveTypeInfo)a, (PrimitiveTypeInfo)b, pcA);
    }
    for (PrimitiveCategory t : numericTypeList) {
      if (FunctionRegistry.implicitConvertible(pcA, t)
          && FunctionRegistry.implicitConvertible(pcB, t)) {
        return getTypeInfoForPrimitiveCategory((PrimitiveTypeInfo)a, (PrimitiveTypeInfo)b, t);
      }
    }

    return null;
  }

  /**
   * Find a common class that objects of both TypeInfo a and TypeInfo b can
   * convert to. This is used for comparing objects of type a and type b.
   *
   * When we are comparing string and double, we will always convert both of
   * them to double and then compare.
   *
   * @return null if no common class could be found.
   */
  public static TypeInfo getCommonClassForComparison(TypeInfo a, TypeInfo b) {
    // If same return one of them
    if (a.equals(b)) {
      return a;
    }
    if (a.getCategory() != Category.PRIMITIVE || b.getCategory() != Category.PRIMITIVE) {
      return null;
    }
    PrimitiveCategory pcA = ((PrimitiveTypeInfo)a).getPrimitiveCategory();
    PrimitiveCategory pcB = ((PrimitiveTypeInfo)b).getPrimitiveCategory();

    if (pcA == pcB) {
      // Same primitive category but different qualifiers.
      // Rely on getTypeInfoForPrimitiveCategory() to sort out the type params.
      return getTypeInfoForPrimitiveCategory((PrimitiveTypeInfo)a, (PrimitiveTypeInfo)b, pcA);
    }

    PrimitiveGrouping pgA = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(pcA);
    PrimitiveGrouping pgB = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(pcB);
    // handle string types properly
    if (pgA == PrimitiveGrouping.STRING_GROUP && pgB == PrimitiveGrouping.STRING_GROUP) {
      // Compare as strings. Char comparison semantics may be different if/when implemented.
      return getTypeInfoForPrimitiveCategory(
          (PrimitiveTypeInfo)a, (PrimitiveTypeInfo)b,PrimitiveCategory.STRING);
    }

    // Another special case, because timestamp is not implicitly convertible to numeric types.
    if ((pgA == PrimitiveGrouping.NUMERIC_GROUP || pgB == PrimitiveGrouping.NUMERIC_GROUP)
        && (pcA == PrimitiveCategory.TIMESTAMP || pcB == PrimitiveCategory.TIMESTAMP)) {
      return TypeInfoFactory.doubleTypeInfo;
    }

    for (PrimitiveCategory t : numericTypeList) {
      if (FunctionRegistry.implicitConvertible(pcA, t)
          && FunctionRegistry.implicitConvertible(pcB, t)) {
        return getTypeInfoForPrimitiveCategory((PrimitiveTypeInfo)a, (PrimitiveTypeInfo)b, t);
      }
    }

    return null;
  }

  public static PrimitiveCategory getCommonCategory(TypeInfo a, TypeInfo b) {
    if (a.getCategory() != Category.PRIMITIVE || b.getCategory() != Category.PRIMITIVE) {
      return null;
    }
    PrimitiveCategory pcA = ((PrimitiveTypeInfo)a).getPrimitiveCategory();
    PrimitiveCategory pcB = ((PrimitiveTypeInfo)b).getPrimitiveCategory();

    PrimitiveGrouping pgA = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(pcA);
    PrimitiveGrouping pgB = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(pcB);
    // handle string types properly
    if (pgA == PrimitiveGrouping.STRING_GROUP && pgB == PrimitiveGrouping.STRING_GROUP) {
      return PrimitiveCategory.STRING;
    }

    Integer ai = numericTypes.get(pcA);
    Integer bi = numericTypes.get(pcB);
    if (ai == null || bi == null) {
      // If either is not a numeric type, return null.
      return null;
    }

    return (ai > bi) ? pcA : pcB;
  }

  /**
   * Find a common class that objects of both TypeInfo a and TypeInfo b can
   * convert to. This is used for places other than comparison.
   *
   * The common class of string and double is string.
   *
   * @return null if no common class could be found.
   */
  public static TypeInfo getCommonClass(TypeInfo a, TypeInfo b) {
    if (a.equals(b)) {
      return a;
    }

    PrimitiveCategory commonCat = getCommonCategory(a, b);
    if (commonCat == null)
      return null;
    return getTypeInfoForPrimitiveCategory((PrimitiveTypeInfo)a, (PrimitiveTypeInfo)b, commonCat);
  }

  public static boolean implicitConvertible(PrimitiveCategory from, PrimitiveCategory to) {
    if (from == to) {
      return true;
    }

    PrimitiveGrouping fromPg = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(from);
    PrimitiveGrouping toPg = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(to);

    // Allow implicit String to Double conversion
    if (fromPg == PrimitiveGrouping.STRING_GROUP && to == PrimitiveCategory.DOUBLE) {
      return true;
    }
    // Allow implicit String to Decimal conversion
    if (fromPg == PrimitiveGrouping.STRING_GROUP && to == PrimitiveCategory.DECIMAL) {
      return true;
    }
    // Void can be converted to any type
    if (from == PrimitiveCategory.VOID) {
      return true;
    }

    // Allow implicit String to Date conversion
    if (fromPg == PrimitiveGrouping.DATE_GROUP && toPg == PrimitiveGrouping.STRING_GROUP) {
      return true;
    }
    // Allow implicit Numeric to String conversion
    if (fromPg == PrimitiveGrouping.NUMERIC_GROUP && toPg == PrimitiveGrouping.STRING_GROUP) {
      return true;
    }
    // Allow implicit String to varchar conversion, and vice versa
    if (fromPg == PrimitiveGrouping.STRING_GROUP && toPg == PrimitiveGrouping.STRING_GROUP) {
      return true;
    }

    // Allow implicit conversion from Byte -> Integer -> Long -> Float -> Double
    // Decimal -> String
    Integer f = numericTypes.get(from);
    Integer t = numericTypes.get(to);
    if (f == null || t == null) {
      return false;
    }
    if (f.intValue() > t.intValue()) {
      return false;
    }
    return true;
  }

  /**
   * Returns whether it is possible to implicitly convert an object of Class
   * from to Class to.
   */
  public static boolean implicitConvertible(TypeInfo from, TypeInfo to) {
    if (from.equals(to)) {
      return true;
    }

    // Reimplemented to use PrimitiveCategory rather than TypeInfo, because
    // 2 TypeInfos from the same qualified type (varchar, decimal) should still be
    // seen as equivalent.
    if (from.getCategory() == Category.PRIMITIVE && to.getCategory() == Category.PRIMITIVE) {
      return implicitConvertible(
          ((PrimitiveTypeInfo) from).getPrimitiveCategory(),
          ((PrimitiveTypeInfo) to).getPrimitiveCategory());
    }
    return false;
  }

  /**
   * Get the GenericUDAF evaluator for the name and argumentClasses.
   *
   * @param name
   *          the name of the UDAF
   * @param argumentOIs
   * @param isDistinct
   * @param isAllColumns
   * @return The UDAF evaluator
   */
  @SuppressWarnings("deprecation")
  public static GenericUDAFEvaluator getGenericUDAFEvaluator(String name,
      List<ObjectInspector> argumentOIs, boolean isDistinct,
      boolean isAllColumns) throws SemanticException {

    GenericUDAFResolver udafResolver = getGenericUDAFResolver(name);
    if (udafResolver == null) {
      return null;
    }

    ObjectInspector args[] = new ObjectInspector[argumentOIs.size()];
    // Can't use toArray here because Java is dumb when it comes to
    // generics + arrays.
    for (int ii = 0; ii < argumentOIs.size(); ++ii) {
      args[ii] = argumentOIs.get(ii);
    }

    GenericUDAFParameterInfo paramInfo =
        new SimpleGenericUDAFParameterInfo(
            args, isDistinct, isAllColumns);
    
    GenericUDAFEvaluator udafEvaluator;
    if (udafResolver instanceof GenericUDAFResolver2) {
      udafEvaluator =
          ((GenericUDAFResolver2) udafResolver).getEvaluator(paramInfo);
    } else {
      udafEvaluator = udafResolver.getEvaluator(paramInfo.getParameters());
    }
    return udafEvaluator;
  }

  public static GenericUDAFEvaluator getGenericWindowingEvaluator(String name,
      List<ObjectInspector> argumentOIs, boolean isDistinct,
      boolean isAllColumns) throws SemanticException {
    Registry registry = SessionState.getRegistry();
    GenericUDAFEvaluator evaluator = registry == null ? null :
        registry.getGenericWindowingEvaluator(name, argumentOIs, isDistinct, isAllColumns);
    return evaluator != null ? evaluator :
        system.getGenericWindowingEvaluator(name, argumentOIs, isDistinct, isAllColumns);
  }

  /**
   * This method is shared between UDFRegistry and UDAFRegistry. methodName will
   * be "evaluate" for UDFRegistry, and "aggregate"/"evaluate"/"evaluatePartial"
   * for UDAFRegistry.
   * @throws UDFArgumentException
   */
  public static <T> Method getMethodInternal(Class<? extends T> udfClass,
      String methodName, boolean exact, List<TypeInfo> argumentClasses)
      throws UDFArgumentException {

    List<Method> mlist = new ArrayList<Method>();

    for (Method m : udfClass.getMethods()) {
      if (m.getName().equals(methodName)) {
        mlist.add(m);
      }
    }

    return getMethodInternal(udfClass, mlist, exact, argumentClasses);
  }

  public static GenericUDAFResolver getGenericUDAFResolver(String functionName)
      throws SemanticException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Looking up GenericUDAF: " + functionName);
    }
    FunctionInfo finfo = getFunctionInfo(functionName);
    if (finfo == null) {
      return null;
    }
    GenericUDAFResolver result = finfo.getGenericUDAFResolver();
    return result;
  }

  public static Object invoke(Method m, Object thisObject, Object... arguments)
      throws HiveException {
    Object o;
    try {
      o = m.invoke(thisObject, arguments);
    } catch (Exception e) {
      String thisObjectString = "" + thisObject + " of class "
          + (thisObject == null ? "null" : thisObject.getClass().getName());

      StringBuilder argumentString = new StringBuilder();
      if (arguments == null) {
        argumentString.append("null");
      } else {
        argumentString.append("{");
        for (int i = 0; i < arguments.length; i++) {
          if (i > 0) {
            argumentString.append(", ");
          }
          if (arguments[i] == null) {
            argumentString.append("null");
          } else {
            argumentString.append("" + arguments[i] + ":"
                + arguments[i].getClass().getName());
          }
        }
        argumentString.append("} of size " + arguments.length);
      }

      throw new HiveException("Unable to execute method " + m + " "
          + " on object " + thisObjectString + " with arguments "
          + argumentString.toString(), e);
    }
    return o;
  }

  /**
   * Returns -1 if passed does not match accepted. Otherwise return the cost
   * (usually 0 for no conversion and 1 for conversion).
   */
  public static int matchCost(TypeInfo argumentPassed,
      TypeInfo argumentAccepted, boolean exact) {
    if (argumentAccepted.equals(argumentPassed)
        || TypeInfoUtils.doPrimitiveCategoriesMatch(argumentPassed, argumentAccepted)) {
      // matches
      return 0;
    }
    if (argumentPassed.equals(TypeInfoFactory.voidTypeInfo)) {
      // passing null matches everything
      return 0;
    }
    if (argumentPassed.getCategory().equals(Category.LIST)
        && argumentAccepted.getCategory().equals(Category.LIST)) {
      // lists are compatible if and only-if the elements are compatible
      TypeInfo argumentPassedElement = ((ListTypeInfo) argumentPassed)
          .getListElementTypeInfo();
      TypeInfo argumentAcceptedElement = ((ListTypeInfo) argumentAccepted)
          .getListElementTypeInfo();
      return matchCost(argumentPassedElement, argumentAcceptedElement, exact);
    }
    if (argumentPassed.getCategory().equals(Category.MAP)
        && argumentAccepted.getCategory().equals(Category.MAP)) {
      // lists are compatible if and only-if the elements are compatible
      TypeInfo argumentPassedKey = ((MapTypeInfo) argumentPassed)
          .getMapKeyTypeInfo();
      TypeInfo argumentAcceptedKey = ((MapTypeInfo) argumentAccepted)
          .getMapKeyTypeInfo();
      TypeInfo argumentPassedValue = ((MapTypeInfo) argumentPassed)
          .getMapValueTypeInfo();
      TypeInfo argumentAcceptedValue = ((MapTypeInfo) argumentAccepted)
          .getMapValueTypeInfo();
      int cost1 = matchCost(argumentPassedKey, argumentAcceptedKey, exact);
      int cost2 = matchCost(argumentPassedValue, argumentAcceptedValue, exact);
      if (cost1 < 0 || cost2 < 0) {
        return -1;
      }
      return Math.max(cost1, cost2);
    }

    if (argumentAccepted.equals(TypeInfoFactory.unknownTypeInfo)) {
      // accepting Object means accepting everything,
      // but there is a conversion cost.
      return 1;
    }
    if (!exact && implicitConvertible(argumentPassed, argumentAccepted)) {
      return 1;
    }

    return -1;
  }

  /**
   * Given a set of candidate methods and list of argument types, try to
   * select the best candidate based on how close the passed argument types are
   * to the candidate argument types.
   * For a varchar argument, we would prefer evaluate(string) over evaluate(double).
   * @param udfMethods  list of candidate methods
   * @param argumentsPassed list of argument types to match to the candidate methods
   */
  static void filterMethodsByTypeAffinity(List<Method> udfMethods, List<TypeInfo> argumentsPassed) {
    if (udfMethods.size() > 1) {
      // Prefer methods with a closer signature based on the primitive grouping of each argument.
      // Score each method based on its similarity to the passed argument types.
      int currentScore = 0;
      int bestMatchScore = 0;
      Method bestMatch = null;
      for (Method m: udfMethods) {
        currentScore = 0;
        List<TypeInfo> argumentsAccepted =
            TypeInfoUtils.getParameterTypeInfos(m, argumentsPassed.size());
        Iterator<TypeInfo> argsPassedIter = argumentsPassed.iterator();
        for (TypeInfo acceptedType : argumentsAccepted) {
          // Check the affinity of the argument passed in with the accepted argument,
          // based on the PrimitiveGrouping
          TypeInfo passedType = argsPassedIter.next();
          if (acceptedType.getCategory() == Category.PRIMITIVE
              && passedType.getCategory() == Category.PRIMITIVE) {
            PrimitiveGrouping acceptedPg = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
                ((PrimitiveTypeInfo) acceptedType).getPrimitiveCategory());
            PrimitiveGrouping passedPg = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
                ((PrimitiveTypeInfo) passedType).getPrimitiveCategory());
            if (acceptedPg == passedPg) {
              // The passed argument matches somewhat closely with an accepted argument
              ++currentScore;
            }
          }
        }
        // Check if the score for this method is any better relative to others
        if (currentScore > bestMatchScore) {
          bestMatchScore = currentScore;
          bestMatch = m;
        } else if (currentScore == bestMatchScore) {
          bestMatch = null; // no longer a best match if more than one.
        }
      }

      if (bestMatch != null) {
        // Found a best match during this processing, use it.
        udfMethods.clear();
        udfMethods.add(bestMatch);
      }
    }
  }

  /**
   * Gets the closest matching method corresponding to the argument list from a
   * list of methods.
   *
   * @param mlist
   *          The list of methods to inspect.
   * @param exact
   *          Boolean to indicate whether this is an exact match or not.
   * @param argumentsPassed
   *          The classes for the argument.
   * @return The matching method.
   */
  public static Method getMethodInternal(Class<?> udfClass, List<Method> mlist, boolean exact,
      List<TypeInfo> argumentsPassed) throws UDFArgumentException {

    // result
    List<Method> udfMethods = new ArrayList<Method>();
    // The cost of the result
    int leastConversionCost = Integer.MAX_VALUE;

    for (Method m : mlist) {
      List<TypeInfo> argumentsAccepted = TypeInfoUtils.getParameterTypeInfos(m,
          argumentsPassed.size());
      if (argumentsAccepted == null) {
        // null means the method does not accept number of arguments passed.
        continue;
      }

      boolean match = (argumentsAccepted.size() == argumentsPassed.size());
      int conversionCost = 0;

      for (int i = 0; i < argumentsPassed.size() && match; i++) {
        int cost = matchCost(argumentsPassed.get(i), argumentsAccepted.get(i),
            exact);
        if (cost == -1) {
          match = false;
        } else {
          conversionCost += cost;
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Method " + (match ? "did" : "didn't") + " match: passed = "
                  + argumentsPassed + " accepted = " + argumentsAccepted +
                  " method = " + m);
      }
      if (match) {
        // Always choose the function with least implicit conversions.
        if (conversionCost < leastConversionCost) {
          udfMethods.clear();
          udfMethods.add(m);
          leastConversionCost = conversionCost;
          // Found an exact match
          if (leastConversionCost == 0) {
            break;
          }
        } else if (conversionCost == leastConversionCost) {
          // Ambiguous call: two methods with the same number of implicit
          // conversions
          udfMethods.add(m);
          // Don't break! We might find a better match later.
        } else {
          // do nothing if implicitConversions > leastImplicitConversions
        }
      }
    }

    if (udfMethods.size() == 0) {
      // No matching methods found
      throw new NoMatchingMethodException(udfClass, argumentsPassed, mlist);
    }

    if (udfMethods.size() > 1) {
      // First try selecting methods based on the type affinity of the arguments passed
      // to the candidate method arguments.
      filterMethodsByTypeAffinity(udfMethods, argumentsPassed);
    }

    if (udfMethods.size() > 1) {

      // if the only difference is numeric types, pick the method
      // with the smallest overall numeric type.
      int lowestNumericType = Integer.MAX_VALUE;
      boolean multiple = true;
      Method candidate = null;
      List<TypeInfo> referenceArguments = null;

      for (Method m: udfMethods) {
        int maxNumericType = 0;

        List<TypeInfo> argumentsAccepted = TypeInfoUtils.getParameterTypeInfos(m, argumentsPassed.size());

        if (referenceArguments == null) {
          // keep the arguments for reference - we want all the non-numeric
          // arguments to be the same
          referenceArguments = argumentsAccepted;
        }

        Iterator<TypeInfo> referenceIterator = referenceArguments.iterator();

        for (TypeInfo accepted: argumentsAccepted) {
          TypeInfo reference = referenceIterator.next();

          boolean acceptedIsPrimitive = false;
          PrimitiveCategory acceptedPrimCat = PrimitiveCategory.UNKNOWN;
          if (accepted.getCategory() == Category.PRIMITIVE) {
            acceptedIsPrimitive = true;
            acceptedPrimCat = ((PrimitiveTypeInfo) accepted).getPrimitiveCategory();
          }
          if (acceptedIsPrimitive && numericTypes.containsKey(acceptedPrimCat)) {
            // We're looking for the udf with the smallest maximum numeric type.
            int typeValue = numericTypes.get(acceptedPrimCat);
            maxNumericType = typeValue > maxNumericType ? typeValue : maxNumericType;
          } else if (!accepted.equals(reference)) {
            // There are non-numeric arguments that don't match from one UDF to
            // another. We give up at this point.
            throw new AmbiguousMethodException(udfClass, argumentsPassed, mlist);
          }
        }

        if (lowestNumericType > maxNumericType) {
          multiple = false;
          lowestNumericType = maxNumericType;
          candidate = m;
        } else if (maxNumericType == lowestNumericType) {
          // multiple udfs with the same max type. Unless we find a lower one
          // we'll give up.
          multiple = true;
        }
      }

      if (!multiple) {
        return candidate;
      } else {
        throw new AmbiguousMethodException(udfClass, argumentsPassed, mlist);
      }
    }
    return udfMethods.get(0);
  }

  /**
   * A shortcut to get the "index" GenericUDF. This is used for getting elements
   * out of array and getting values out of map.
   * @throws SemanticException
   */
  public static GenericUDF getGenericUDFForIndex() {
    try {
      return FunctionRegistry.getFunctionInfo("index").getGenericUDF();
    } catch (SemanticException e) {
      throw new RuntimeException("hive operator -- never be thrown", e);
    }
  }

  /**
   * A shortcut to get the "and" GenericUDF.
   * @throws SemanticException
   */
  public static GenericUDF getGenericUDFForAnd() {
    try {
      return FunctionRegistry.getFunctionInfo("and").getGenericUDF();
    } catch (SemanticException e) {
      throw new RuntimeException("hive operator -- never be thrown", e);
    }
  }

  /**
   * Create a copy of an existing GenericUDF.
   */
  public static GenericUDF cloneGenericUDF(GenericUDF genericUDF) {
    if (null == genericUDF) {
      return null;
    }

    GenericUDF clonedUDF;
    if (genericUDF instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge) genericUDF;
      clonedUDF = new GenericUDFBridge(bridge.getUdfName(), bridge.isOperator(),
          bridge.getUdfClassName());
    } else if (genericUDF instanceof GenericUDFMacro) {
      GenericUDFMacro bridge = (GenericUDFMacro) genericUDF;
      clonedUDF = new GenericUDFMacro(bridge.getMacroName(), bridge.getBody(),
          bridge.getColNames(), bridge.getColTypes());
    } else {
      clonedUDF = ReflectionUtils.newInstance(genericUDF.getClass(), null);
    }

    if (clonedUDF != null) {
      // Copy info that may be required in the new copy.
      // The SettableUDF calls below could be replaced using this mechanism as well.
      try {
        genericUDF.copyToNewInstance(clonedUDF);
      } catch (UDFArgumentException err) {
        throw new IllegalArgumentException(err);
      }

      // The original may have settable info that needs to be added to the new copy.
      if (genericUDF instanceof SettableUDF) {
        try {
          TypeInfo typeInfo = ((SettableUDF)genericUDF).getTypeInfo();
          if (typeInfo != null) {
            ((SettableUDF)clonedUDF).setTypeInfo(typeInfo);
          }
        } catch (UDFArgumentException err) {
          // In theory this should not happen - if the original copy of the UDF had this
          // data, we should be able to set the UDF copy with this same settableData.
          LOG.error("Unable to add settable data to UDF " + genericUDF.getClass());
          throw new IllegalArgumentException(err);
        }
      }
    }

    return clonedUDF;
  }

  /**
   * Create a copy of an existing GenericUDTF.
   */
  public static GenericUDTF cloneGenericUDTF(GenericUDTF genericUDTF) {
    if (null == genericUDTF) {
      return null;
    }
    return ReflectionUtils.newInstance(genericUDTF.getClass(), null);
  }

  /**
   * Get the UDF class from an exprNodeDesc. Returns null if the exprNodeDesc
   * does not contain a UDF class.
   */
  private static Class getUDFClassFromExprDesc(ExprNodeDesc desc) {
    if (!(desc instanceof ExprNodeGenericFuncDesc)) {
      return null;
    }
    ExprNodeGenericFuncDesc genericFuncDesc = (ExprNodeGenericFuncDesc) desc;
    GenericUDF genericUDF = genericFuncDesc.getGenericUDF();
    if (genericUDF instanceof GenericUDFBridge) {
      return ((GenericUDFBridge) genericUDF).getUdfClass();
    }
    return genericUDF.getClass();
  }

  /**
   * Returns whether a GenericUDF is deterministic or not.
   */
  public static boolean isDeterministic(GenericUDF genericUDF) {
    if (isStateful(genericUDF)) {
      // stateful implies non-deterministic, regardless of whatever
      // the deterministic annotation declares
      return false;
    }
    UDFType genericUDFType = AnnotationUtils.getAnnotation(genericUDF.getClass(), UDFType.class);
    if (genericUDFType != null && genericUDFType.deterministic() == false) {
      return false;
    }

    if (genericUDF instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge) (genericUDF);
      UDFType bridgeUDFType = AnnotationUtils.getAnnotation(bridge.getUdfClass(), UDFType.class);
      if (bridgeUDFType != null && bridgeUDFType.deterministic() == false) {
        return false;
      }
    }

    if (genericUDF instanceof GenericUDFMacro) {
      GenericUDFMacro macro = (GenericUDFMacro) (genericUDF);
      return macro.isDeterministic();
    }

    return true;
  }

  /**
   * Returns whether a GenericUDF is stateful or not.
   */
  public static boolean isStateful(GenericUDF genericUDF) {
    UDFType genericUDFType = AnnotationUtils.getAnnotation(genericUDF.getClass(), UDFType.class);
    if (genericUDFType != null && genericUDFType.stateful()) {
      return true;
    }

    if (genericUDF instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge) genericUDF;
      UDFType bridgeUDFType = AnnotationUtils.getAnnotation(bridge.getUdfClass(), UDFType.class);
      if (bridgeUDFType != null && bridgeUDFType.stateful()) {
        return true;
      }
    }

    if (genericUDF instanceof GenericUDFMacro) {
      GenericUDFMacro macro = (GenericUDFMacro) (genericUDF);
      return macro.isStateful();
    }

    return false;
  }

  /**
   * Returns whether the exprNodeDesc is a node of "and", "or", "not".
   */
  public static boolean isOpAndOrNot(ExprNodeDesc desc) {
    Class genericUdfClass = getUDFClassFromExprDesc(desc);
    return GenericUDFOPAnd.class == genericUdfClass
        || GenericUDFOPOr.class == genericUdfClass
        || GenericUDFOPNot.class == genericUdfClass;
  }

  /**
   * Returns whether the exprNodeDesc is a node of "and".
   */
  public static boolean isOpAnd(ExprNodeDesc desc) {
    return GenericUDFOPAnd.class == getUDFClassFromExprDesc(desc);
  }

  /**
   * Returns whether the exprNodeDesc is a node of "or".
   */
  public static boolean isOpOr(ExprNodeDesc desc) {
    return GenericUDFOPOr.class == getUDFClassFromExprDesc(desc);
  }

  /**
   * Returns whether the exprNodeDesc is a node of "not".
   */
  public static boolean isOpNot(ExprNodeDesc desc) {
    return GenericUDFOPNot.class == getUDFClassFromExprDesc(desc);
  }

  /**
   * Returns whether the exprNodeDesc is a node of "positive".
   */
  public static boolean isOpPositive(ExprNodeDesc desc) {
    return GenericUDFOPPositive.class == getUDFClassFromExprDesc(desc);
  }

  /**
   * Returns whether the exprNodeDesc is node of "cast".
   */
  public static boolean isOpCast(ExprNodeDesc desc) {
    if (!(desc instanceof ExprNodeGenericFuncDesc)) {
      return false;
    }
    return isOpCast(((ExprNodeGenericFuncDesc)desc).getGenericUDF());
  }

  public static boolean isOpCast(GenericUDF genericUDF) {
    Class udfClass = (genericUDF instanceof GenericUDFBridge) ?
        ((GenericUDFBridge)genericUDF).getUdfClass() : genericUDF.getClass();

    return udfClass == UDFToBoolean.class || udfClass == UDFToByte.class ||
        udfClass == UDFToDouble.class || udfClass == UDFToFloat.class ||
        udfClass == UDFToInteger.class || udfClass == UDFToLong.class ||
        udfClass == UDFToShort.class || udfClass == UDFToString.class ||
        udfClass == GenericUDFToVarchar.class || udfClass == GenericUDFToChar.class ||
        udfClass == GenericUDFTimestamp.class || udfClass == GenericUDFToBinary.class ||
        udfClass == GenericUDFToDate.class  || udfClass == GenericUDFToDecimal.class;
  }

  /**
   * Returns whether the exprNodeDesc can recommend name for the expression
   */
  public static boolean isOpPreserveInputName(ExprNodeDesc desc) {
    return isOpCast(desc);
  }

  /**
   * Registers the appropriate kind of temporary function based on a class's
   * type.
   *
   * @param functionName name under which to register function
   *
   * @param udfClass class implementing UD[A|T]F
   *
   * @return true if udfClass's type was recognized (so registration
   * succeeded); false otherwise
   */
  public static FunctionInfo registerTemporaryUDF(
      String functionName, Class<?> udfClass, FunctionResource... resources) {
    return SessionState.getRegistryForWrite().registerFunction(
        functionName, udfClass, resources);
  }

  public static void unregisterTemporaryUDF(String functionName) throws HiveException {
    if (SessionState.getRegistry() != null) {
      SessionState.getRegistry().unregisterFunction(functionName);
    }
  }

  /**
   * Registers the appropriate kind of temporary function based on a class's
   * type.
   *
   * @param macroName name under which to register the macro
   *
   * @param body the expression which the macro evaluates to
   *@param colNames the names of the arguments to the macro
   *@param colTypes the types of the arguments to the macro
   */
  public static void registerTemporaryMacro(
    String macroName, ExprNodeDesc body, List<String> colNames, List<TypeInfo> colTypes) {
    SessionState.getRegistryForWrite().registerMacro(macroName, body, colNames, colTypes);
  }

  public static FunctionInfo registerPermanentFunction(String functionName,
      String className, boolean registerToSession, FunctionResource[] resources) {
    return system.registerPermanentFunction(functionName, className, registerToSession, resources);
  }

  public static void unregisterPermanentFunction(String functionName) throws HiveException {
    system.unregisterFunction(functionName);
    unregisterTemporaryUDF(functionName);
  }

  private FunctionRegistry() {
    // prevent instantiation
  }


  //---------PTF functions------------

  /**
   * Both UDF and UDAF functions can imply order for analytical functions
   *
   * @param functionName
   *          name of function
   * @return true if a GenericUDF or GenericUDAF exists for this name and implyOrder is true, false
   *         otherwise.
   * @throws SemanticException
   */
  public static boolean impliesOrder(String functionName) throws SemanticException {

    FunctionInfo info = getFunctionInfo(functionName);
    if (info != null && info.isGenericUDF()) {
      UDFType type =
          AnnotationUtils.getAnnotation(info.getGenericUDF().getClass(), UDFType.class);
      if (type != null) {
        return type.impliesOrder();
      }
    }
    WindowFunctionInfo windowInfo = getWindowFunctionInfo(functionName);
    if (windowInfo != null) {
      return windowInfo.isImpliesOrder();
    }
    return false;
  }

  public static boolean pivotResult(String functionName) throws SemanticException {
    WindowFunctionInfo windowInfo = getWindowFunctionInfo(functionName);
    if (windowInfo != null) {
      return windowInfo.isPivotResult();
    }
    return false;
  }

  public static boolean isTableFunction(String functionName)
      throws SemanticException {
    FunctionInfo tFInfo = getFunctionInfo(functionName);
    return tFInfo != null && !tFInfo.isInternalTableFunction() && tFInfo.isTableFunction();
  }

  public static TableFunctionResolver getTableFunctionResolver(String functionName)
      throws SemanticException {
    FunctionInfo tfInfo = getFunctionInfo(functionName);
    if (tfInfo != null && tfInfo.isTableFunction()) {
      return (TableFunctionResolver) ReflectionUtils.newInstance(tfInfo.getFunctionClass(), null);
    }
    return null;
  }

  public static TableFunctionResolver getWindowingTableFunction()
      throws SemanticException {
    return getTableFunctionResolver(WINDOWING_TABLE_FUNCTION);
  }

  public static boolean isNoopFunction(String fnName) {
    fnName = fnName.toLowerCase();
    return fnName.equals(NOOP_MAP_TABLE_FUNCTION) ||
        fnName.equals(NOOP_STREAMING_MAP_TABLE_FUNCTION) ||
        fnName.equals(NOOP_TABLE_FUNCTION) ||
        fnName.equals(NOOP_STREAMING_TABLE_FUNCTION);
  }

  /**
   * Use this to check if function is ranking function
   *
   * @param name
   *          name of a function
   * @return true if function is a UDAF, has WindowFunctionDescription annotation and the annotations
   *         confirms a ranking function, false otherwise
   * @throws SemanticException
   */
  public static boolean isRankingFunction(String name) throws SemanticException {
    FunctionInfo info = getFunctionInfo(name);
    if (info == null) {
      return false;
    }
    GenericUDAFResolver res = info.getGenericUDAFResolver();
    if (res == null) {
      return false;
    }
    WindowFunctionDescription desc =
        AnnotationUtils.getAnnotation(res.getClass(), WindowFunctionDescription.class);
    return (desc != null) && desc.rankingFunction();
  }

  /**
   * @param fnExpr Function expression.
   * @return True iff the fnExpr represents a hive built-in function (native, non-permanent)
   */
  public static boolean isBuiltInFuncExpr(ExprNodeGenericFuncDesc fnExpr) {
    Class<?> udfClass = FunctionRegistry.getUDFClassFromExprDesc(fnExpr);
    if (udfClass != null) {
      return system.isBuiltInFunc(udfClass);
    }
    return false;
  }

  /**
   * Setup blocked flag for all builtin UDFs as per udf whitelist and blacklist
   * @param whiteList
   * @param blackList
   */
  public static void setupPermissionsForBuiltinUDFs(String whiteListStr,
      String blackListStr) {
    system.setupPermissionsForUDFs(whiteListStr, blackListStr);
  }
}
