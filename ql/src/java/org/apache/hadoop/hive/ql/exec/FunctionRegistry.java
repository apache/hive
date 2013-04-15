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

import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.UDAFPercentile;
import org.apache.hadoop.hive.ql.udf.UDFAbs;
import org.apache.hadoop.hive.ql.udf.UDFAcos;
import org.apache.hadoop.hive.ql.udf.UDFAscii;
import org.apache.hadoop.hive.ql.udf.UDFAsin;
import org.apache.hadoop.hive.ql.udf.UDFAtan;
import org.apache.hadoop.hive.ql.udf.UDFBin;
import org.apache.hadoop.hive.ql.udf.UDFCeil;
import org.apache.hadoop.hive.ql.udf.UDFConcat;
import org.apache.hadoop.hive.ql.udf.UDFConv;
import org.apache.hadoop.hive.ql.udf.UDFCos;
import org.apache.hadoop.hive.ql.udf.UDFDate;
import org.apache.hadoop.hive.ql.udf.UDFDateAdd;
import org.apache.hadoop.hive.ql.udf.UDFDateDiff;
import org.apache.hadoop.hive.ql.udf.UDFDateSub;
import org.apache.hadoop.hive.ql.udf.UDFDayOfMonth;
import org.apache.hadoop.hive.ql.udf.UDFDegrees;
import org.apache.hadoop.hive.ql.udf.UDFE;
import org.apache.hadoop.hive.ql.udf.UDFExp;
import org.apache.hadoop.hive.ql.udf.UDFFindInSet;
import org.apache.hadoop.hive.ql.udf.UDFFloor;
import org.apache.hadoop.hive.ql.udf.UDFFromUnixTime;
import org.apache.hadoop.hive.ql.udf.UDFHex;
import org.apache.hadoop.hive.ql.udf.UDFHour;
import org.apache.hadoop.hive.ql.udf.UDFJson;
import org.apache.hadoop.hive.ql.udf.UDFLTrim;
import org.apache.hadoop.hive.ql.udf.UDFLength;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.UDFLn;
import org.apache.hadoop.hive.ql.udf.UDFLog;
import org.apache.hadoop.hive.ql.udf.UDFLog10;
import org.apache.hadoop.hive.ql.udf.UDFLog2;
import org.apache.hadoop.hive.ql.udf.UDFLower;
import org.apache.hadoop.hive.ql.udf.UDFLpad;
import org.apache.hadoop.hive.ql.udf.UDFMinute;
import org.apache.hadoop.hive.ql.udf.UDFMonth;
import org.apache.hadoop.hive.ql.udf.UDFOPBitAnd;
import org.apache.hadoop.hive.ql.udf.UDFOPBitNot;
import org.apache.hadoop.hive.ql.udf.UDFOPBitOr;
import org.apache.hadoop.hive.ql.udf.UDFOPBitXor;
import org.apache.hadoop.hive.ql.udf.UDFOPDivide;
import org.apache.hadoop.hive.ql.udf.UDFOPLongDivide;
import org.apache.hadoop.hive.ql.udf.UDFOPMinus;
import org.apache.hadoop.hive.ql.udf.UDFOPMod;
import org.apache.hadoop.hive.ql.udf.UDFOPMultiply;
import org.apache.hadoop.hive.ql.udf.UDFOPNegative;
import org.apache.hadoop.hive.ql.udf.UDFOPPlus;
import org.apache.hadoop.hive.ql.udf.UDFOPPositive;
import org.apache.hadoop.hive.ql.udf.UDFPI;
import org.apache.hadoop.hive.ql.udf.UDFParseUrl;
import org.apache.hadoop.hive.ql.udf.UDFPosMod;
import org.apache.hadoop.hive.ql.udf.UDFPower;
import org.apache.hadoop.hive.ql.udf.UDFRTrim;
import org.apache.hadoop.hive.ql.udf.UDFRadians;
import org.apache.hadoop.hive.ql.udf.UDFRand;
import org.apache.hadoop.hive.ql.udf.UDFRegExp;
import org.apache.hadoop.hive.ql.udf.UDFRegExpExtract;
import org.apache.hadoop.hive.ql.udf.UDFRegExpReplace;
import org.apache.hadoop.hive.ql.udf.UDFRepeat;
import org.apache.hadoop.hive.ql.udf.UDFReverse;
import org.apache.hadoop.hive.ql.udf.UDFRound;
import org.apache.hadoop.hive.ql.udf.UDFRpad;
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
import org.apache.hadoop.hive.ql.udf.UDFTrim;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.UDFUnhex;
import org.apache.hadoop.hive.ql.udf.UDFUpper;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.apache.hadoop.hive.ql.udf.UDFYear;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLeadLag.GenericUDFLag;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLeadLag.GenericUDFLead;
import org.apache.hadoop.hive.ql.udf.ptf.NPath.NPathResolver;
import org.apache.hadoop.hive.ql.udf.ptf.Noop.NoopResolver;
import org.apache.hadoop.hive.ql.udf.ptf.NoopWithMap.NoopWithMapResolver;
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
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


/**
 * FunctionRegistry.
 */
public final class FunctionRegistry {

  private static Log LOG = LogFactory.getLog(FunctionRegistry.class);

  /**
   * The mapping from expression function names to expression classes.
   */
  static Map<String, FunctionInfo> mFunctions = Collections.synchronizedMap(new LinkedHashMap<String, FunctionInfo>());

  /*
   * PTF variables
   * */

  public static final String LEAD_FUNC_NAME = "lead";
  public static final String LAG_FUNC_NAME = "lag";
  public static final String LAST_VALUE_FUNC_NAME = "last_value";


  public static final String WINDOWING_TABLE_FUNCTION = "windowingtablefunction";
  public static final String NOOP_TABLE_FUNCTION = "noop";
  public static final String NOOP_MAP_TABLE_FUNCTION = "noopwithmap";

  static Map<String, PTFFunctionInfo> tableFunctions = Collections.synchronizedMap(new LinkedHashMap<String, PTFFunctionInfo>());
  static Map<String, WindowFunctionInfo> windowFunctions = Collections.synchronizedMap(new LinkedHashMap<String, WindowFunctionInfo>());

  /*
   * UDAFS that only work when the input rows have an order.
   */
  public static final HashSet<String> UDAFS_IMPLY_ORDER = new HashSet<String>();

  static {
    registerUDF("concat", UDFConcat.class, false);
    registerUDF("substr", UDFSubstr.class, false);
    registerUDF("substring", UDFSubstr.class, false);
    registerUDF("space", UDFSpace.class, false);
    registerUDF("repeat", UDFRepeat.class, false);
    registerUDF("ascii", UDFAscii.class, false);
    registerUDF("lpad", UDFLpad.class, false);
    registerUDF("rpad", UDFRpad.class, false);

    registerGenericUDF("size", GenericUDFSize.class);

    registerUDF("round", UDFRound.class, false);
    registerUDF("floor", UDFFloor.class, false);
    registerUDF("sqrt", UDFSqrt.class, false);
    registerUDF("ceil", UDFCeil.class, false);
    registerUDF("ceiling", UDFCeil.class, false);
    registerUDF("rand", UDFRand.class, false);
    registerUDF("abs", UDFAbs.class, false);
    registerUDF("pmod", UDFPosMod.class, false);

    registerUDF("ln", UDFLn.class, false);
    registerUDF("log2", UDFLog2.class, false);
    registerUDF("sin", UDFSin.class, false);
    registerUDF("asin", UDFAsin.class, false);
    registerUDF("cos", UDFCos.class, false);
    registerUDF("acos", UDFAcos.class, false);
    registerUDF("log10", UDFLog10.class, false);
    registerUDF("log", UDFLog.class, false);
    registerUDF("exp", UDFExp.class, false);
    registerUDF("power", UDFPower.class, false);
    registerUDF("pow", UDFPower.class, false);
    registerUDF("sign", UDFSign.class, false);
    registerUDF("pi", UDFPI.class, false);
    registerUDF("degrees", UDFDegrees.class, false);
    registerUDF("radians", UDFRadians.class, false);
    registerUDF("atan", UDFAtan.class, false);
    registerUDF("tan", UDFTan.class, false);
    registerUDF("e", UDFE.class, false);

    registerUDF("conv", UDFConv.class, false);
    registerUDF("bin", UDFBin.class, false);
    registerUDF("hex", UDFHex.class, false);
    registerUDF("unhex", UDFUnhex.class, false);

    registerUDF("upper", UDFUpper.class, false);
    registerUDF("lower", UDFLower.class, false);
    registerUDF("ucase", UDFUpper.class, false);
    registerUDF("lcase", UDFLower.class, false);
    registerUDF("trim", UDFTrim.class, false);
    registerUDF("ltrim", UDFLTrim.class, false);
    registerUDF("rtrim", UDFRTrim.class, false);
    registerUDF("length", UDFLength.class, false);
    registerUDF("reverse", UDFReverse.class, false);
    registerGenericUDF("field", GenericUDFField.class);
    registerUDF("find_in_set", UDFFindInSet.class, false);

    registerUDF("like", UDFLike.class, true);
    registerUDF("rlike", UDFRegExp.class, true);
    registerUDF("regexp", UDFRegExp.class, true);
    registerUDF("regexp_replace", UDFRegExpReplace.class, false);
    registerUDF("regexp_extract", UDFRegExpExtract.class, false);
    registerUDF("parse_url", UDFParseUrl.class, false);
    registerGenericUDF("nvl", GenericUDFNvl.class);
    registerGenericUDF("split", GenericUDFSplit.class);
    registerGenericUDF("str_to_map", GenericUDFStringToMap.class);
    registerGenericUDF("translate", GenericUDFTranslate.class);

    registerUDF("positive", UDFOPPositive.class, true, "+");
    registerUDF("negative", UDFOPNegative.class, true, "-");

    registerUDF("day", UDFDayOfMonth.class, false);
    registerUDF("dayofmonth", UDFDayOfMonth.class, false);
    registerUDF("month", UDFMonth.class, false);
    registerUDF("year", UDFYear.class, false);
    registerUDF("hour", UDFHour.class, false);
    registerUDF("minute", UDFMinute.class, false);
    registerUDF("second", UDFSecond.class, false);
    registerUDF("from_unixtime", UDFFromUnixTime.class, false);
    registerUDF("to_date", UDFDate.class, false);
    registerUDF("weekofyear", UDFWeekOfYear.class, false);

    registerUDF("date_add", UDFDateAdd.class, false);
    registerUDF("date_sub", UDFDateSub.class, false);
    registerUDF("datediff", UDFDateDiff.class, false);

    registerUDF("get_json_object", UDFJson.class, false);

    registerUDF("xpath_string", UDFXPathString.class, false);
    registerUDF("xpath_boolean", UDFXPathBoolean.class, false);
    registerUDF("xpath_number", UDFXPathDouble.class, false);
    registerUDF("xpath_double", UDFXPathDouble.class, false);
    registerUDF("xpath_float", UDFXPathFloat.class, false);
    registerUDF("xpath_long", UDFXPathLong.class, false);
    registerUDF("xpath_int", UDFXPathInteger.class, false);
    registerUDF("xpath_short", UDFXPathShort.class, false);
    registerGenericUDF("xpath", GenericUDFXPath.class);

    registerUDF("+", UDFOPPlus.class, true);
    registerUDF("-", UDFOPMinus.class, true);
    registerUDF("*", UDFOPMultiply.class, true);
    registerUDF("/", UDFOPDivide.class, true);
    registerUDF("%", UDFOPMod.class, true);
    registerUDF("div", UDFOPLongDivide.class, true);

    registerUDF("&", UDFOPBitAnd.class, true);
    registerUDF("|", UDFOPBitOr.class, true);
    registerUDF("^", UDFOPBitXor.class, true);
    registerUDF("~", UDFOPBitNot.class, true);

    registerGenericUDF("isnull", GenericUDFOPNull.class);
    registerGenericUDF("isnotnull", GenericUDFOPNotNull.class);

    registerGenericUDF("if", GenericUDFIf.class);
    registerGenericUDF("in", GenericUDFIn.class);
    registerGenericUDF("and", GenericUDFOPAnd.class);
    registerGenericUDF("or", GenericUDFOPOr.class);
    registerGenericUDF("=", GenericUDFOPEqual.class);
    registerGenericUDF("==", GenericUDFOPEqual.class);
    registerGenericUDF("<=>", GenericUDFOPEqualNS.class);
    registerGenericUDF("!=", GenericUDFOPNotEqual.class);
    registerGenericUDF("<>", GenericUDFOPNotEqual.class);
    registerGenericUDF("<", GenericUDFOPLessThan.class);
    registerGenericUDF("<=", GenericUDFOPEqualOrLessThan.class);
    registerGenericUDF(">", GenericUDFOPGreaterThan.class);
    registerGenericUDF(">=", GenericUDFOPEqualOrGreaterThan.class);
    registerGenericUDF("not", GenericUDFOPNot.class);
    registerGenericUDF("!", GenericUDFOPNot.class);
    registerGenericUDF("between", GenericUDFBetween.class);

    registerGenericUDF("ewah_bitmap_and", GenericUDFEWAHBitmapAnd.class);
    registerGenericUDF("ewah_bitmap_or", GenericUDFEWAHBitmapOr.class);
    registerGenericUDF("ewah_bitmap_empty", GenericUDFEWAHBitmapEmpty.class);


    // Aliases for Java Class Names
    // These are used in getImplicitConvertUDFMethod
    registerUDF(serdeConstants.BOOLEAN_TYPE_NAME, UDFToBoolean.class, false,
        UDFToBoolean.class.getSimpleName());
    registerUDF(serdeConstants.TINYINT_TYPE_NAME, UDFToByte.class, false,
        UDFToByte.class.getSimpleName());
    registerUDF(serdeConstants.SMALLINT_TYPE_NAME, UDFToShort.class, false,
        UDFToShort.class.getSimpleName());
    registerUDF(serdeConstants.INT_TYPE_NAME, UDFToInteger.class, false,
        UDFToInteger.class.getSimpleName());
    registerUDF(serdeConstants.BIGINT_TYPE_NAME, UDFToLong.class, false,
        UDFToLong.class.getSimpleName());
    registerUDF(serdeConstants.FLOAT_TYPE_NAME, UDFToFloat.class, false,
        UDFToFloat.class.getSimpleName());
    registerUDF(serdeConstants.DOUBLE_TYPE_NAME, UDFToDouble.class, false,
        UDFToDouble.class.getSimpleName());
    registerUDF(serdeConstants.STRING_TYPE_NAME, UDFToString.class, false,
        UDFToString.class.getSimpleName());

    registerGenericUDF(serdeConstants.TIMESTAMP_TYPE_NAME,
        GenericUDFTimestamp.class);
    registerGenericUDF(serdeConstants.BINARY_TYPE_NAME,
        GenericUDFToBinary.class);
    registerGenericUDF(serdeConstants.DECIMAL_TYPE_NAME,
        GenericUDFToDecimal.class);

    // Aggregate functions
    registerGenericUDAF("max", new GenericUDAFMax());
    registerGenericUDAF("min", new GenericUDAFMin());

    registerGenericUDAF("sum", new GenericUDAFSum());
    registerGenericUDAF("count", new GenericUDAFCount());
    registerGenericUDAF("avg", new GenericUDAFAverage());
    registerGenericUDAF("std", new GenericUDAFStd());
    registerGenericUDAF("stddev", new GenericUDAFStd());
    registerGenericUDAF("stddev_pop", new GenericUDAFStd());
    registerGenericUDAF("stddev_samp", new GenericUDAFStdSample());
    registerGenericUDAF("variance", new GenericUDAFVariance());
    registerGenericUDAF("var_pop", new GenericUDAFVariance());
    registerGenericUDAF("var_samp", new GenericUDAFVarianceSample());
    registerGenericUDAF("covar_pop", new GenericUDAFCovariance());
    registerGenericUDAF("covar_samp", new GenericUDAFCovarianceSample());
    registerGenericUDAF("corr", new GenericUDAFCorrelation());
    registerGenericUDAF("histogram_numeric", new GenericUDAFHistogramNumeric());
    registerGenericUDAF("percentile_approx", new GenericUDAFPercentileApprox());
    registerGenericUDAF("collect_set", new GenericUDAFCollectSet());

    registerGenericUDAF("ngrams", new GenericUDAFnGrams());
    registerGenericUDAF("context_ngrams", new GenericUDAFContextNGrams());

    registerGenericUDAF("ewah_bitmap", new GenericUDAFEWAHBitmap());

    registerGenericUDAF("compute_stats" , new GenericUDAFComputeStats());

    registerUDAF("percentile", UDAFPercentile.class);


    // Generic UDFs
    registerGenericUDF("reflect", GenericUDFReflect.class);
    registerGenericUDF("reflect2", GenericUDFReflect2.class);
    registerGenericUDF("java_method", GenericUDFReflect.class);

    registerGenericUDF("array", GenericUDFArray.class);
    registerGenericUDF("assert_true", GenericUDFAssertTrue.class);
    registerGenericUDF("map", GenericUDFMap.class);
    registerGenericUDF("struct", GenericUDFStruct.class);
    registerGenericUDF("named_struct", GenericUDFNamedStruct.class);
    registerGenericUDF("create_union", GenericUDFUnion.class);

    registerGenericUDF("case", GenericUDFCase.class);
    registerGenericUDF("when", GenericUDFWhen.class);
    registerGenericUDF("hash", GenericUDFHash.class);
    registerGenericUDF("coalesce", GenericUDFCoalesce.class);
    registerGenericUDF("index", GenericUDFIndex.class);
    registerGenericUDF("in_file", GenericUDFInFile.class);
    registerGenericUDF("instr", GenericUDFInstr.class);
    registerGenericUDF("locate", GenericUDFLocate.class);
    registerGenericUDF("elt", GenericUDFElt.class);
    registerGenericUDF("concat_ws", GenericUDFConcatWS.class);
    registerGenericUDF("sort_array", GenericUDFSortArray.class);
    registerGenericUDF("array_contains", GenericUDFArrayContains.class);
    registerGenericUDF("sentences", GenericUDFSentences.class);
    registerGenericUDF("map_keys", GenericUDFMapKeys.class);
    registerGenericUDF("map_values", GenericUDFMapValues.class);
    registerGenericUDF("format_number", GenericUDFFormatNumber.class);
    registerGenericUDF("printf", GenericUDFPrintf.class);

    registerGenericUDF("from_utc_timestamp", GenericUDFFromUtcTimestamp.class);
    registerGenericUDF("to_utc_timestamp", GenericUDFToUtcTimestamp.class);

    registerGenericUDF("unix_timestamp", GenericUDFUnixTimeStamp.class);
    registerGenericUDF("to_unix_timestamp", GenericUDFToUnixTimeStamp.class);

    // Generic UDTF's
    registerGenericUDTF("explode", GenericUDTFExplode.class);
    registerGenericUDTF("inline", GenericUDTFInline.class);
    registerGenericUDTF("json_tuple", GenericUDTFJSONTuple.class);
    registerGenericUDTF("parse_url_tuple", GenericUDTFParseUrlTuple.class);
    registerGenericUDTF("stack", GenericUDTFStack.class);

    //PTF declarations
    registerGenericUDF(true, LEAD_FUNC_NAME, GenericUDFLead.class);
    registerGenericUDF(true, LAG_FUNC_NAME, GenericUDFLag.class);

    registerHiveUDAFsAsWindowFunctions();
    registerWindowFunction("row_number", new GenericUDAFRowNumber());
    registerWindowFunction("rank", new GenericUDAFRank());
    registerWindowFunction("dense_rank", new GenericUDAFDenseRank());
    registerWindowFunction("percent_rank", new GenericUDAFPercentRank());
    registerWindowFunction("cume_dist", new GenericUDAFCumeDist());
    registerWindowFunction("ntile", new GenericUDAFNTile());
    registerWindowFunction("first_value", new GenericUDAFFirstValue());
    registerWindowFunction("last_value", new GenericUDAFLastValue());
    registerWindowFunction(LEAD_FUNC_NAME, new GenericUDAFLead(), false);
    registerWindowFunction(LAG_FUNC_NAME, new GenericUDAFLag(), false);

    UDAFS_IMPLY_ORDER.add("rank");
    UDAFS_IMPLY_ORDER.add("dense_rank");
    UDAFS_IMPLY_ORDER.add("percent_rank");
    UDAFS_IMPLY_ORDER.add("cume_dist");
    UDAFS_IMPLY_ORDER.add(LEAD_FUNC_NAME);
    UDAFS_IMPLY_ORDER.add(LAG_FUNC_NAME);
    UDAFS_IMPLY_ORDER.add("first_value");
    UDAFS_IMPLY_ORDER.add("last_value");

    registerTableFunction(NOOP_TABLE_FUNCTION, NoopResolver.class);
    registerTableFunction(NOOP_MAP_TABLE_FUNCTION, NoopWithMapResolver.class);
    registerTableFunction(WINDOWING_TABLE_FUNCTION,  WindowingTableFunctionResolver.class);
    registerTableFunction("npath", NPathResolver.class);
  }

  public static void registerTemporaryUDF(String functionName,
      Class<? extends UDF> UDFClass, boolean isOperator) {
    registerUDF(false, functionName, UDFClass, isOperator);
  }

  static void registerUDF(String functionName, Class<? extends UDF> UDFClass,
      boolean isOperator) {
    registerUDF(true, functionName, UDFClass, isOperator);
  }

  public static void registerUDF(boolean isNative, String functionName,
      Class<? extends UDF> UDFClass, boolean isOperator) {
    registerUDF(isNative, functionName, UDFClass, isOperator, functionName
        .toLowerCase());
  }

  public static void registerUDF(String functionName,
      Class<? extends UDF> UDFClass, boolean isOperator, String displayName) {
    registerUDF(true, functionName, UDFClass, isOperator, displayName);
  }

  public static void registerUDF(boolean isNative, String functionName,
      Class<? extends UDF> UDFClass, boolean isOperator, String displayName) {
    if (UDF.class.isAssignableFrom(UDFClass)) {
      FunctionInfo fI = new FunctionInfo(isNative, displayName,
          new GenericUDFBridge(displayName, isOperator, UDFClass));
      mFunctions.put(functionName.toLowerCase(), fI);
    } else {
      throw new RuntimeException("Registering UDF Class " + UDFClass
          + " which does not extend " + UDF.class);
    }
  }

  public static void registerTemporaryGenericUDF(String functionName,
      Class<? extends GenericUDF> genericUDFClass) {
    registerGenericUDF(false, functionName, genericUDFClass);
  }

  static void registerGenericUDF(String functionName,
      Class<? extends GenericUDF> genericUDFClass) {
    registerGenericUDF(true, functionName, genericUDFClass);
  }

  public static void registerGenericUDF(boolean isNative, String functionName,
      Class<? extends GenericUDF> genericUDFClass) {
    if (GenericUDF.class.isAssignableFrom(genericUDFClass)) {
      FunctionInfo fI = new FunctionInfo(isNative, functionName,
          (GenericUDF) ReflectionUtils.newInstance(genericUDFClass, null));
      mFunctions.put(functionName.toLowerCase(), fI);
    } else {
      throw new RuntimeException("Registering GenericUDF Class "
          + genericUDFClass + " which does not extend " + GenericUDF.class);
    }
  }

  public static void registerTemporaryGenericUDTF(String functionName,
      Class<? extends GenericUDTF> genericUDTFClass) {
    registerGenericUDTF(false, functionName, genericUDTFClass);
  }

  static void registerGenericUDTF(String functionName,
      Class<? extends GenericUDTF> genericUDTFClass) {
    registerGenericUDTF(true, functionName, genericUDTFClass);
  }

  public static void registerGenericUDTF(boolean isNative, String functionName,
      Class<? extends GenericUDTF> genericUDTFClass) {
    if (GenericUDTF.class.isAssignableFrom(genericUDTFClass)) {
      FunctionInfo fI = new FunctionInfo(isNative, functionName,
          (GenericUDTF) ReflectionUtils.newInstance(genericUDTFClass, null));
      mFunctions.put(functionName.toLowerCase(), fI);
    } else {
      throw new RuntimeException("Registering GenericUDTF Class "
          + genericUDTFClass + " which does not extend " + GenericUDTF.class);
    }
  }

  public static FunctionInfo getFunctionInfo(String functionName) {
    return mFunctions.get(functionName.toLowerCase());
  }

  /**
   * Returns a set of registered function names. This is used for the CLI
   * command "SHOW FUNCTIONS;"
   *
   * @return set of strings contains function names
   */
  public static Set<String> getFunctionNames() {
    return mFunctions.keySet();
  }

  /**
   * Returns a set of registered function names. This is used for the CLI
   * command "SHOW FUNCTIONS 'regular expression';" Returns an empty set when
   * the regular expression is not valid.
   *
   * @param funcPatternStr
   *          regular expression of the interested function names
   * @return set of strings contains function names
   */
  public static Set<String> getFunctionNames(String funcPatternStr) {
    Set<String> funcNames = new TreeSet<String>();
    Pattern funcPattern = null;
    try {
      funcPattern = Pattern.compile(funcPatternStr);
    } catch (PatternSyntaxException e) {
      return funcNames;
    }
    for (String funcName : mFunctions.keySet()) {
      if (funcPattern.matcher(funcName).matches()) {
        funcNames.add(funcName);
      }
    }
    return funcNames;
  }

  /**
   * Returns the set of synonyms of the supplied function.
   *
   * @param funcName
   *          the name of the function
   * @return Set of synonyms for funcName
   */
  public static Set<String> getFunctionSynonyms(String funcName) {
    Set<String> synonyms = new HashSet<String>();

    FunctionInfo funcInfo = getFunctionInfo(funcName);
    if (null == funcInfo) {
      return synonyms;
    }

    Class<?> funcClass = funcInfo.getFunctionClass();
    for (String name : mFunctions.keySet()) {
      if (name.equals(funcName)) {
        continue;
      }
      if (mFunctions.get(name).getFunctionClass().equals(funcClass)) {
        synonyms.add(name);
      }
    }

    return synonyms;
  }

  static Map<TypeInfo, Integer> numericTypes = new HashMap<TypeInfo, Integer>();
  static List<TypeInfo> numericTypeList = new ArrayList<TypeInfo>();

  static void registerNumericType(String typeName, int level) {
    TypeInfo t = TypeInfoFactory.getPrimitiveTypeInfo(typeName);
    numericTypeList.add(t);
    numericTypes.put(t, level);
  }

  static {
    registerNumericType(serdeConstants.TINYINT_TYPE_NAME, 1);
    registerNumericType(serdeConstants.SMALLINT_TYPE_NAME, 2);
    registerNumericType(serdeConstants.INT_TYPE_NAME, 3);
    registerNumericType(serdeConstants.BIGINT_TYPE_NAME, 4);
    registerNumericType(serdeConstants.FLOAT_TYPE_NAME, 5);
    registerNumericType(serdeConstants.DOUBLE_TYPE_NAME, 6);
    registerNumericType(serdeConstants.DECIMAL_TYPE_NAME, 7);
    registerNumericType(serdeConstants.STRING_TYPE_NAME, 8);
  }

  /**
   * Find a common class for union-all operator
   */
  public static TypeInfo getCommonClassForUnionAll(TypeInfo a, TypeInfo b) {
    if (a.equals(b)) {
      return a;
    }
    if (FunctionRegistry.implicitConvertable(a, b)) {
      return b;
    }
    if (FunctionRegistry.implicitConvertable(b, a)) {
      return a;
    }
    for (TypeInfo t : numericTypeList) {
      if (FunctionRegistry.implicitConvertable(a, t)
          && FunctionRegistry.implicitConvertable(b, t)) {
        return t;
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
    for (TypeInfo t : numericTypeList) {
      if (FunctionRegistry.implicitConvertable(a, t)
          && FunctionRegistry.implicitConvertable(b, t)) {
        return t;
      }
    }
    return null;
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
    Integer ai = numericTypes.get(a);
    Integer bi = numericTypes.get(b);
    if (ai == null || bi == null) {
      // If either is not a numeric type, return null.
      return null;
    }
    return (ai > bi) ? a : b;
  }

  /**
   * Returns whether it is possible to implicitly convert an object of Class
   * from to Class to.
   */
  public static boolean implicitConvertable(TypeInfo from, TypeInfo to) {
    if (from.equals(to)) {
      return true;
    }
    // Allow implicit String to Double conversion
    if (from.equals(TypeInfoFactory.stringTypeInfo)
        && to.equals(TypeInfoFactory.doubleTypeInfo)) {
      return true;
    }
    // Allow implicit String to Decimal conversion
    if (from.equals(TypeInfoFactory.stringTypeInfo)
        && to.equals(TypeInfoFactory.decimalTypeInfo)) {
      return true;
    }
    // Void can be converted to any type
    if (from.equals(TypeInfoFactory.voidTypeInfo)) {
      return true;
    }

    if (from.equals(TypeInfoFactory.timestampTypeInfo)
        && to.equals(TypeInfoFactory.stringTypeInfo)) {
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

    GenericUDAFEvaluator udafEvaluator = null;
    ObjectInspector args[] = new ObjectInspector[argumentOIs.size()];
    // Can't use toArray here because Java is dumb when it comes to
    // generics + arrays.
    for (int ii = 0; ii < argumentOIs.size(); ++ii) {
      args[ii] = argumentOIs.get(ii);
    }

    GenericUDAFParameterInfo paramInfo =
        new SimpleGenericUDAFParameterInfo(
            args, isDistinct, isAllColumns);
    if (udafResolver instanceof GenericUDAFResolver2) {
      udafEvaluator =
          ((GenericUDAFResolver2) udafResolver).getEvaluator(paramInfo);
    } else {
      udafEvaluator = udafResolver.getEvaluator(paramInfo.getParameters());
    }
    return udafEvaluator;
  }

  @SuppressWarnings("deprecation")
  public static GenericUDAFEvaluator getGenericWindowingEvaluator(String name,
      List<ObjectInspector> argumentOIs, boolean isDistinct,
      boolean isAllColumns) throws SemanticException {

    WindowFunctionInfo finfo = windowFunctions.get(name.toLowerCase());
    if (finfo == null) { return null;}
    if ( !name.toLowerCase().equals(LEAD_FUNC_NAME) &&
    !name.toLowerCase().equals(LAG_FUNC_NAME) ) {
    return getGenericUDAFEvaluator(name, argumentOIs, isDistinct, isAllColumns);
    }

    // this must be lead/lag UDAF
    ObjectInspector args[] = new ObjectInspector[argumentOIs.size()];
    GenericUDAFResolver udafResolver = finfo.getfInfo().getGenericUDAFResolver();
    GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(
    argumentOIs.toArray(args), isDistinct, isAllColumns);
    return ((GenericUDAFResolver2) udafResolver).getEvaluator(paramInfo);
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

  public static void registerTemporaryGenericUDAF(String functionName,
      GenericUDAFResolver genericUDAFResolver) {
    registerGenericUDAF(false, functionName, genericUDAFResolver);
  }

  static void registerGenericUDAF(String functionName,
      GenericUDAFResolver genericUDAFResolver) {
    registerGenericUDAF(true, functionName, genericUDAFResolver);
  }

  public static void registerGenericUDAF(boolean isNative, String functionName,
      GenericUDAFResolver genericUDAFResolver) {
    mFunctions.put(functionName.toLowerCase(), new FunctionInfo(isNative,
        functionName.toLowerCase(), genericUDAFResolver));
  }

  public static void registerTemporaryUDAF(String functionName,
      Class<? extends UDAF> udafClass) {
    registerUDAF(false, functionName, udafClass);
  }

  static void registerUDAF(String functionName, Class<? extends UDAF> udafClass) {
    registerUDAF(true, functionName, udafClass);
  }

  public static void registerUDAF(boolean isNative, String functionName,
      Class<? extends UDAF> udafClass) {
    mFunctions.put(functionName.toLowerCase(), new FunctionInfo(isNative,
        functionName.toLowerCase(), new GenericUDAFBridge(
        (UDAF) ReflectionUtils.newInstance(udafClass, null))));
  }

  public static void unregisterTemporaryUDF(String functionName) throws HiveException {
    FunctionInfo fi = mFunctions.get(functionName.toLowerCase());
    if (fi != null) {
      if (!fi.isNative()) {
        mFunctions.remove(functionName.toLowerCase());
      } else {
        throw new HiveException("Function " + functionName
            + " is hive native, it can't be dropped");
      }
    }
  }

  public static GenericUDAFResolver getGenericUDAFResolver(String functionName) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Looking up GenericUDAF: " + functionName);
    }
    FunctionInfo finfo = mFunctions.get(functionName.toLowerCase());
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
    if (argumentAccepted.equals(argumentPassed)) {
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
    if (!exact && implicitConvertable(argumentPassed, argumentAccepted)) {
      return 1;
    }

    return -1;
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

          if (numericTypes.containsKey(accepted)) {
            // We're looking for the udf with the smallest maximum numeric type.
            int typeValue = numericTypes.get(accepted);
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
   */
  public static GenericUDF getGenericUDFForIndex() {
    return FunctionRegistry.getFunctionInfo("index").getGenericUDF();
  }

  /**
   * A shortcut to get the "and" GenericUDF.
   */
  public static GenericUDF getGenericUDFForAnd() {
    return FunctionRegistry.getFunctionInfo("and").getGenericUDF();
  }

  /**
   * Create a copy of an existing GenericUDF.
   */
  public static GenericUDF cloneGenericUDF(GenericUDF genericUDF) {
    if (null == genericUDF) {
      return null;
    }

    if (genericUDF instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge) genericUDF;
      return new GenericUDFBridge(bridge.getUdfName(), bridge.isOperator(),
          bridge.getUdfClass());
    }

    return (GenericUDF) ReflectionUtils
        .newInstance(genericUDF.getClass(), null);
  }

  /**
   * Create a copy of an existing GenericUDTF.
   */
  public static GenericUDTF cloneGenericUDTF(GenericUDTF genericUDTF) {
    if (null == genericUDTF) {
      return null;
    }
    return (GenericUDTF) ReflectionUtils.newInstance(genericUDTF.getClass(),
        null);
  }

  /**
   * Get the UDF class from an exprNodeDesc. Returns null if the exprNodeDesc
   * does not contain a UDF class.
   */
  private static Class<? extends GenericUDF> getGenericUDFClassFromExprDesc(ExprNodeDesc desc) {
    if (!(desc instanceof ExprNodeGenericFuncDesc)) {
      return null;
    }
    ExprNodeGenericFuncDesc genericFuncDesc = (ExprNodeGenericFuncDesc) desc;
    return genericFuncDesc.getGenericUDF().getClass();
  }

  /**
   * Get the UDF class from an exprNodeDesc. Returns null if the exprNodeDesc
   * does not contain a UDF class.
   */
  private static Class<? extends UDF> getUDFClassFromExprDesc(ExprNodeDesc desc) {
    if (!(desc instanceof ExprNodeGenericFuncDesc)) {
      return null;
    }
    ExprNodeGenericFuncDesc genericFuncDesc = (ExprNodeGenericFuncDesc) desc;
    if (!(genericFuncDesc.getGenericUDF() instanceof GenericUDFBridge)) {
      return null;
    }
    GenericUDFBridge bridge = (GenericUDFBridge) (genericFuncDesc
        .getGenericUDF());
    return bridge.getUdfClass();
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
    UDFType genericUDFType = genericUDF.getClass().getAnnotation(UDFType.class);
    if (genericUDFType != null && genericUDFType.deterministic() == false) {
      return false;
    }

    if (genericUDF instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge) (genericUDF);
      UDFType bridgeUDFType = bridge.getUdfClass().getAnnotation(UDFType.class);
      if (bridgeUDFType != null && bridgeUDFType.deterministic() == false) {
        return false;
      }
    }

    return true;
  }

  /**
   * Returns whether a GenericUDF is stateful or not.
   */
  public static boolean isStateful(GenericUDF genericUDF) {
    UDFType genericUDFType = genericUDF.getClass().getAnnotation(UDFType.class);
    if (genericUDFType != null && genericUDFType.stateful()) {
      return true;
    }

    if (genericUDF instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge) genericUDF;
      UDFType bridgeUDFType = bridge.getUdfClass().getAnnotation(UDFType.class);
      if (bridgeUDFType != null && bridgeUDFType.stateful()) {
        return true;
      }
    }

    return false;
  }

  /**
   * Returns whether the exprNodeDesc is a node of "and", "or", "not".
   */
  public static boolean isOpAndOrNot(ExprNodeDesc desc) {
    Class<? extends GenericUDF> genericUdfClass = getGenericUDFClassFromExprDesc(desc);
    return GenericUDFOPAnd.class == genericUdfClass
        || GenericUDFOPOr.class == genericUdfClass
        || GenericUDFOPNot.class == genericUdfClass;
  }

  /**
   * Returns whether the exprNodeDesc is a node of "and".
   */
  public static boolean isOpAnd(ExprNodeDesc desc) {
    return GenericUDFOPAnd.class == getGenericUDFClassFromExprDesc(desc);
  }

  /**
   * Returns whether the exprNodeDesc is a node of "or".
   */
  public static boolean isOpOr(ExprNodeDesc desc) {
    return GenericUDFOPOr.class == getGenericUDFClassFromExprDesc(desc);
  }

  /**
   * Returns whether the exprNodeDesc is a node of "not".
   */
  public static boolean isOpNot(ExprNodeDesc desc) {
    return GenericUDFOPNot.class == getGenericUDFClassFromExprDesc(desc);
  }

  /**
   * Returns whether the exprNodeDesc is a node of "positive".
   */
  public static boolean isOpPositive(ExprNodeDesc desc) {
    Class<? extends UDF> udfClass = getUDFClassFromExprDesc(desc);
    return UDFOPPositive.class == udfClass;
  }

  /**
   * Returns whether the exprNodeDesc is node of "cast".
   */
  private static boolean isOpCast(ExprNodeDesc desc) {
    if (!(desc instanceof ExprNodeGenericFuncDesc)) {
      return false;
    }
    GenericUDF genericUDF = ((ExprNodeGenericFuncDesc)desc).getGenericUDF();
    Class udfClass;
    if (genericUDF instanceof GenericUDFBridge) {
      udfClass = ((GenericUDFBridge)genericUDF).getUdfClass();
    } else {
      udfClass = genericUDF.getClass();
    }
    return udfClass == UDFToBoolean.class || udfClass == UDFToByte.class ||
        udfClass == UDFToDouble.class || udfClass == UDFToFloat.class ||
        udfClass == UDFToInteger.class || udfClass == UDFToLong.class ||
        udfClass == UDFToShort.class || udfClass == UDFToString.class ||
        udfClass == GenericUDFTimestamp.class || udfClass == GenericUDFToBinary.class;
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
  public static boolean registerTemporaryFunction(
    String functionName, Class<?> udfClass) {

    if (UDF.class.isAssignableFrom(udfClass)) {
      FunctionRegistry.registerTemporaryUDF(
        functionName, (Class<? extends UDF>) udfClass, false);
    } else if (GenericUDF.class.isAssignableFrom(udfClass)) {
      FunctionRegistry.registerTemporaryGenericUDF(
        functionName, (Class<? extends GenericUDF>) udfClass);
    } else if (GenericUDTF.class.isAssignableFrom(udfClass)) {
      FunctionRegistry.registerTemporaryGenericUDTF(
        functionName, (Class<? extends GenericUDTF>) udfClass);
    } else if (UDAF.class.isAssignableFrom(udfClass)) {
      FunctionRegistry.registerTemporaryUDAF(
        functionName, (Class<? extends UDAF>) udfClass);
    } else if (GenericUDAFResolver.class.isAssignableFrom(udfClass)) {
      FunctionRegistry.registerTemporaryGenericUDAF(
        functionName, (GenericUDAFResolver)
        ReflectionUtils.newInstance(udfClass, null));
    } else {
      return false;
    }
    return true;
  }

  /**
   * Registers Hive functions from a plugin jar, using metadata from
   * the jar's META-INF/class-info.xml.
   *
   * @param jarLocation URL for reading jar file
   *
   * @param classLoader classloader to use for loading function classes
   */
  public static void registerFunctionsFromPluginJar(
    URL jarLocation,
    ClassLoader classLoader) throws Exception {

    URL url = new URL("jar:" + jarLocation + "!/META-INF/class-info.xml");
    InputStream inputStream = null;
    try {
      inputStream = url.openStream();
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder docBuilder = dbf.newDocumentBuilder();
      Document doc = docBuilder.parse(inputStream);
      Element root = doc.getDocumentElement();
      if (!root.getTagName().equals("ClassList")) {
        return;
      }
      NodeList children = root.getElementsByTagName("Class");
      for (int i = 0; i < children.getLength(); ++i) {
        Element child = (Element) children.item(i);
        String javaName = child.getAttribute("javaname");
        String sqlName = child.getAttribute("sqlname");
        Class<?> udfClass = Class.forName(javaName, true, classLoader);
        boolean registered = registerTemporaryFunction(sqlName, udfClass);
        if (!registered) {
          throw new RuntimeException(
            "Class " + udfClass + " is not a Hive function implementation");
        }
      }
    } finally {
      IOUtils.closeStream(inputStream);
    }
  }

  private FunctionRegistry() {
    // prevent instantiation
  }


  //---------PTF functions------------

  public static void registerWindowFunction(String name, GenericUDAFResolver wFn)
  {
    registerWindowFunction(name, wFn, true);
  }

  /**
   * Typically a WindowFunction is the same as a UDAF. The only exceptions are Lead & Lag UDAFs. These
   * are not registered as regular UDAFs because
   * - we plan to support Lead & Lag as UDFs (usable only within argument expressions
   *   of UDAFs when windowing is involved). Since mFunctions holds both UDFs and UDAFs we cannot
   *   add both FunctionInfos to mFunctions.
   * We choose to only register UDFs in mFunctions. The implication of this is that Lead/Lag UDAFs
   * are only usable when windowing is involved.
   *
   * @param name
   * @param wFn
   * @param registerAsUDAF
   */
  public static void registerWindowFunction(String name, GenericUDAFResolver wFn, boolean registerAsUDAF)
  {
    FunctionInfo fInfo = null;
    if (registerAsUDAF) {
      registerGenericUDAF(true, name, wFn);
      fInfo = getFunctionInfo(name);
    }
    else {
      fInfo = new FunctionInfo(true,
          name.toLowerCase(), wFn);
    }

    WindowFunctionInfo wInfo = new WindowFunctionInfo(fInfo);
    windowFunctions.put(name.toLowerCase(), wInfo);
  }

  public static WindowFunctionInfo getWindowFunctionInfo(String name)
  {
    return windowFunctions.get(name.toLowerCase());
  }

  public static boolean impliesOrder(String functionName) {
    return functionName == null ? false : UDAFS_IMPLY_ORDER.contains(functionName.toLowerCase());
  }

  static void registerHiveUDAFsAsWindowFunctions()
  {
    Set<String> fNames = getFunctionNames();
    for(String fName : fNames)
    {
      FunctionInfo fInfo = getFunctionInfo(fName);
      if ( fInfo.isGenericUDAF())
      {
        WindowFunctionInfo wInfo = new WindowFunctionInfo(fInfo);
        windowFunctions.put(fName, wInfo);
      }
    }
  }

  public static boolean isTableFunction(String name)
  {
    PTFFunctionInfo tFInfo = tableFunctions.get(name.toLowerCase());
     return tFInfo != null && !tFInfo.isInternal();
  }

  public static TableFunctionResolver getTableFunctionResolver(String name)
  {
    PTFFunctionInfo tfInfo = tableFunctions.get(name.toLowerCase());
    return (TableFunctionResolver) ReflectionUtils.newInstance(tfInfo.getFunctionResolver(), null);
  }

  public static TableFunctionResolver getWindowingTableFunction()
  {
    return getTableFunctionResolver(WINDOWING_TABLE_FUNCTION);
  }

  public static TableFunctionResolver getNoopTableFunction()
  {
    return getTableFunctionResolver(NOOP_TABLE_FUNCTION);
  }

  public static void registerTableFunction(String name, Class<? extends TableFunctionResolver> tFnCls)
  {
    PTFFunctionInfo tInfo = new PTFFunctionInfo(name, tFnCls);
    tableFunctions.put(name.toLowerCase(), tInfo);
  }

}
