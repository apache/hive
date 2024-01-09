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

package org.apache.hadoop.hive.ql.exec;

import java.lang.reflect.Method;
import java.util.function.BiFunction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.hive.ql.udf.esri.ST_Aggr_ConvexHull;
import org.apache.hadoop.hive.ql.udf.esri.ST_Aggr_Union;
import org.apache.hadoop.hive.ql.udf.esri.ST_Area;
import org.apache.hadoop.hive.ql.udf.esri.ST_AsBinary;
import org.apache.hadoop.hive.ql.udf.esri.ST_AsGeoJson;
import org.apache.hadoop.hive.ql.udf.esri.ST_AsJson;
import org.apache.hadoop.hive.ql.udf.esri.ST_AsShape;
import org.apache.hadoop.hive.ql.udf.esri.ST_AsText;
import org.apache.hadoop.hive.ql.udf.esri.ST_Bin;
import org.apache.hadoop.hive.ql.udf.esri.ST_BinEnvelope;
import org.apache.hadoop.hive.ql.udf.esri.ST_Boundary;
import org.apache.hadoop.hive.ql.udf.esri.ST_Buffer;
import org.apache.hadoop.hive.ql.udf.esri.ST_Centroid;
import org.apache.hadoop.hive.ql.udf.esri.ST_Contains;
import org.apache.hadoop.hive.ql.udf.esri.ST_ConvexHull;
import org.apache.hadoop.hive.ql.udf.esri.ST_CoordDim;
import org.apache.hadoop.hive.ql.udf.esri.ST_Crosses;
import org.apache.hadoop.hive.ql.udf.esri.ST_Difference;
import org.apache.hadoop.hive.ql.udf.esri.ST_Dimension;
import org.apache.hadoop.hive.ql.udf.esri.ST_Disjoint;
import org.apache.hadoop.hive.ql.udf.esri.ST_Distance;
import org.apache.hadoop.hive.ql.udf.esri.ST_EndPoint;
import org.apache.hadoop.hive.ql.udf.esri.ST_EnvIntersects;
import org.apache.hadoop.hive.ql.udf.esri.ST_Envelope;
import org.apache.hadoop.hive.ql.udf.esri.ST_Equals;
import org.apache.hadoop.hive.ql.udf.esri.ST_ExteriorRing;
import org.apache.hadoop.hive.ql.udf.esri.ST_GeodesicLengthWGS84;
import org.apache.hadoop.hive.ql.udf.esri.ST_GeomCollection;
import org.apache.hadoop.hive.ql.udf.esri.ST_GeomFromGeoJson;
import org.apache.hadoop.hive.ql.udf.esri.ST_GeomFromJson;
import org.apache.hadoop.hive.ql.udf.esri.ST_GeomFromShape;
import org.apache.hadoop.hive.ql.udf.esri.ST_GeomFromText;
import org.apache.hadoop.hive.ql.udf.esri.ST_GeomFromWKB;
import org.apache.hadoop.hive.ql.udf.esri.ST_GeometryN;
import org.apache.hadoop.hive.ql.udf.esri.ST_GeometryType;
import org.apache.hadoop.hive.ql.udf.esri.ST_InteriorRingN;
import org.apache.hadoop.hive.ql.udf.esri.ST_Intersection;
import org.apache.hadoop.hive.ql.udf.esri.ST_Intersects;
import org.apache.hadoop.hive.ql.udf.esri.ST_Is3D;
import org.apache.hadoop.hive.ql.udf.esri.ST_IsClosed;
import org.apache.hadoop.hive.ql.udf.esri.ST_IsEmpty;
import org.apache.hadoop.hive.ql.udf.esri.ST_IsMeasured;
import org.apache.hadoop.hive.ql.udf.esri.ST_IsRing;
import org.apache.hadoop.hive.ql.udf.esri.ST_IsSimple;
import org.apache.hadoop.hive.ql.udf.esri.ST_Length;
import org.apache.hadoop.hive.ql.udf.esri.ST_LineFromWKB;
import org.apache.hadoop.hive.ql.udf.esri.ST_LineString;
import org.apache.hadoop.hive.ql.udf.esri.ST_M;
import org.apache.hadoop.hive.ql.udf.esri.ST_MLineFromWKB;
import org.apache.hadoop.hive.ql.udf.esri.ST_MPointFromWKB;
import org.apache.hadoop.hive.ql.udf.esri.ST_MPolyFromWKB;
import org.apache.hadoop.hive.ql.udf.esri.ST_MaxM;
import org.apache.hadoop.hive.ql.udf.esri.ST_MaxX;
import org.apache.hadoop.hive.ql.udf.esri.ST_MaxY;
import org.apache.hadoop.hive.ql.udf.esri.ST_MaxZ;
import org.apache.hadoop.hive.ql.udf.esri.ST_MinM;
import org.apache.hadoop.hive.ql.udf.esri.ST_MinX;
import org.apache.hadoop.hive.ql.udf.esri.ST_MinY;
import org.apache.hadoop.hive.ql.udf.esri.ST_MinZ;
import org.apache.hadoop.hive.ql.udf.esri.ST_MultiLineString;
import org.apache.hadoop.hive.ql.udf.esri.ST_MultiPoint;
import org.apache.hadoop.hive.ql.udf.esri.ST_MultiPolygon;
import org.apache.hadoop.hive.ql.udf.esri.ST_NumGeometries;
import org.apache.hadoop.hive.ql.udf.esri.ST_NumInteriorRing;
import org.apache.hadoop.hive.ql.udf.esri.ST_NumPoints;
import org.apache.hadoop.hive.ql.udf.esri.ST_Overlaps;
import org.apache.hadoop.hive.ql.udf.esri.ST_Point;
import org.apache.hadoop.hive.ql.udf.esri.ST_PointFromWKB;
import org.apache.hadoop.hive.ql.udf.esri.ST_PointN;
import org.apache.hadoop.hive.ql.udf.esri.ST_PointZ;
import org.apache.hadoop.hive.ql.udf.esri.ST_PolyFromWKB;
import org.apache.hadoop.hive.ql.udf.esri.ST_Polygon;
import org.apache.hadoop.hive.ql.udf.esri.ST_Relate;
import org.apache.hadoop.hive.ql.udf.esri.ST_SRID;
import org.apache.hadoop.hive.ql.udf.esri.ST_SetSRID;
import org.apache.hadoop.hive.ql.udf.esri.ST_StartPoint;
import org.apache.hadoop.hive.ql.udf.esri.ST_SymmetricDiff;
import org.apache.hadoop.hive.ql.udf.esri.ST_Touches;
import org.apache.hadoop.hive.ql.udf.esri.ST_Union;
import org.apache.hadoop.hive.ql.udf.esri.ST_Within;
import org.apache.hadoop.hive.ql.udf.esri.ST_X;
import org.apache.hadoop.hive.ql.udf.esri.ST_Y;
import org.apache.hadoop.hive.ql.udf.esri.ST_Z;
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
import org.apache.hadoop.hive.ql.udf.UDFBuildVersion;
import org.apache.hadoop.hive.ql.udf.UDFChr;
import org.apache.hadoop.hive.ql.udf.UDFConv;
import org.apache.hadoop.hive.ql.udf.UDFCos;
import org.apache.hadoop.hive.ql.udf.UDFCrc32;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorDay;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorHour;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorMinute;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorMonth;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorQuarter;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorSecond;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorWeek;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorYear;
import org.apache.hadoop.hive.ql.udf.UDFDayOfMonth;
import org.apache.hadoop.hive.ql.udf.UDFDayOfWeek;
import org.apache.hadoop.hive.ql.udf.UDFDegrees;
import org.apache.hadoop.hive.ql.udf.UDFE;
import org.apache.hadoop.hive.ql.udf.UDFExp;
import org.apache.hadoop.hive.ql.udf.UDFFindInSet;
import org.apache.hadoop.hive.ql.udf.UDFHex;
import org.apache.hadoop.hive.ql.udf.UDFHour;
import org.apache.hadoop.hive.ql.udf.UDFJson;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.UDFLn;
import org.apache.hadoop.hive.ql.udf.UDFLog;
import org.apache.hadoop.hive.ql.udf.UDFLog10;
import org.apache.hadoop.hive.ql.udf.UDFLog2;
import org.apache.hadoop.hive.ql.udf.UDFMd5;
import org.apache.hadoop.hive.ql.udf.UDFMinute;
import org.apache.hadoop.hive.ql.udf.UDFMonth;
import org.apache.hadoop.hive.ql.udf.UDFOPBitAnd;
import org.apache.hadoop.hive.ql.udf.UDFOPBitNot;
import org.apache.hadoop.hive.ql.udf.UDFOPBitOr;
import org.apache.hadoop.hive.ql.udf.UDFOPBitShiftLeft;
import org.apache.hadoop.hive.ql.udf.UDFOPBitShiftRight;
import org.apache.hadoop.hive.ql.udf.UDFOPBitShiftRightUnsigned;
import org.apache.hadoop.hive.ql.udf.UDFOPBitXor;
import org.apache.hadoop.hive.ql.udf.UDFOPLongDivide;
import org.apache.hadoop.hive.ql.udf.UDFPI;
import org.apache.hadoop.hive.ql.udf.UDFParseUrl;
import org.apache.hadoop.hive.ql.udf.UDFRadians;
import org.apache.hadoop.hive.ql.udf.UDFRand;
import org.apache.hadoop.hive.ql.udf.UDFRegExpExtract;
import org.apache.hadoop.hive.ql.udf.UDFRegExpReplace;
import org.apache.hadoop.hive.ql.udf.UDFRepeat;
import org.apache.hadoop.hive.ql.udf.UDFReplace;
import org.apache.hadoop.hive.ql.udf.UDFReverse;
import org.apache.hadoop.hive.ql.udf.UDFSecond;
import org.apache.hadoop.hive.ql.udf.UDFSha1;
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
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.UDFUUID;
import org.apache.hadoop.hive.ql.udf.UDFUnbase64;
import org.apache.hadoop.hive.ql.udf.UDFUnhex;
import org.apache.hadoop.hive.ql.udf.UDFVersion;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.apache.hadoop.hive.ql.udf.UDFYear;
import org.apache.hadoop.hive.ql.udf.UDFSinh;
import org.apache.hadoop.hive.ql.udf.UDFCosh;
import org.apache.hadoop.hive.ql.udf.UDFTanh;
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
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.common.util.AnnotationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FunctionRegistry.
 */
public final class FunctionRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(FunctionRegistry.class);

  /*
   * PTF variables
   * */

  public static final String LEAD_FUNC_NAME = "lead";
  public static final String LAG_FUNC_NAME = "lag";
  public static final String LAST_VALUE_FUNC_NAME = "last_value";

  public static final String UNARY_PLUS_FUNC_NAME = "positive";
  public static final String UNARY_MINUS_FUNC_NAME = "negative";

  public static final String BLOOM_FILTER_FUNCTION = "bloom_filter";
  public static final String WINDOWING_TABLE_FUNCTION = "windowingtablefunction";

  public static final String ARRAY_FUNC_NAME = "array";
  public static final String INLINE_FUNC_NAME = "inline";

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
    system.registerUDF("mid", UDFSubstr.class, false);
    system.registerUDF("substr", UDFSubstr.class, false);
    system.registerUDF("substring", UDFSubstr.class, false);
    system.registerGenericUDF("substring_index", GenericUDFSubstringIndex.class);
    system.registerUDF("space", UDFSpace.class, false);
    system.registerUDF("repeat", UDFRepeat.class, false);
    system.registerUDF("ascii", UDFAscii.class, false);
    system.registerGenericUDF("lpad", GenericUDFLpad.class);
    system.registerGenericUDF("rpad", GenericUDFRpad.class);
    system.registerGenericUDF("levenshtein", GenericUDFLevenshtein.class);
    system.registerGenericUDF("soundex", GenericUDFSoundex.class);

    system.registerGenericUDF("size", GenericUDFSize.class);

    system.registerGenericUDF("round", GenericUDFRound.class);
    system.registerGenericUDF("bround", GenericUDFBRound.class);
    system.registerGenericUDF("floor", GenericUDFFloor.class);
    system.registerUDF("sqrt", UDFSqrt.class, false);
    system.registerGenericUDF("cbrt", GenericUDFCbrt.class);
    system.registerGenericUDF("ceil", GenericUDFCeil.class);
    system.registerGenericUDF("ceiling", GenericUDFCeil.class);
    system.registerUDF("rand", UDFRand.class, false);
    system.registerGenericUDF("abs", GenericUDFAbs.class);
    system.registerGenericUDF("json_read", GenericUDFJsonRead.class);
    system.registerGenericUDF("sq_count_check", GenericUDFSQCountCheck.class);
    system.registerGenericUDF("enforce_constraint", GenericUDFEnforceConstraint.class);
    system.registerGenericUDF("pmod", GenericUDFPosMod.class);

    system.registerUDF("ln", UDFLn.class, false);
    system.registerUDF("log2", UDFLog2.class, false);
    system.registerUDF("sin", UDFSin.class, false);
    system.registerUDF("asin", UDFAsin.class, false);
    system.registerUDF("sinh", UDFSinh.class, false);
    system.registerUDF("cos", UDFCos.class, false);
    system.registerUDF("acos", UDFAcos.class, false);
    system.registerUDF("cosh", UDFCosh.class, false);
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
    system.registerUDF("tanh", UDFTanh.class, false);
    system.registerUDF("e", UDFE.class, false);
    system.registerGenericUDF("factorial", GenericUDFFactorial.class);
    system.registerUDF("crc32", UDFCrc32.class, false);

    system.registerUDF("conv", UDFConv.class, false);
    system.registerUDF("bin", UDFBin.class, false);
    system.registerUDF("chr", UDFChr.class, false);
    system.registerUDF("hex", UDFHex.class, false);
    system.registerUDF("unhex", UDFUnhex.class, false);
    system.registerUDF("base64", UDFBase64.class, false);
    system.registerUDF("unbase64", UDFUnbase64.class, false);
    system.registerGenericUDF("sha2", GenericUDFSha2.class);
    system.registerUDF("md5", UDFMd5.class, false);
    system.registerUDF("sha1", UDFSha1.class, false);
    system.registerUDF("sha", UDFSha1.class, false);
    system.registerGenericUDF("aes_encrypt", GenericUDFAesEncrypt.class);
    system.registerGenericUDF("aes_decrypt", GenericUDFAesDecrypt.class);
    system.registerUDF("uuid", UDFUUID.class, false);

    system.registerGenericUDF("encode", GenericUDFEncode.class);
    system.registerGenericUDF("decode", GenericUDFDecode.class);

    system.registerGenericUDF("upper", GenericUDFUpper.class);
    system.registerGenericUDF("lower", GenericUDFLower.class);
    system.registerGenericUDF("ucase", GenericUDFUpper.class);
    system.registerGenericUDF("lcase", GenericUDFLower.class);
    system.registerGenericUDF("trim", GenericUDFTrim.class);
    system.registerGenericUDF("ltrim", GenericUDFLTrim.class);
    system.registerGenericUDF("rtrim", GenericUDFRTrim.class);
    system.registerGenericUDF("length", GenericUDFLength.class);
    system.registerGenericUDF("character_length", GenericUDFCharacterLength.class);
    system.registerGenericUDF("char_length", GenericUDFCharacterLength.class);
    system.registerGenericUDF("octet_length", GenericUDFOctetLength.class);
    system.registerUDF("reverse", UDFReverse.class, false);
    system.registerGenericUDF("field", GenericUDFField.class);
    system.registerUDF("find_in_set", UDFFindInSet.class, false);
    system.registerGenericUDF("initcap", GenericUDFInitCap.class);

    system.registerUDF("like", UDFLike.class, true);
    system.registerGenericUDF("likeany", GenericUDFLikeAny.class);
    system.registerGenericUDF("likeall", GenericUDFLikeAll.class);
    system.registerGenericUDF("rlike", GenericUDFRegExp.class);
    system.registerGenericUDF("regexp", GenericUDFRegExp.class);
    system.registerUDF("regexp_replace", UDFRegExpReplace.class, false);
    system.registerUDF("replace", UDFReplace.class, false);
    system.registerUDF("regexp_extract", UDFRegExpExtract.class, false);
    system.registerUDF("parse_url", UDFParseUrl.class, false);
    system.registerGenericUDF("quote", GenericUDFQuote.class);
    system.registerGenericUDF("nvl", GenericUDFCoalesce.class); //HIVE-20961
    system.registerGenericUDF("split", GenericUDFSplit.class);
    system.registerGenericUDF("split_map_privs", GenericUDFStringToPrivilege.class);
    system.registerGenericUDF("str_to_map", GenericUDFStringToMap.class);
    system.registerGenericUDF("translate", GenericUDFTranslate.class);
    system.registerGenericUDF("validate_acid_sort_order", GenericUDFValidateAcidSortOrder.class);

    system.registerGenericUDF(UNARY_PLUS_FUNC_NAME, GenericUDFOPPositive.class);
    system.registerGenericUDF(UNARY_MINUS_FUNC_NAME, GenericUDFOPNegative.class);

    system.registerGenericUDF("day", UDFDayOfMonth.class);
    system.registerGenericUDF("dayofmonth", UDFDayOfMonth.class);
    system.registerUDF("dayofweek", UDFDayOfWeek.class, false);
    system.registerGenericUDF("month", UDFMonth.class);
    system.registerGenericUDF("quarter", GenericUDFQuarter.class);
    system.registerGenericUDF("year", UDFYear.class);
    system.registerGenericUDF("hour", UDFHour.class);
    system.registerGenericUDF("minute", UDFMinute.class);
    system.registerGenericUDF("second", UDFSecond.class);
    system.registerGenericUDF("from_unixtime", GenericUDFFromUnixTime.class);
    system.registerGenericUDF("to_date", GenericUDFDate.class);
    system.registerUDF("weekofyear", UDFWeekOfYear.class, false);
    system.registerGenericUDF("last_day", GenericUDFLastDay.class);
    system.registerGenericUDF("next_day", GenericUDFNextDay.class);
    system.registerGenericUDF("trunc", GenericUDFTrunc.class);
    system.registerGenericUDF("date_format", GenericUDFDateFormat.class);

    // Special date formatting functions
    system.registerUDF("floor_year", UDFDateFloorYear.class, false);
    system.registerUDF("floor_quarter", UDFDateFloorQuarter.class, false);
    system.registerUDF("floor_month", UDFDateFloorMonth.class, false);
    system.registerUDF("floor_day", UDFDateFloorDay.class, false);
    system.registerUDF("floor_week", UDFDateFloorWeek.class, false);
    system.registerUDF("floor_hour", UDFDateFloorHour.class, false);
    system.registerUDF("floor_minute", UDFDateFloorMinute.class, false);
    system.registerUDF("floor_second", UDFDateFloorSecond.class, false);

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
    system.registerGenericUDF("mod", GenericUDFOPMod.class);
    system.registerUDF("div", UDFOPLongDivide.class, true);

    system.registerUDF("&", UDFOPBitAnd.class, true);
    system.registerUDF("|", UDFOPBitOr.class, true);
    system.registerUDF("^", UDFOPBitXor.class, true);
    system.registerUDF("~", UDFOPBitNot.class, true);
    system.registerUDF("shiftleft", UDFOPBitShiftLeft.class, true);
    system.registerUDF("shiftright", UDFOPBitShiftRight.class, true);
    system.registerUDF("shiftrightunsigned", UDFOPBitShiftRightUnsigned.class, true);

    system.registerGenericUDF("grouping", GenericUDFGrouping.class);

    system.registerGenericUDF("current_database", GenericUDFCurrentDatabase.class);
    system.registerGenericUDF("current_schema", GenericUDFCurrentSchema.class);
    system.registerGenericUDF("current_catalog", GenericUDFCurrentCatalog.class);
    system.registerGenericUDF("current_date", GenericUDFCurrentDate.class);
    system.registerGenericUDF("current_timestamp", GenericUDFCurrentTimestamp.class);
    system.registerGenericUDF("current_user", GenericUDFCurrentUser.class);
    system.registerGenericUDF("current_groups", GenericUDFCurrentGroups.class);
    system.registerGenericUDF("logged_in_user", GenericUDFLoggedInUser.class);
    system.registerGenericUDF("restrict_information_schema", GenericUDFRestrictInformationSchema.class);
    system.registerGenericUDF("current_authorizer", GenericUDFCurrentAuthorizer.class);

    system.registerGenericUDF("surrogate_key", GenericUDFSurrogateKey.class);

    system.registerGenericUDF("isnull", GenericUDFOPNull.class);
    system.registerGenericUDF("isnotnull", GenericUDFOPNotNull.class);
    system.registerGenericUDF("istrue", GenericUDFOPTrue.class);
    system.registerGenericUDF("isnottrue", GenericUDFOPNotTrue.class);
    system.registerGenericUDF("isfalse", GenericUDFOPFalse.class);
    system.registerGenericUDF("isnotfalse", GenericUDFOPNotFalse.class);

    system.registerGenericUDF("if", GenericUDFIf.class);
    system.registerGenericUDF("in", GenericUDFIn.class);
    system.registerGenericUDF("and", GenericUDFOPAnd.class);
    system.registerGenericUDF("or", GenericUDFOPOr.class);
    system.registerGenericUDF("=", GenericUDFOPEqual.class);
    system.registerGenericUDF("==", GenericUDFOPEqual.class);
    system.registerGenericUDF("<=>", GenericUDFOPEqualNS.class);
    system.registerGenericUDF("is_not_distinct_from", GenericUDFOPEqualNS.class);
    system.registerGenericUDF("!=", GenericUDFOPNotEqual.class);
    system.registerGenericUDF("<>", GenericUDFOPNotEqual.class);
    system.registerGenericUDF("<", GenericUDFOPLessThan.class);
    system.registerGenericUDF("<=", GenericUDFOPEqualOrLessThan.class);
    system.registerGenericUDF(">", GenericUDFOPGreaterThan.class);
    system.registerGenericUDF(">=", GenericUDFOPEqualOrGreaterThan.class);
    system.registerGenericUDF("not", GenericUDFOPNot.class);
    system.registerGenericUDF("!", GenericUDFOPNot.class);
    system.registerGenericUDF("between", GenericUDFBetween.class);
    system.registerGenericUDF("in_bloom_filter", GenericUDFInBloomFilter.class);
    system.registerGenericUDF("toMap", GenericUDFToMap.class);
    system.registerGenericUDF("toArray", GenericUDFToArray.class);
    system.registerGenericUDF("toStruct", GenericUDFToStruct.class);

    // Utility UDFs
    system.registerUDF("version", UDFVersion.class, false);
    system.registerUDF("buildversion", UDFBuildVersion.class, false);

    // Aliases for Java Class Names
    // These are used in getImplicitConvertUDFMethod
    system.registerUDF(serdeConstants.BOOLEAN_TYPE_NAME, UDFToBoolean.class, false, UDFToBoolean.class.getSimpleName());
    system.registerUDF(serdeConstants.TINYINT_TYPE_NAME, UDFToByte.class, false, UDFToByte.class.getSimpleName());
    system.registerUDF(serdeConstants.SMALLINT_TYPE_NAME, UDFToShort.class, false, UDFToShort.class.getSimpleName());
    system.registerUDF(serdeConstants.INT_TYPE_NAME, UDFToInteger.class, false, UDFToInteger.class.getSimpleName());
    system.registerUDF(serdeConstants.BIGINT_TYPE_NAME, UDFToLong.class, false, UDFToLong.class.getSimpleName());
    system.registerUDF(serdeConstants.FLOAT_TYPE_NAME, UDFToFloat.class, false, UDFToFloat.class.getSimpleName());
    system.registerUDF(serdeConstants.DOUBLE_TYPE_NAME, UDFToDouble.class, false, UDFToDouble.class.getSimpleName());
    // following mapping is to enable UDFName to UDF while generating expression for default value (in operator tree)
    //  e.g. cast(4 as string) is serialized as UDFToString(4) into metastore, to allow us to generate appropriate UDF for
    //  UDFToString we need the following mappings
    // Rest of the types e.g. DATE, CHAR, VARCHAR etc are already registered
    // TODO: According to vgarg, these function mappings are no longer necessary as the default value logic has changed.
    system.registerUDF(UDFToBoolean.class.getSimpleName(), UDFToBoolean.class, false, UDFToBoolean.class.getSimpleName());
    system.registerUDF(UDFToDouble.class.getSimpleName(), UDFToDouble.class, false, UDFToDouble.class.getSimpleName());
    system.registerUDF(UDFToFloat.class.getSimpleName(), UDFToFloat.class, false, UDFToFloat.class.getSimpleName());
    system.registerUDF(UDFToInteger.class.getSimpleName(), UDFToInteger.class, false, UDFToInteger.class.getSimpleName());
    system.registerUDF(UDFToLong.class.getSimpleName(), UDFToLong.class, false, UDFToLong.class.getSimpleName());
    system.registerUDF(UDFToShort.class.getSimpleName(), UDFToShort.class, false, UDFToShort.class.getSimpleName());
    system.registerUDF(UDFToByte.class.getSimpleName(), UDFToByte.class, false, UDFToByte.class.getSimpleName());

    system.registerGenericUDF(serdeConstants.STRING_TYPE_NAME, GenericUDFToString.class);
    system.registerGenericUDF(serdeConstants.DATE_TYPE_NAME, GenericUDFToDate.class);
    system.registerGenericUDF(serdeConstants.TIMESTAMP_TYPE_NAME, GenericUDFTimestamp.class);
    system.registerGenericUDF(serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME, GenericUDFToTimestampLocalTZ.class);
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
    system.registerGenericUDAF("$SUM0", new GenericUDAFSumEmptyIsZero());
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
    system.registerGenericUDAF("regr_slope", new GenericUDAFBinarySetFunctions.RegrSlope());
    system.registerGenericUDAF("regr_intercept", new GenericUDAFBinarySetFunctions.RegrIntercept());
    system.registerGenericUDAF("regr_r2", new GenericUDAFBinarySetFunctions.RegrR2());
    system.registerGenericUDAF("regr_sxx", new GenericUDAFBinarySetFunctions.RegrSXX());
    system.registerGenericUDAF("regr_syy", new GenericUDAFBinarySetFunctions.RegrSYY());
    system.registerGenericUDAF("regr_sxy", new GenericUDAFBinarySetFunctions.RegrSXY());
    system.registerGenericUDAF("regr_avgx", new GenericUDAFBinarySetFunctions.RegrAvgX());
    system.registerGenericUDAF("regr_avgy", new GenericUDAFBinarySetFunctions.RegrAvgY());
    system.registerGenericUDAF("regr_count", new GenericUDAFBinarySetFunctions.RegrCount());

    system.registerGenericUDAF("histogram_numeric", new GenericUDAFHistogramNumeric());
    system.registerGenericUDAF("percentile_approx", new GenericUDAFPercentileApprox());
    system.registerGenericUDAF("collect_set", new GenericUDAFCollectSet());
    system.registerGenericUDAF("collect_list", new GenericUDAFCollectList());

    system.registerGenericUDAF("ngrams", new GenericUDAFnGrams());
    system.registerGenericUDAF("context_ngrams", new GenericUDAFContextNGrams());

    system.registerGenericUDF("ndv_compute_bit_vector", GenericUDFNDVComputeBitVector.class);
    system.registerGenericUDAF("compute_bit_vector_hll", new GenericUDAFComputeBitVectorHLL());
    system.registerGenericUDAF("compute_bit_vector_fm", new GenericUDAFComputeBitVectorFMSketch());
    system.registerGenericUDAF(BLOOM_FILTER_FUNCTION, new GenericUDAFBloomFilter());
    system.registerGenericUDAF("approx_distinct", new GenericUDAFApproximateDistinct());
    system.registerUDAF("percentile", UDAFPercentile.class);
    system.registerGenericUDAF("percentile_cont", new GenericUDAFPercentileCont());
    system.registerGenericUDAF("percentile_disc", new GenericUDAFPercentileDisc());
    system.registerGenericUDAF("exception_in_vertex_udaf", new GenericUDAFExceptionInVertex());

    system.registerUDFPlugin(DataSketchesFunctions.INSTANCE);

    // Generic UDFs
    system.registerGenericUDF("reflect", GenericUDFReflect.class);
    system.registerGenericUDF("reflect2", GenericUDFReflect2.class);
    system.registerGenericUDF("java_method", GenericUDFReflect.class);
    system.registerGenericUDF("exception_in_vertex_udf", GenericUDFExceptionInVertex.class);

    system.registerGenericUDF(ARRAY_FUNC_NAME, GenericUDFArray.class);
    system.registerGenericUDF("assert_true", GenericUDFAssertTrue.class);
    system.registerGenericUDF("assert_true_oom", GenericUDFAssertTrueOOM.class);
    system.registerGenericUDF("map", GenericUDFMap.class);
    system.registerGenericUDF("struct", GenericUDFStruct.class);
    system.registerGenericUDF("named_struct", GenericUDFNamedStruct.class);
    system.registerGenericUDF("create_union", GenericUDFUnion.class);
    system.registerGenericUDF("extract_union", GenericUDFExtractUnion.class);

    system.registerGenericUDF("case", GenericUDFCase.class);
    system.registerGenericUDF("when", GenericUDFWhen.class);
    system.registerGenericUDF("nullif", GenericUDFNullif.class);
    system.registerGenericUDF("hash", GenericUDFHash.class);
    system.registerGenericUDF("murmur_hash", GenericUDFMurmurHash.class);
    system.registerGenericUDF("coalesce", GenericUDFCoalesce.class);
    system.registerGenericUDF("index", GenericUDFIndex.class);
    system.registerGenericUDF("in_file", GenericUDFInFile.class);
    system.registerGenericUDF("instr", GenericUDFInstr.class);
    system.registerGenericUDF("locate", GenericUDFLocate.class);
    system.registerGenericUDF("position", GenericUDFLocate.class);
    system.registerGenericUDF("elt", GenericUDFElt.class);
    system.registerGenericUDF("concat_ws", GenericUDFConcatWS.class);
    system.registerGenericUDF("sort_array", GenericUDFSortArray.class);
    system.registerGenericUDF("sort_array_by", GenericUDFSortArrayByField.class);
    system.registerGenericUDF("array_contains", GenericUDFArrayContains.class);
    system.registerGenericUDF("array_min", GenericUDFArrayMin.class);
    system.registerGenericUDF("array_max", GenericUDFArrayMax.class);
    system.registerGenericUDF("array_distinct", GenericUDFArrayDistinct.class);
    system.registerGenericUDF("array_join", GenericUDFArrayJoin.class);
    system.registerGenericUDF("array_slice", GenericUDFArraySlice.class);
    system.registerGenericUDF("array_except", GenericUDFArrayExcept.class);
    system.registerGenericUDF("array_intersect", GenericUDFArrayIntersect.class);
    system.registerGenericUDF("array_union", GenericUDFArrayUnion.class);
    system.registerGenericUDF("array_remove", GenericUDFArrayRemove.class);
    system.registerGenericUDF("deserialize", GenericUDFDeserialize.class);
    system.registerGenericUDF("sentences", GenericUDFSentences.class);
    system.registerGenericUDF("map_keys", GenericUDFMapKeys.class);
    system.registerGenericUDF("map_values", GenericUDFMapValues.class);
    system.registerGenericUDF("format_number", GenericUDFFormatNumber.class);
    system.registerGenericUDF("printf", GenericUDFPrintf.class);
    system.registerGenericUDF("greatest", GenericUDFGreatest.class);
    system.registerGenericUDF("least", GenericUDFLeast.class);
    system.registerGenericUDF("cardinality_violation", GenericUDFCardinalityViolation.class);
    system.registerGenericUDF("width_bucket", GenericUDFWidthBucket.class);
    system.registerGenericUDF("typeof", GenericUDFTypeOf.class);

    system.registerGenericUDF("from_utc_timestamp", GenericUDFFromUtcTimestamp.class);
    system.registerGenericUDF("to_utc_timestamp", GenericUDFToUtcTimestamp.class);

    system.registerGenericUDF("unix_timestamp", GenericUDFUnixTimeStamp.class);
    system.registerGenericUDF("to_unix_timestamp", GenericUDFToUnixTimeStamp.class);

    system.registerGenericUDF("datetime_legacy_hybrid_calendar", GenericUDFDatetimeLegacyHybridCalendar.class);

    system.registerGenericUDF("internal_interval", GenericUDFInternalInterval.class);

    system.registerGenericUDF("to_epoch_milli", GenericUDFEpochMilli.class);
    system.registerGenericUDF("bucket_number", GenericUDFBucketNumber.class);
    system.registerGenericUDF("tumbling_window", GenericUDFTumbledWindow.class);
    system.registerGenericUDF("cast_format", GenericUDFCastFormat.class);

    // Generic UDTF's
    system.registerGenericUDTF("explode", GenericUDTFExplode.class);
    system.registerGenericUDTF("replicate_rows", GenericUDTFReplicateRows.class);
    system.registerGenericUDTF(INLINE_FUNC_NAME, GenericUDTFInline.class);
    system.registerGenericUDTF("json_tuple", GenericUDTFJSONTuple.class);
    system.registerGenericUDTF("parse_url_tuple", GenericUDTFParseUrlTuple.class);
    system.registerGenericUDTF("posexplode", GenericUDTFPosExplode.class);
    system.registerGenericUDTF("stack", GenericUDTFStack.class);
    system.registerGenericUDTF("get_splits", GenericUDTFGetSplits.class);
    system.registerGenericUDTF("get_llap_splits", GenericUDTFGetSplits2.class);
    system.registerGenericUDTF("get_sql_schema", GenericUDTFGetSQLSchema.class);

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

    // Arithmetic specializations are done in a convoluted manner; mark them as built-in.
    system.registerHiddenBuiltIn(GenericUDFOPDTIMinus.class);
    system.registerHiddenBuiltIn(GenericUDFOPDTIPlus.class);
    system.registerHiddenBuiltIn(GenericUDFOPNumericMinus.class);
    system.registerHiddenBuiltIn(GenericUDFOPNumericPlus.class);
    // No operator for nullsafe not equal, but add as built-in to allow for LLAP.
    system.registerHiddenBuiltIn(GenericUDFOPNotEqualNS.class);

    // mask UDFs
    system.registerGenericUDF(GenericUDFMask.UDF_NAME, GenericUDFMask.class);
    system.registerGenericUDF(GenericUDFMaskFirstN.UDF_NAME, GenericUDFMaskFirstN.class);
    system.registerGenericUDF(GenericUDFMaskLastN.UDF_NAME, GenericUDFMaskLastN.class);
    system.registerGenericUDF(GenericUDFMaskShowFirstN.UDF_NAME, GenericUDFMaskShowFirstN.class);
    system.registerGenericUDF(GenericUDFMaskShowLastN.UDF_NAME, GenericUDFMaskShowLastN.class);
    system.registerGenericUDF(GenericUDFMaskHash.UDF_NAME, GenericUDFMaskHash.class);

    // GeoSpatial UDFs
    system.registerFunction("ST_Length", ST_Length.class);
    system.registerFunction("ST_LineString", ST_LineString.class);
    system.registerFunction("ST_Point", ST_Point.class);
    system.registerFunction("ST_AsText", ST_AsText.class);
    system.registerFunction("ST_Aggr_ConvexHull", ST_Aggr_ConvexHull.class);
    system.registerFunction("ST_Aggr_Union", ST_Aggr_Union.class);
    system.registerFunction("ST_Area", ST_Area.class);
    system.registerFunction("ST_AsBinary", ST_AsBinary.class);
    system.registerFunction("ST_AsGeoJson", ST_AsGeoJson.class);
    system.registerFunction("ST_AsJson", ST_AsJson.class);
    system.registerFunction("ST_AsShape", ST_AsShape.class);
    system.registerFunction("ST_Bin", ST_Bin.class);
    system.registerFunction("ST_BinEnvelope", ST_BinEnvelope.class);
    system.registerFunction("ST_Boundary", ST_Boundary.class);
    system.registerFunction("ST_Buffer", ST_Buffer.class);
    system.registerFunction("ST_Centroid", ST_Centroid.class);
    system.registerFunction("ST_Contains", ST_Contains.class);
    system.registerFunction("ST_ConvexHull", ST_ConvexHull.class);
    system.registerFunction("ST_CoordDim", ST_CoordDim.class);
    system.registerFunction("ST_Crosses", ST_Crosses.class);
    system.registerFunction("ST_Difference", ST_Difference.class);
    system.registerFunction("ST_Dimension", ST_Dimension.class);
    system.registerFunction("ST_Disjoint", ST_Disjoint.class);
    system.registerFunction("ST_Distance", ST_Distance.class);
    system.registerFunction("ST_EndPoint", ST_EndPoint.class);
    system.registerFunction("ST_Envelope", ST_Envelope.class);
    system.registerFunction("ST_EnvIntersects", ST_EnvIntersects.class);
    system.registerFunction("ST_Equals", ST_Equals.class);
    system.registerFunction("ST_ExteriorRing", ST_ExteriorRing.class);
    system.registerFunction("ST_GeodesicLengthWGS84", ST_GeodesicLengthWGS84.class);
    system.registerFunction("ST_GeomCollection", ST_GeomCollection.class);
    system.registerFunction("ST_GeometryN", ST_GeometryN.class);
    system.registerFunction("ST_GeomFromGeoJson", ST_GeomFromGeoJson.class);
    system.registerFunction("ST_GeomFromJson", ST_GeomFromJson.class);
    system.registerFunction("ST_GeomFromShape", ST_GeomFromShape.class);
    system.registerFunction("ST_GeomFromText", ST_GeomFromText.class);
    system.registerFunction("ST_GeomFromWKB", ST_GeomFromWKB.class);
    system.registerFunction("ST_GeometryType", ST_GeometryType.class);
    system.registerFunction("ST_InteriorRingN", ST_InteriorRingN.class);
    system.registerFunction("ST_Intersection", ST_Intersection.class);
    system.registerFunction("ST_Intersects", ST_Intersects.class);
    system.registerFunction("ST_Is3D", ST_Is3D.class);
    system.registerFunction("ST_IsClosed", ST_IsClosed.class);
    system.registerFunction("ST_IsEmpty", ST_IsEmpty.class);
    system.registerFunction("ST_IsMeasured", ST_IsMeasured.class);
    system.registerFunction("ST_IsRing", ST_IsRing.class);
    system.registerFunction("ST_IsSimple", ST_IsSimple.class);
    system.registerFunction("ST_LineFromWKB", ST_LineFromWKB.class);
    system.registerFunction("ST_M", ST_M.class);
    system.registerFunction("ST_MaxM", ST_MaxM.class);
    system.registerFunction("ST_MaxX", ST_MaxX.class);

    system.registerFunction("ST_MaxY", ST_MaxY.class);
    system.registerFunction("ST_MaxZ", ST_MaxZ.class);
    system.registerFunction("ST_MinM", ST_MinM.class);
    system.registerFunction("ST_MinX", ST_MinX.class);
    system.registerFunction("ST_MinY", ST_MinY.class);
    system.registerFunction("ST_MinZ", ST_MinZ.class);
    system.registerFunction("ST_MLineFromWKB", ST_MLineFromWKB.class);
    system.registerFunction("ST_MPointFromWKB", ST_MPointFromWKB.class);
    system.registerFunction("ST_MPolyFromWKB", ST_MPolyFromWKB.class);
    system.registerFunction("ST_MultiLineString", ST_MultiLineString.class);
    system.registerFunction("ST_MultiPoint", ST_MultiPoint.class);
    system.registerFunction("ST_MultiPolygon", ST_MultiPolygon.class);
    system.registerFunction("ST_NumGeometries", ST_NumGeometries.class);
    system.registerFunction("ST_NumInteriorRing", ST_NumInteriorRing.class);
    system.registerFunction("ST_NumPoints", ST_NumPoints.class);
    system.registerFunction("ST_Overlaps", ST_Overlaps.class);
    system.registerFunction("ST_PointFromWKB", ST_PointFromWKB.class);
    system.registerFunction("ST_PointN", ST_PointN.class);

    system.registerFunction("ST_PointZ", ST_PointZ.class);
    system.registerFunction("ST_PolyFromWKB", ST_PolyFromWKB.class);
    system.registerFunction("ST_Polygon", ST_Polygon.class);
    system.registerFunction("ST_Relate", ST_Relate.class);
    system.registerFunction("ST_SetSRID", ST_SetSRID.class);
    system.registerFunction("ST_SRID", ST_SRID.class);
    system.registerFunction("ST_StartPoint", ST_StartPoint.class);
    system.registerFunction("ST_SymmetricDiff", ST_SymmetricDiff.class);
    system.registerFunction("ST_Touches", ST_Touches.class);
    system.registerFunction("ST_Union", ST_Union.class);
    system.registerFunction("ST_Within", ST_Within.class);
    system.registerFunction("ST_X", ST_X.class);
    system.registerFunction("ST_Y", ST_Y.class);
    system.registerFunction("ST_Z", ST_Z.class);


    try {
      system.registerGenericUDF("iceberg_bucket",
          (Class<? extends GenericUDF>) Class.forName("org.apache.iceberg.mr.hive.udf.GenericUDFIcebergBucket"));
      system.registerGenericUDF("iceberg_truncate",
          (Class<? extends GenericUDF>) Class.forName("org.apache.iceberg.mr.hive.udf.GenericUDFIcebergTruncate"));
      system.registerGenericUDF("iceberg_year",
          (Class<? extends GenericUDF>) Class.forName("org.apache.iceberg.mr.hive.udf.GenericUDFIcebergYear"));
      system.registerGenericUDF("iceberg_month",
          (Class<? extends GenericUDF>) Class.forName("org.apache.iceberg.mr.hive.udf.GenericUDFIcebergMonth"));
      system.registerGenericUDF("iceberg_day",
          (Class<? extends GenericUDF>) Class.forName("org.apache.iceberg.mr.hive.udf.GenericUDFIcebergDay"));
      system.registerGenericUDF("iceberg_hour",
          (Class<? extends GenericUDF>) Class.forName("org.apache.iceberg.mr.hive.udf.GenericUDFIcebergHour"));
    } catch (ClassNotFoundException e) {
      LOG.warn("iceberg_bucket function could not be registered");
    }
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
      subpattern = "(?i)" + UDFLike.likePatternToRegExp(subpattern);
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
   *   varchar(10), varchar(20), primitive category varchar =&gt; varchar(20)
   *   date, string, primitive category string =&gt; string
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
   * Find a common type for union-all operator. Only the common types for the same
   * type group will resolve to a common type. No implicit conversion across different
   * type groups will be done.
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

    // untyped nulls
    if (pgA == PrimitiveGrouping.VOID_GROUP) {
      return b;
    }
    if (pgB == PrimitiveGrouping.VOID_GROUP) {
      return a;
    }

    if (pgA != pgB) {
      return null;
    }

    switch(pgA) {
    case STRING_GROUP:
      return getTypeInfoForPrimitiveCategory(
          (PrimitiveTypeInfo)a, (PrimitiveTypeInfo)b,PrimitiveCategory.STRING);
    case NUMERIC_GROUP:
      return TypeInfoUtils.implicitConvertible(a, b) ? b : a;
    case DATE_GROUP:
      return TypeInfoFactory.timestampTypeInfo;
    default:
      return null;
    }
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
      // It is not primitive; check if it is a struct and we can infer a common class
      if (a.getCategory() == Category.STRUCT && b.getCategory() == Category.STRUCT) {
        return getCommonClassForStruct((StructTypeInfo)a, (StructTypeInfo)b,
            (type1, type2) -> getCommonClassForComparison(type1, type2));
      }
      return null;
    }

    PrimitiveCategory pcA = ((PrimitiveTypeInfo)a).getPrimitiveCategory();
    PrimitiveCategory pcB = ((PrimitiveTypeInfo)b).getPrimitiveCategory();

    if (pcA == pcB) {
      // Same primitive category but different qualifiers.
      // Rely on getTypeInfoForPrimitiveCategory() to sort out the type params.
      return getTypeInfoForPrimitiveCategory((PrimitiveTypeInfo)a, (PrimitiveTypeInfo)b, pcA);
    }

    if (pcA == PrimitiveCategory.VOID) {
      // Handle NULL, we return the type of pcB
      return b;
    }
    if (pcB == PrimitiveCategory.VOID) {
      // Handle NULL, we return the type of pcA
      return a;
    }

    PrimitiveGrouping pgA = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(pcA);
    PrimitiveGrouping pgB = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(pcB);

    if (pgA == pgB) {
      // grouping is same, but category is not.
      if (pgA == PrimitiveGrouping.DATE_GROUP) {
        Integer ai = TypeInfoUtils.dateTypes.get(pcA);
        Integer bi = TypeInfoUtils.dateTypes.get(pcB);
        return (ai > bi) ? a : b;
      }
    }
    // handle string types properly
    if (pgA == PrimitiveGrouping.STRING_GROUP && pgB == PrimitiveGrouping.STRING_GROUP) {
      // Compare as strings. Char comparison semantics may be different if/when implemented.
      return getTypeInfoForPrimitiveCategory(
          (PrimitiveTypeInfo)a, (PrimitiveTypeInfo)b,PrimitiveCategory.STRING);
    }

    // timestamp/date is higher precedence than String_GROUP
    if (pgA == PrimitiveGrouping.STRING_GROUP && pgB == PrimitiveGrouping.DATE_GROUP) {
      return b;
    }
    // date/timestamp is higher precedence than String_GROUP
    if (pgB == PrimitiveGrouping.STRING_GROUP && pgA == PrimitiveGrouping.DATE_GROUP) {
      return a;
    }
    // Another special case, because timestamp is not implicitly convertible to numeric types.
    if ((pgA == PrimitiveGrouping.NUMERIC_GROUP || pgB == PrimitiveGrouping.NUMERIC_GROUP)
        && (pcA == PrimitiveCategory.TIMESTAMP || pcB == PrimitiveCategory.TIMESTAMP)) {
      return TypeInfoFactory.doubleTypeInfo;
    }

    for (PrimitiveCategory t : TypeInfoUtils.numericTypeList) {
      if (TypeInfoUtils.implicitConvertible(pcA, t)
          && TypeInfoUtils.implicitConvertible(pcB, t)) {
        return getTypeInfoForPrimitiveCategory((PrimitiveTypeInfo)a, (PrimitiveTypeInfo)b, t);
      }
    }

    return null;
  }

  public static PrimitiveCategory getPrimitiveCommonCategory(TypeInfo a, TypeInfo b) {
    if (a.getCategory() != Category.PRIMITIVE || b.getCategory() != Category.PRIMITIVE) {
      return null;
    }

    PrimitiveCategory pcA = ((PrimitiveTypeInfo)a).getPrimitiveCategory();
    PrimitiveCategory pcB = ((PrimitiveTypeInfo)b).getPrimitiveCategory();

    if (pcA == pcB) {
      // Same primitive category
      return pcA;
    }

    if (pcA == PrimitiveCategory.VOID) {
      // Handle NULL, we return the type of pcB
      return pcB;
    }
    if (pcB == PrimitiveCategory.VOID) {
      // Handle NULL, we return the type of pcA
      return pcA;
    }

    PrimitiveGrouping pgA = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(pcA);
    PrimitiveGrouping pgB = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(pcB);

    if (pgA == pgB) {
      // Equal groups, return what we can handle
      switch (pgA) {
        case NUMERIC_GROUP: {
          Integer ai = TypeInfoUtils.numericTypes.get(pcA);
          Integer bi = TypeInfoUtils.numericTypes.get(pcB);
          return (ai > bi) ? pcA : pcB;
        }
        case DATE_GROUP: {
          Integer ai = TypeInfoUtils.dateTypes.get(pcA);
          Integer bi = TypeInfoUtils.dateTypes.get(pcB);
          return (ai > bi) ? pcA : pcB;
        }
        case STRING_GROUP: {
          // handle string types properly
          return PrimitiveCategory.STRING;
        }
        default:
          break;
      }
    }

    // Handle date-string common category and numeric-string common category
    if (pgA == PrimitiveGrouping.STRING_GROUP
        && (pgB == PrimitiveGrouping.DATE_GROUP || pgB == PrimitiveGrouping.NUMERIC_GROUP)) {
      return pcA;
    }
    if (pgB == PrimitiveGrouping.STRING_GROUP
        && (pgA == PrimitiveGrouping.DATE_GROUP || pgA == PrimitiveGrouping.NUMERIC_GROUP)) {
      return pcB;
    }

    // We could not find a common category, return null
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

    // We try to infer a common primitive category
    PrimitiveCategory commonCat = getPrimitiveCommonCategory(a, b);
    if (commonCat != null) {
      return getTypeInfoForPrimitiveCategory((PrimitiveTypeInfo)a, (PrimitiveTypeInfo)b, commonCat);
    }
    // It is not primitive; check if it is a struct and we can infer a common class
    if (a.getCategory() == Category.STRUCT && b.getCategory() == Category.STRUCT) {
      return getCommonClassForStruct((StructTypeInfo)a, (StructTypeInfo)b,
          (type1, type2) -> getCommonClass(type1, type2));
    }
    return null;
  }

  /**
   * Find a common class that objects of both StructTypeInfo a and StructTypeInfo b can
   * convert to. This is used for places other than comparison.
   *
   * @return null if no common class could be found.
   */
  public static TypeInfo getCommonClassForStruct(StructTypeInfo a, StructTypeInfo b,
      BiFunction<TypeInfo, TypeInfo, TypeInfo> commonClassFunction) {
    if (a == b || a.equals(b)) {
      return a;
    }

    List<String> names = new ArrayList<String>();
    List<TypeInfo> typeInfos = new ArrayList<TypeInfo>();

    Iterator<String> namesIterator = a.getAllStructFieldNames().iterator();
    Iterator<String> otherNamesIterator = b.getAllStructFieldNames().iterator();

    // Compare the field names using ignore-case semantics
    while (namesIterator.hasNext() && otherNamesIterator.hasNext()) {
      String name = namesIterator.next();
      if (!name.equalsIgnoreCase(otherNamesIterator.next())) {
        return null;
      }
      names.add(name);
    }

    // Different number of field names
    if (namesIterator.hasNext() || otherNamesIterator.hasNext()) {
      return null;
    }

    // Compare the field types
    List<TypeInfo> fromTypes = a.getAllStructFieldTypeInfos();
    List<TypeInfo> toTypes = b.getAllStructFieldTypeInfos();
    for (int i = 0; i < fromTypes.size(); i++) {
      TypeInfo commonType = commonClassFunction.apply(fromTypes.get(i), toTypes.get(i));
      if (commonType == null) {
        return null;
      }
      typeInfos.add(commonType);
    }

    return TypeInfoFactory.getStructTypeInfo(names, typeInfos);
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
            args, false, isDistinct, isAllColumns);

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
      boolean isAllColumns, boolean respectNulls) throws SemanticException {
    Registry registry = SessionState.getRegistry();
    GenericUDAFEvaluator evaluator = registry == null ? null :
        registry.getGenericWindowingEvaluator(name, argumentOIs, isDistinct, isAllColumns, respectNulls);
    return evaluator != null ? evaluator :
        system.getGenericWindowingEvaluator(name, argumentOIs, isDistinct, isAllColumns, respectNulls);
  }

  public static GenericUDAFResolver getGenericUDAFResolver(String functionName)
      throws SemanticException {
    LOG.debug("Looking up GenericUDAF: {}", functionName);
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
      StringBuilder argumentString = new StringBuilder();
      if (arguments == null) {
        argumentString.append("null");
      } else {
        argumentString.append("{");
        for (int i = 0; i < arguments.length; i++) {
          if (i > 0) {
            argumentString.append(",");
          }

          argumentString.append(arguments[i]);
        }
        argumentString.append("}");
      }

      String detailedMsg = e instanceof java.lang.reflect.InvocationTargetException ?
        e.getCause().getMessage() : e.getMessage();

      // Log the arguments into a debug message for the ease of debugging. But when exposed through
      // an error message they can leak sensitive information, even to the client application.
      LOG.trace("Unable to execute method " + m + " with arguments "
              + argumentString);
      throw new HiveException("Unable to execute method " + m + ":" + detailedMsg, e);
    }
    return o;
  }

  /**
   * A shortcut to get the "index" GenericUDF. This is used for getting elements
   * out of array and getting values out of map.
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
      clonedUDF = new GenericUDFMacro(bridge.getMacroName(), bridge.getBody().clone(),
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
  private static Class<? extends GenericUDF> getGenericUDFClassFromExprDesc(ExprNodeDesc desc) {
    if (!(desc instanceof ExprNodeGenericFuncDesc)) {
      return null;
    }
    ExprNodeGenericFuncDesc genericFuncDesc = (ExprNodeGenericFuncDesc) desc;
    return genericFuncDesc.getGenericUDF().getClass();
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
   * Returns whether a GenericUDF is a runtime constant or not.
   */
  public static boolean isRuntimeConstant(GenericUDF genericUDF) {
    UDFType genericUDFType = AnnotationUtils.getAnnotation(genericUDF.getClass(), UDFType.class);
    if (genericUDFType != null && genericUDFType.runtimeConstant()) {
      return true;
    }

    if (genericUDF instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge) genericUDF;
      UDFType bridgeUDFType = AnnotationUtils.getAnnotation(bridge.getUdfClass(), UDFType.class);
      if (bridgeUDFType != null && bridgeUDFType.runtimeConstant()) {
        return true;
      }
    }

    if (genericUDF instanceof GenericUDFMacro) {
      GenericUDFMacro macro = (GenericUDFMacro) (genericUDF);
      return macro.isRuntimeConstant();
    }

    return false;
  }

  /**
   * Returns whether the expression, for a single query, returns the same result given
   * the same arguments/children. This includes deterministic functions as well as runtime
   * constants (which may not be deterministic across queries).
   */
  public static boolean isConsistentWithinQuery(GenericUDF genericUDF) {
    return (isDeterministic(genericUDF) || isRuntimeConstant(genericUDF)) && !isStateful(genericUDF);
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
   * Returns whether the exprNodeDesc is a node of "in".
   */
  public static boolean isIn(ExprNodeDesc desc) {
    return GenericUDFIn.class == getGenericUDFClassFromExprDesc(desc);
  }

  /**
   * Returns whether the exprNodeDesc is a node of "not".
   */
  public static boolean isOpNot(ExprNodeDesc desc) {
    return GenericUDFOPNot.class == getGenericUDFClassFromExprDesc(desc);
  }

  /**
   * Returns whether the fn is an exact equality comparison.
   */
  public static boolean isEq(GenericUDF fn) {
    return fn instanceof GenericUDFOPEqual;
  }

  /**
   * Returns whether the fn is an exact non-equality comparison.
   */
  public static boolean isNeq(GenericUDF fn) {
    return fn instanceof GenericUDFOPNotEqual;
  }

  /**
   * Returns whether the exprNodeDesc is a node of "positive".
   */
  public static boolean isOpPositive(ExprNodeDesc desc) {
    return GenericUDFOPPositive.class == getGenericUDFClassFromExprDesc(desc);
  }

  /**
   * Returns whether the exprNodeDesc is a node of "negative".
   */
  public static boolean isOpNegative(ExprNodeDesc desc) {
    return GenericUDFOPNegative.class == getGenericUDFClassFromExprDesc(desc);
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
        udfClass == UDFToShort.class || udfClass == GenericUDFToString.class ||
        udfClass == GenericUDFToVarchar.class || udfClass == GenericUDFToChar.class ||
        udfClass == GenericUDFTimestamp.class || udfClass == GenericUDFToBinary.class ||
        udfClass == GenericUDFToDate.class || udfClass == GenericUDFToDecimal.class ||
        udfClass == GenericUDFToTimestampLocalTZ.class;
  }

  /**
   * Returns whether the exprNodeDesc can recommend name for the expression
   */
  public static boolean isOpPreserveInputName(ExprNodeDesc desc) {
    return isOpCast(desc);
  }

  public static boolean isOpBetween(ExprNodeDesc desc) {
    return GenericUDFBetween.class == getGenericUDFClassFromExprDesc(desc);
  }

  public static boolean isOpInBloomFilter(ExprNodeDesc desc) {
    return GenericUDFInBloomFilter.class == getGenericUDFClassFromExprDesc(desc);
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
      String className, boolean registerToSession, FunctionResource[] resources) throws SemanticException {
    return system.registerPermanentFunction(functionName, className, registerToSession, resources);
  }

  public static boolean isPermanentFunction(ExprNodeGenericFuncDesc fnExpr) {
    GenericUDF udf = fnExpr.getGenericUDF();
    if (udf == null) {
      return false;
    }

    Class<?> clazz = udf.getClass();
    if (udf instanceof GenericUDFBridge) {
      clazz = ((GenericUDFBridge)udf).getUdfClass();
    }

    if (clazz != null) {
      // Use session registry - see Registry.isPermanentFunc()
      return SessionState.getRegistryForWrite().isPermanentFunc(clazz);
    }
    return false;
  }

  public static void unregisterPermanentFunction(String functionName) throws HiveException {
    system.unregisterFunction(functionName);
    unregisterTemporaryUDF(functionName);
  }

  /**
   * Unregisters all the functions under the database dbName
   * @param dbName specified database name
   * @throws HiveException
   */
  public static void unregisterPermanentFunctions(String dbName) throws HiveException {
    system.unregisterFunctions(dbName);
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
    GenericUDF udf = fnExpr.getGenericUDF();
    if (udf == null) {
      return false;
    }

    Class clazz = udf.getClass();
    if (udf instanceof GenericUDFBridge) {
      clazz = ((GenericUDFBridge)udf).getUdfClass();
    }

    if (clazz != null) {
      return system.isBuiltInFunc(clazz);
    }
    return false;
  }

  /** Unlike isBuiltInFuncExpr, does not expand GenericUdfBridge. */
  public static boolean isBuiltInFuncClass(Class<?> clazz) {
    return system.isBuiltInFunc(clazz);
  }

  /**
   * Setup blocked flag for all builtin UDFs as per udf whitelist and blacklist
   * @param whiteListStr
   * @param blackListStr
   */
  public static void setupPermissionsForBuiltinUDFs(String whiteListStr,
      String blackListStr) {
    system.setupPermissionsForUDFs(whiteListStr, blackListStr);
  }

  /**
   * Function to invert non-equi function texts
   * @param funcText
   */
  public static String invertFuncText(final String funcText) {
    // Reverse the text
    switch (funcText) {
      case "<":
        return ">";
      case "<=":
        return ">=";
      case ">":
        return "<";
      case ">=":
        return "<=";
      default:
        return null; // helps identify unsupported functions
    }
  }

  public static boolean isOrderedAggregate(String functionName) throws SemanticException {
    WindowFunctionInfo windowInfo = getWindowFunctionInfo(functionName);
    if (windowInfo != null) {
      return windowInfo.isOrderedAggregate();
    }
    return false;
  }
}
