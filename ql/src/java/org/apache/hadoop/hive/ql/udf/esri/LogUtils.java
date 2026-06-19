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
package org.apache.hadoop.hive.ql.udf.esri;

import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;

public class LogUtils {

  private static final int MSG_SRID_MISMATCH = 0;
  private static final int MSG_ARGUMENTS_NULL = 1;
  private static final int MSG_ARGUMENT_LENGTH_XY = 2;
  private static final int MSG_MULTI_ARGUMENT_LENGTH_XY = 3;
  private static final int MSG_INVALID_TYPE = 4;
  private static final int MSG_INVALID_TEXT = 5;
  private static final int MSG_INVALID_INDEX = 6;
  private static final int MSG_INTERNAL_ERROR = 7;
  private static final int MSG_ARGUMENT_LENGTH = 8;
  private static final int MSG_EXCEPTION_THROWN = 9;
  private static final int MSG_NOT_3D = 10;
  private static final int MSG_NOT_MEASURED = 11;

  private static final String[] messages =
      { "Mismatched spatial references ('%d' <> '%d')", "Invalid arguments - one or more arguments are null.",
          "Invalid arguments.  Expecting one or more x,y pairs.",
          "Invalid arguments.  Expecting one or more x,y pairs in array argument %d.",
          "Invalid geometry type.  Expecting %s but found %s", "Invalid arguments.  Ill-formed text: %s ....",
          "Invalid index.  Expected range [%d, %d], actual index %d.", "Internal error - %s.",
          "Invalid arguments.  Expecting one or more arguments.", "Exception thrown by %s", "Invalid argument - not 3D",
          "Invalid argument - not measured" };

  /**
   * Log when comparing geometries in different spatial references
   *
   * @param logger
   * @param geomref1
   * @param geomref2
   */
  public static void Log_SRIDMismatch(Logger logger, BytesWritable geomref1, BytesWritable geomref2) {
    logger.error(
        String.format(messages[MSG_SRID_MISMATCH], GeometryUtils.getWKID(geomref1), GeometryUtils.getWKID(geomref2)));
  }

  public static void Log_SRIDMismatch(Logger logger, BytesWritable geomref1, int wkid2) {
    logger.error(String.format(messages[MSG_SRID_MISMATCH], GeometryUtils.getWKID(geomref1), wkid2));
  }

  /**
   * Log when arguments passed to evaluate are null
   * @param logger
   */
  public static void Log_ArgumentsNull(Logger logger) {
    logger.error(messages[MSG_ARGUMENTS_NULL]);
  }

  public static void Log_VariableArgumentLengthXY(Logger logger) {
    logger.error(messages[MSG_ARGUMENT_LENGTH_XY]);
  }

  public static void Log_VariableArgumentLengthXY(Logger logger, int array_argument_index) {
    logger.error(String.format(messages[MSG_MULTI_ARGUMENT_LENGTH_XY], array_argument_index));
  }

  public static void Log_InvalidType(Logger logger, GeometryUtils.OGCType expecting, GeometryUtils.OGCType actual) {
    logger.error(String.format(messages[MSG_INVALID_TYPE], expecting, actual));
  }

  public static void Log_InvalidText(Logger logger, String text) {
    int limit = text.length();
    limit = limit > 80 ? 80 : limit;
    logger.error(String.format(messages[MSG_INVALID_TEXT], text.substring(0, limit)));
  }

  public static void Log_InvalidIndex(Logger logger, int actual, int expMin, int expMax) {
    logger.error(String.format(messages[MSG_INVALID_INDEX], expMin, expMax, actual));
  }

  public static void Log_InternalError(Logger logger, String text) {
    logger.error(String.format(messages[MSG_INTERNAL_ERROR], text));
  }

  public static void Log_VariableArgumentLength(Logger logger) {
    logger.error(messages[MSG_ARGUMENT_LENGTH]);
  }

  public static void Log_ExceptionThrown(Logger logger, String method, Exception e) {
    logger.error(String.format(messages[MSG_EXCEPTION_THROWN], method), e);
  }

  public static void Log_Not3D(Logger logger) {
    logger.error(messages[MSG_NOT_3D]);
  }

  public static void Log_NotMeasured(Logger logger) {
    logger.error(messages[MSG_NOT_MEASURED]);
  }

}
