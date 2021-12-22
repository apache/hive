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

package org.apache.hadoop.hive.ql.metadata.formatting;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Format table and index information for human readability using
 * simple lines of text.
 */
class TextMetaDataFormatter implements MetaDataFormatter {
  private static final int TERMINATOR = Utilities.newLineCode;

  /**
   * Write an error message.
   */
  @Override
  public void error(OutputStream out, String msg, int errorCode, String sqlState) throws HiveException {
    error(out, msg, errorCode, sqlState, null);
  }

  @Override
  public void error(OutputStream out, String errorMessage, int errorCode, String sqlState, String errorDetail)
      throws HiveException {
    try {
      out.write(errorMessage.getBytes("UTF-8"));
      if (errorDetail != null) {
        out.write(errorDetail.getBytes("UTF-8"));
      }
      out.write(errorCode);
      if (sqlState != null) {
        out.write(sqlState.getBytes("UTF-8")); //this breaks all the tests in .q files
      }
      out.write(TERMINATOR);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private static byte[] str(String str) {
    return str.getBytes(UTF_8);
  }

  private static void write(DataOutputStream out, String val) throws IOException {
    out.write(str(val));
  }

  public void showErrors(DataOutputStream out, WMValidateResourcePlanResponse response) throws HiveException {
    try {
      for (String error : response.getErrors()) {
        write(out, error);
        out.write(TERMINATOR);
      }
      for (String warning : response.getWarnings()) {
        write(out, "warn: ");
        write(out, warning);
        out.write(TERMINATOR);
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }
}
