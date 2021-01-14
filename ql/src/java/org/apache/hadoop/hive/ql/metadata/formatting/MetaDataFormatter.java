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
import java.io.OutputStream;

import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Interface to format table and index information.  We can format it
 * for human readability (lines of text) or for machine readability
 * (json).
 */
public interface MetaDataFormatter {
  /**
   * Write an error message.
   * 
   * @param sqlState if {@code null}, will be ignored
   */
  void error(OutputStream out, String msg, int errorCode, String sqlState) throws HiveException;

  /**
   * @param sqlState if {@code null}, will be skipped in output
   * @param errorDetail usually string version of some Exception, if {@code null}, will be ignored
   */
  void error(OutputStream out, String errorMessage, int errorCode, String sqlState, String errorDetail)
      throws HiveException;

  void showErrors(DataOutputStream out, WMValidateResourcePlanResponse errors) throws HiveException;
}
