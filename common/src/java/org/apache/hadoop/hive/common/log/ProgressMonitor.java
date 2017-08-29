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
package org.apache.hadoop.hive.common.log;

import java.util.Collections;
import java.util.List;

public interface ProgressMonitor {

  ProgressMonitor NULL = new ProgressMonitor() {
    @Override
    public List<String> headers() {
      return Collections.emptyList();
    }

    @Override
    public List<List<String>> rows() {
      return Collections.emptyList();
    }

    @Override
    public String footerSummary() {
      return "";
    }

    @Override
    public long startTime() {
      return 0;
    }

    @Override
    public String executionStatus() {
      return "";
    }

    @Override
    public double progressedPercentage() {
      return 0;
    }
  };

  List<String> headers();

  List<List<String>> rows();

  String footerSummary();

  long startTime();

  String executionStatus();

  double progressedPercentage();
}
