/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.execution.conf;

import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;

public class QFileTestBatch implements TestBatch {

  private final String driver;
  private final String queryFilesProperty;
  private final String name;
  private final Set<String> tests;
  private final boolean isParallel;
  public QFileTestBatch(String driver, String queryFilesProperty,
      Set<String> tests, boolean isParallel) {
    this.driver = driver;
    this.queryFilesProperty = queryFilesProperty;
    this.tests = tests;
    String name = Joiner.on("-").join(driver, Joiner.on("-").join(
        Iterators.toArray(Iterators.limit(tests.iterator(), 3), String.class)));
    if(tests.size() > 3) {
      name = Joiner.on("-").join(name, "and", (tests.size() - 3), "more");
    }
    this.name = name;
    this.isParallel = isParallel;
  }
  public String getDriver() {
    return driver;
  }
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getTestArguments() {
    return String.format("-Dtestcase=%s -D%s=%s", driver, queryFilesProperty,
        Joiner.on(",").join(tests));
  }

  @Override
  public String toString() {
    return "QFileTestBatch [driver=" + driver + ", queryFilesProperty="
        + queryFilesProperty + ", name=" + name + ", tests=" + tests
        + ", isParallel=" + isParallel + "]";
  }
  @Override
  public boolean isParallel() {
    return isParallel;
  }
}
