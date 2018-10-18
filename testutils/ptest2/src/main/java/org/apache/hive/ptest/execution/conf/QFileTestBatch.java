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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

public class QFileTestBatch extends TestBatch {

  private final String testCasePropertyName;
  private final String driver;
  private final String queryFilesProperty;
  private final String name;
  private final String moduleName;
  private final List<String> tests;
  private final boolean isParallel;

  public QFileTestBatch(AtomicInteger batchIdCounter, String testCasePropertyName, String driver,
                        String queryFilesProperty, Set<String> tests, boolean isParallel,
                        String moduleName) {
    super(batchIdCounter);
    this.testCasePropertyName = testCasePropertyName;
    this.driver = driver;
    this.queryFilesProperty = queryFilesProperty;
    // Store as a list to have a consistent order between getTests, and the test argument generation.
    this.tests = Lists.newArrayList(tests);
    String name = Joiner.on("-").join(getBatchId(), driver, Joiner.on("-").join(
        Iterators.toArray(Iterators.limit(tests.iterator(), 3), String.class)));
    if(tests.size() > 3) {
      name = Joiner.on("-").join(name, "and", (tests.size() - 3), "more");
    }
    this.name = name;
    this.isParallel = isParallel;
    this.moduleName = moduleName;
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
    return String.format("-D%s=%s -D%s=%s", testCasePropertyName, driver, queryFilesProperty,
        Joiner.on(",").join(tests));
  }

  public Collection<String> getTests() {
    return Collections.unmodifiableList(tests);
  }

  @Override
  public String toString() {
    return "QFileTestBatch [batchId=" + getBatchId() + ", size=" + tests.size() + ", driver=" +
        driver + ", queryFilesProperty="
        + queryFilesProperty + ", name=" + name + ", tests=" + tests
        + ", isParallel=" + isParallel + ", moduleName=" + moduleName + "]";
  }
  @Override
  public boolean isParallel() {
    return isParallel;
  }

  @Override
  public String getTestModuleRelativeDir() {
    return moduleName;
  }

  @Override
  public int getNumTestsInBatch() {
    return tests.size();
  }

  @Override
  public Collection<String> getTestClasses() {
    return Collections.singleton(driver);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((driver == null) ? 0 : driver.hashCode());
    result = prime * result + (isParallel ? 1231 : 1237);
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result
        + ((queryFilesProperty == null) ? 0 : queryFilesProperty.hashCode());
    result = prime * result + ((tests == null) ? 0 : tests.hashCode());
    return result;
  }
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    QFileTestBatch other = (QFileTestBatch) obj;
    if (driver == null) {
      if (other.driver != null)
        return false;
    } else if (!driver.equals(other.driver))
      return false;
    if (isParallel != other.isParallel)
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (queryFilesProperty == null) {
      if (other.queryFilesProperty != null)
        return false;
    } else if (!queryFilesProperty.equals(other.queryFilesProperty))
      return false;
    if (tests == null) {
      if (other.tests != null)
        return false;
    } else if (!tests.equals(other.tests))
      return false;
    return true;
  }
}
