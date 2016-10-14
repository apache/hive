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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class UnitTestBatch extends TestBatch {

  private final String testCasePropertyName;
  private final List<String> testList;
  private final boolean isParallel;
  private final String moduleName;
  private final String batchName;

  public UnitTestBatch(AtomicInteger batchIdCounter, String testCasePropertyName,
                       List<String> tests, String moduleName, boolean isParallel) {
    super(batchIdCounter);
    Preconditions.checkNotNull(testCasePropertyName);
    Preconditions.checkArgument(tests!= null && !tests.isEmpty());
    this.testCasePropertyName = testCasePropertyName;
    this.testList = tests;
    this.isParallel = isParallel;
    this.moduleName = moduleName;
    if (tests.size() == 1) {
      batchName = String.format("%d_%s", getBatchId(), tests.get(0));
    } else {
      batchName = String.format("%d_UTBatch_%s_%d_tests", getBatchId(),
          (moduleName.replace("/", "__").replace(".", "__")), tests.size());
    }
  }
  @Override
  public String getTestArguments() {
    String testArg = Joiner.on(",").join(testList);
    return String.format("-D%s=%s", testCasePropertyName, testArg);
  }

  @Override
  public String getName() {
    // Used for logDir, failure messages etc.
    return batchName;
  }

  @Override
  public String toString() {
    return "UnitTestBatch [name=" + batchName + ", id=" + getBatchId() + ", moduleName=" +
        moduleName +", batchSize=" + testList.size() +
        ", isParallel=" + isParallel + ", testList=" + testList + "]";
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
    return testList.size();
  }

  @Override
  public Collection<String> getTestClasses() {
    return testList;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    UnitTestBatch that = (UnitTestBatch) o;

    if (isParallel != that.isParallel) {
      return false;
    }
    if (testList != null ? !testList.equals(that.testList) : that.testList != null) {
      return false;
    }
    if (moduleName != null ? !moduleName.equals(that.moduleName) : that.moduleName != null) {
      return false;
    }
    return batchName != null ? batchName.equals(that.batchName) : that.batchName == null;

  }

  @Override
  public int hashCode() {
    int result = testList != null ? testList.hashCode() : 0;
    result = 31 * result + (isParallel ? 1 : 0);
    result = 31 * result + (moduleName != null ? moduleName.hashCode() : 0);
    result = 31 * result + (batchName != null ? batchName.hashCode() : 0);
    return result;
  }
}
