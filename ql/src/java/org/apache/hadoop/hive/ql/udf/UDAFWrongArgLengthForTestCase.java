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

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * UDAFWrongArgLengthForTestCase.
 *
 */
public class UDAFWrongArgLengthForTestCase extends UDAF {

  /**
   * UDAFWrongArgLengthForTestCaseEvaluator.
   *
   */
  public static class UDAFWrongArgLengthForTestCaseEvaluator implements
      UDAFEvaluator {

    private long mCount;

    public UDAFWrongArgLengthForTestCaseEvaluator() {
      super();
      init();
    }

    public void init() {
      mCount = 0;
    }

    Text emptyText = new Text();

    public boolean iterate(Object o) {
      if (o != null && !emptyText.equals(o)) {
        mCount++;
      }
      return true;
    }

    public LongWritable terminatePartial() {
      return new LongWritable(mCount);
    }

    public boolean merge() {
      return true;
    }

    public LongWritable terminate() {
      return new LongWritable(mCount);
    }
  }

}
