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

import org.apache.hadoop.hive.ql.exec.NumericUDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.Text;



public class UDAFAvg extends NumericUDAF {

  public static class UDAFAvgEvaluator implements UDAFEvaluator {
    private long mCount;
    private double mSum;
    
    public UDAFAvgEvaluator() {
      super();
      init();
    }
  
    public void init() {
      mSum = 0;
      mCount = 0;
    }
  
    public boolean iterate(DoubleWritable o) {
      if (o != null) {
        mSum += o.get();
        mCount ++;
      }
      return true;
    }
    
    public Text terminatePartial() {
      // This is SQL standard - average of zero items should be null.
      return mCount == 0 ? null : new Text(String.valueOf(mSum) + '/' + String.valueOf(mCount));
    }
  
    public boolean merge(Text o) {
      if (o != null) {
        String s = o.toString();
        int pos = s.indexOf('/');
        assert(pos != -1);
        mSum += Double.parseDouble(s.substring(0, pos));
        mCount += Long.parseLong(s.substring(pos+1));
      }
      return true;
    }
  
    public DoubleWritable terminate() {
      // This is SQL standard - average of zero items should be null.
      return mCount == 0 ? null : new DoubleWritable(mSum / mCount);
    }
  }
  
}
