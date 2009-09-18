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



public class UDAFSum extends NumericUDAF {

  public static class UDAFSumEvaluator implements UDAFEvaluator { 
    private double mSum;
    private boolean mEmpty;
    
    public UDAFSumEvaluator() {
      super();
      init();
    }
  
    public void init() {
      mSum = 0;
      mEmpty = true;
    }
  
    public boolean iterate(DoubleWritable o) {
      if (o != null) {
        mSum += o.get();
        mEmpty = false;
      }
      return true;
    }
    
    public DoubleWritable terminatePartial() {
      // This is SQL standard - sum of zero items should be null.
      return mEmpty ? null : new DoubleWritable(mSum);
    }
  
    public boolean merge(DoubleWritable o) {
      if (o != null) {
        mSum += o.get();
        mEmpty = false;
      }
      return true;
    }
  
    public DoubleWritable terminate() {
      // This is SQL standard - sum of zero items should be null.
      return mEmpty ? null : new DoubleWritable(mSum);
    }
  }
  
}
