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

import java.sql.Date;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;



public class UDAFMax extends UDAF {

  static public class MaxShortEvaluator implements UDAFEvaluator {
    private short mMax;
    private boolean mEmpty;

    public MaxShortEvaluator() {
      super();
      init();
    }

    public void init() {
      mMax = 0;
      mEmpty = true;
    }

    public boolean iterate(Short o) {
      if (o != null) {
        if (mEmpty) {
          mMax = o;
          mEmpty = false;
        } else {
          mMax = (short) Math.max(mMax, o);
        }
      }
      return true;
    }

    public Short terminatePartial() {
      return mEmpty ? null : Short.valueOf(mMax);
    }

    public boolean merge(Short o) {
      return iterate(o);
    }

    public Short terminate() {
      return mEmpty ? null : Short.valueOf(mMax);
    }
  }

  static public class MaxIntEvaluator implements UDAFEvaluator {
    private int mMax;
    private boolean mEmpty;

    public MaxIntEvaluator() {
      super();
      init();
    }

    public void init() {
      mMax = 0;
      mEmpty = true;
    }

    public boolean iterate(Integer o) {
      if (o != null) {
        if (mEmpty) {
          mMax = o;
          mEmpty = false;
        } else {
          mMax = Math.max(mMax, o);
        }
      }
      return true;
    }

    public Integer terminatePartial() {
      return mEmpty ? null : Integer.valueOf(mMax);
    }

    public boolean merge(Integer o) {
      return iterate(o);
    }

    public Integer terminate() {
      return mEmpty ? null : Integer.valueOf(mMax);
    }
  }

  static public class MaxLongEvaluator implements UDAFEvaluator {
    private long mMax;
    private boolean mEmpty;

    public MaxLongEvaluator() {
      super();
      init();
    }

    public void init() {
      mMax = 0;
      mEmpty = true;
    }

    public boolean iterate(Long o) {
      if (o != null) {
        if (mEmpty) {
          mMax = o;
          mEmpty = false;
        } else {
          mMax = Math.max(mMax, o);
        }
      }
      return true;
    }

    public Long terminatePartial() {
      return mEmpty ? null : Long.valueOf(mMax);
    }

    public boolean merge(Long o) {
      return iterate(o);
    }

    public Long terminate() {
      return mEmpty ? null : Long.valueOf(mMax);
    }
  }

  static public class MaxFloatEvaluator implements UDAFEvaluator {
    private float mMax;
    private boolean mEmpty;

    public MaxFloatEvaluator() {
      super();
      init();
    }

    public void init() {
      mMax = 0;
      mEmpty = true;
    }

    public boolean iterate(Float o) {
      if (o != null) {
        if (mEmpty) {
          mMax = o;
          mEmpty = false;
        } else {
          mMax = Math.max(mMax, o);
        }
      }
      return true;
    }

    public Float terminatePartial() {
      return mEmpty ? null : Float.valueOf(mMax);
    }

    public boolean merge(Float o) {
      return iterate(o);
    }

    public Float terminate() {
      return mEmpty ? null : Float.valueOf(mMax);
    }
  }

  static public class MaxDoubleEvaluator implements UDAFEvaluator {
    private double mMax;
    private boolean mEmpty;

    public MaxDoubleEvaluator() {
      super();
      init();
    }

    public void init() {
      mMax = 0;
      mEmpty = true;
    }

    public boolean iterate(Double o) {
      if (o != null) {
        if (mEmpty) {
          mMax = o;
          mEmpty = false;
        } else {
          mMax = Math.max(mMax, o);
        }
      }
      return true;
    }

    public Double terminatePartial() {
      return mEmpty ? null : Double.valueOf(mMax);
    }

    public boolean merge(Double o) {
      return iterate(o);
    }

    public Double terminate() {
      return mEmpty ? null : Double.valueOf(mMax);
    }
  }

  static public class MaxStringEvaluator implements UDAFEvaluator {
    private String mMax;
    private boolean mEmpty;

    public MaxStringEvaluator() {
      super();
      init();
    }

    public void init() {
      mMax = null;
      mEmpty = true;
    }

    public boolean iterate(String o) {
      if (o != null) {
        if (mEmpty) {
          mMax = o;
          mEmpty = false;
        } else if (mMax.compareTo(o) < 0) {
          mMax = o;
        }
      }
      return true;
    }

    public String terminatePartial() {
      return mEmpty ? null : mMax;
    }

    public boolean merge(String o) {
      return iterate(o);
    }

    public String terminate() {
      return mEmpty ? null : mMax;
    }
  }

  static public class MaxDateEvaluator implements UDAFEvaluator {
    private Date mMax;
    private boolean mEmpty;

    public MaxDateEvaluator() {
      super();
      init();
    }

    public void init() {
      mMax = null;
      mEmpty = true;
    }

    public boolean iterate(Date o) {
      if (o != null) {
        if (mEmpty) {
          mMax = o;
          mEmpty = false;
        } else if (mMax.compareTo(o) < 0){
          mMax = o;
        }
      }
      return true;
    }

    public Date terminatePartial() {
      return mEmpty ? null : mMax;
    }

    public boolean merge(Date o) {
      return iterate(o);
    }

    public Date terminate() {
      return mEmpty ? null : mMax;
    }
  }

}
