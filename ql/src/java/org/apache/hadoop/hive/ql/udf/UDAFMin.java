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


public class UDAFMin extends UDAF {

  static public class MinShortEvaluator implements UDAFEvaluator {
    private short mMin;
    private boolean mEmpty;

    public MinShortEvaluator() {
      super();
      init();
    }

    public void init() {
      mMin = 0;
      mEmpty = true;
    }

    public boolean iterate(Short o) {
      if (o != null) {
        if (mEmpty) {
          mMin = o;
          mEmpty = false;
        } else {
          mMin = (short) Math.min(mMin, o);
        }
      }
      return true;
    }

    public Short terminatePartial() {
      return mEmpty ? null : Short.valueOf(mMin);
    }

    public boolean merge(Short o) {
      return iterate(o);
    }

    public Short terminate() {
      return mEmpty ? null : Short.valueOf(mMin);
    }
  }

  static public class MinIntEvaluator implements UDAFEvaluator {
    private int mMin;
    private boolean mEmpty;

    public MinIntEvaluator() {
      super();
      init();
    }

    public void init() {
      mMin = 0;
      mEmpty = true;
    }

    public boolean iterate(Integer o) {
      if (o != null) {
        if (mEmpty) {
          mMin = o;
          mEmpty = false;
        } else {
          mMin = Math.min(mMin, o);
        }
      }
      return true;
    }

    public Integer terminatePartial() {
      return mEmpty ? null : Integer.valueOf(mMin);
    }

    public boolean merge(Integer o) {
      return iterate(o);
    }

    public Integer terminate() {
      return mEmpty ? null : Integer.valueOf(mMin);
    }
  }

  static public class MinLongEvaluator implements UDAFEvaluator {
    private long mMin;
    private boolean mEmpty;

    public MinLongEvaluator() {
      super();
      init();
    }

    public void init() {
      mMin = 0;
      mEmpty = true;
    }

    public boolean iterate(Long o) {
      if (o != null) {
        if (mEmpty) {
          mMin = o;
          mEmpty = false;
        } else {
          mMin = Math.min(mMin, o);
        }
      }
      return true;
    }

    public Long terminatePartial() {
      return mEmpty ? null : Long.valueOf(mMin);
    }

    public boolean merge(Long o) {
      return iterate(o);
    }

    public Long terminate() {
      return mEmpty ? null : Long.valueOf(mMin);
    }
  }

  static public class MinFloatEvaluator implements UDAFEvaluator {
    private float mMin;
    private boolean mEmpty;

    public MinFloatEvaluator() {
      super();
      init();
    }

    public void init() {
      mMin = 0;
      mEmpty = true;
    }

    public boolean iterate(Float o) {
      if (o != null) {
        if (mEmpty) {
          mMin = o;
          mEmpty = false;
        } else {
          mMin = Math.min(mMin, o);
        }
      }
      return true;
    }

    public Float terminatePartial() {
      return mEmpty ? null : Float.valueOf(mMin);
    }

    public boolean merge(Float o) {
      return iterate(o);
    }

    public Float terminate() {
      return mEmpty ? null : Float.valueOf(mMin);
    }
  }

  static public class MinDoubleEvaluator implements UDAFEvaluator {
    private double mMin;
    private boolean mEmpty;

    public MinDoubleEvaluator() {
      super();
      init();
    }

    public void init() {
      mMin = 0;
      mEmpty = true;
    }

    public boolean iterate(Double o) {
      if (o != null) {
        if (mEmpty) {
          mMin = o;
          mEmpty = false;
        } else {
          mMin = Math.min(mMin, o);
        }
      }
      return true;
    }

    public Double terminatePartial() {
      return mEmpty ? null : Double.valueOf(mMin);
    }

    public boolean merge(Double o) {
      return iterate(o);
    }

    public Double terminate() {
      return mEmpty ? null : Double.valueOf(mMin);
    }
  }

  static public class MinStringEvaluator implements UDAFEvaluator {
    private String mMin;
    private boolean mEmpty;

    public MinStringEvaluator() {
      super();
      init();
    }

    public void init() {
      mMin = null;
      mEmpty = true;
    }

    public boolean iterate(String o) {
      if (o != null) {
        if (mEmpty) {
          mMin = o;
          mEmpty = false;
        } else if (mMin.compareTo(o) > 0) {
          mMin = o;
        }
      }
      return true;
    }

    public String terminatePartial() {
      return mEmpty ? null : mMin;
    }

    public boolean merge(String o) {
      return iterate(o);
    }

    public String terminate() {
      return mEmpty ? null : mMin;
    }
  }

  static public class MinDateEvaluator implements UDAFEvaluator {
    private Date mMin;
    private boolean mEmpty;

    public MinDateEvaluator() {
      super();
      init();
    }

    public void init() {
      mMin = null;
      mEmpty = true;
    }

    public boolean iterate(Date o) {
      if (o != null) {
        if (mEmpty) {
          mMin = o;
          mEmpty = false;
        } else if (mMin.compareTo(o) > 0){
          mMin = o;
        }
      }
      return true;
    }

    public Date terminatePartial() {
      return mEmpty ? null : mMin;
    }

    public boolean merge(Date o) {
      return iterate(o);
    }

    public Date terminate() {
      return mEmpty ? null : mMin;
    }
  }
}
