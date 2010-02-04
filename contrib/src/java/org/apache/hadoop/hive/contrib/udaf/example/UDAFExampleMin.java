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

package org.apache.hadoop.hive.contrib.udaf.example;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@Description(name = "example_min", value = "_FUNC_(expr) - Returns the minimum value of expr")
public class UDAFExampleMin extends UDAF {

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

    public boolean iterate(ShortWritable o) {
      if (o != null) {
        if (mEmpty) {
          mMin = o.get();
          mEmpty = false;
        } else {
          mMin = (short) Math.min(mMin, o.get());
        }
      }
      return true;
    }

    public ShortWritable terminatePartial() {
      return mEmpty ? null : new ShortWritable(mMin);
    }

    public boolean merge(ShortWritable o) {
      return iterate(o);
    }

    public ShortWritable terminate() {
      return mEmpty ? null : new ShortWritable(mMin);
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

    public boolean iterate(IntWritable o) {
      if (o != null) {
        if (mEmpty) {
          mMin = o.get();
          mEmpty = false;
        } else {
          mMin = Math.min(mMin, o.get());
        }
      }
      return true;
    }

    public IntWritable terminatePartial() {
      return mEmpty ? null : new IntWritable(mMin);
    }

    public boolean merge(IntWritable o) {
      return iterate(o);
    }

    public IntWritable terminate() {
      return mEmpty ? null : new IntWritable(mMin);
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

    public boolean iterate(LongWritable o) {
      if (o != null) {
        if (mEmpty) {
          mMin = o.get();
          mEmpty = false;
        } else {
          mMin = Math.min(mMin, o.get());
        }
      }
      return true;
    }

    public LongWritable terminatePartial() {
      return mEmpty ? null : new LongWritable(mMin);
    }

    public boolean merge(LongWritable o) {
      return iterate(o);
    }

    public LongWritable terminate() {
      return mEmpty ? null : new LongWritable(mMin);
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

    public boolean iterate(FloatWritable o) {
      if (o != null) {
        if (mEmpty) {
          mMin = o.get();
          mEmpty = false;
        } else {
          mMin = Math.min(mMin, o.get());
        }
      }
      return true;
    }

    public FloatWritable terminatePartial() {
      return mEmpty ? null : new FloatWritable(mMin);
    }

    public boolean merge(FloatWritable o) {
      return iterate(o);
    }

    public FloatWritable terminate() {
      return mEmpty ? null : new FloatWritable(mMin);
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

    public boolean iterate(DoubleWritable o) {
      if (o != null) {
        if (mEmpty) {
          mMin = o.get();
          mEmpty = false;
        } else {
          mMin = Math.min(mMin, o.get());
        }
      }
      return true;
    }

    public DoubleWritable terminatePartial() {
      return mEmpty ? null : new DoubleWritable(mMin);
    }

    public boolean merge(DoubleWritable o) {
      return iterate(o);
    }

    public DoubleWritable terminate() {
      return mEmpty ? null : new DoubleWritable(mMin);
    }
  }

  static public class MinStringEvaluator implements UDAFEvaluator {
    private Text mMin;
    private boolean mEmpty;

    public MinStringEvaluator() {
      super();
      init();
    }

    public void init() {
      mMin = null;
      mEmpty = true;
    }

    public boolean iterate(Text o) {
      if (o != null) {
        if (mEmpty) {
          mMin = new Text(o);
          mEmpty = false;
        } else if (ShimLoader.getHadoopShims().compareText(mMin, o) > 0) {
          mMin.set(o);
        }
      }
      return true;
    }

    public Text terminatePartial() {
      return mEmpty ? null : mMin;
    }

    public boolean merge(Text o) {
      return iterate(o);
    }

    public Text terminate() {
      return mEmpty ? null : mMin;
    }
  }
}
