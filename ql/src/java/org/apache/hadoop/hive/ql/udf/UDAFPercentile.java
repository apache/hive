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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * UDAF for calculating the percentile values.
 * There are several definitions of percentile, and we take the method recommended by
 * NIST.
 * @see <a href="http://en.wikipedia.org/wiki/Percentile#Alternative_methods">
 *      Percentile references</a>
 */
@Description(name = "percentile",
    value = "_FUNC_(expr, pc) - Returns the percentile(s) of expr at pc (range: [0,1])."
      + "pc can be a double or double array")
public class UDAFPercentile extends UDAF {

  /**
   * A state class to store intermediate aggregation results.
   */
  public static class State {
    private Map<LongWritable, LongWritable> counts;
    private List<DoubleWritable> percentiles;
  }

  /**
   * A comparator to sort the entries in order.
   */
  public static class MyComparator implements Comparator<Map.Entry<LongWritable, LongWritable>> {
    @Override
    public int compare(Map.Entry<LongWritable, LongWritable> o1,
        Map.Entry<LongWritable, LongWritable> o2) {
      return o1.getKey().compareTo(o2.getKey());
    }
  }

  /**
   * Increment the State object with o as the key, and i as the count.
   */
  private static void increment(State s, LongWritable o, long i) {
    if (s.counts == null) {
      s.counts = new HashMap<LongWritable, LongWritable>();
    }
    LongWritable count = s.counts.get(o);
    if (count == null) {
      // We have to create a new object, because the object o belongs
      // to the code that creates it and may get its value changed.
      LongWritable key = new LongWritable();
      key.set(o.get());
      s.counts.put(key, new LongWritable(i));
    } else {
      count.set(count.get() + i);
    }
  }

  /**
   * Get the percentile value.
   */
  private static double getPercentile(List<Map.Entry<LongWritable, LongWritable>> entriesList,
      double position) {
    // We may need to do linear interpolation to get the exact percentile
    long lower = (long)Math.floor(position);
    long higher = (long)Math.ceil(position);

    // Linear search since this won't take much time from the total execution anyway
    // lower has the range of [0 .. total-1]
    // The first entry with accumulated count (lower+1) corresponds to the lower position.
    int i = 0;
    while (entriesList.get(i).getValue().get() < lower + 1) {
      i++;
    }

    long lowerKey = entriesList.get(i).getKey().get();
    if (higher == lower) {
      // no interpolation needed because position does not have a fraction
      return lowerKey;
    }

    if (entriesList.get(i).getValue().get() < higher + 1) {
      i++;
    }
    long higherKey = entriesList.get(i).getKey().get();

    if (higherKey == lowerKey) {
      // no interpolation needed because lower position and higher position has the same key
      return lowerKey;
    }

    // Linear interpolation to get the exact percentile
    return (higher - position) * lowerKey + (position - lower) * higherKey;
  }


  /**
   * The evaluator for percentile computation based on long.
   */
  public static class PercentileLongEvaluator implements UDAFEvaluator {

    private final State state;

    public PercentileLongEvaluator() {
      state = new State();
    }

    public void init() {
      if (state.counts != null) {
        // We reuse the same hashmap to reduce new object allocation.
        // This means counts can be empty when there is no input data.
        state.counts.clear();
      }
    }

    /** Note that percentile can be null in a global aggregation with
     *  0 input rows:  "select percentile(col, 0.5) from t where false"
     *  In that case, iterate(null, null) will be called once.
     */
    public boolean iterate(LongWritable o, Double percentile) {
      if (o == null && percentile == null) {
        return false;
      }
      if (state.percentiles == null) {
        if (percentile < 0.0 || percentile > 1.0) {
          throw new RuntimeException("Percentile value must be wihin the range of 0 to 1.");
        }
        state.percentiles = new ArrayList<DoubleWritable>(1);
        state.percentiles.add(new DoubleWritable(percentile.doubleValue()));
      }
      if (o != null) {
        increment(state, o, 1);
      }
      return true;
    }

    public State terminatePartial() {
      return state;
    }

    public boolean merge(State other) {
      if (other == null || other.counts == null || other.percentiles == null) {
        return false;
      }

      if (state.percentiles == null) {
        state.percentiles = new ArrayList<DoubleWritable>(other.percentiles);
      }

      for (Map.Entry<LongWritable, LongWritable> e: other.counts.entrySet()) {
        increment(state, e.getKey(), e.getValue().get());
      }
      return true;
    }

    private DoubleWritable result;

    public DoubleWritable terminate() {
      // No input data.
      if (state.counts == null || state.counts.size() == 0) {
        return null;
      }

      // Get all items into an array and sort them.
      Set<Map.Entry<LongWritable, LongWritable>> entries = state.counts.entrySet();
      List<Map.Entry<LongWritable, LongWritable>> entriesList =
        new ArrayList<Map.Entry<LongWritable, LongWritable>>(entries);
      Collections.sort(entriesList, new MyComparator());

      // Accumulate the counts.
      long total = 0;
      for (int i = 0; i < entriesList.size(); i++) {
        LongWritable count = entriesList.get(i).getValue();
        total += count.get();
        count.set(total);
      }

      // Initialize the result.
      if (result == null) {
        result = new DoubleWritable();
      }

      // maxPosition is the 1.0 percentile
      long maxPosition = total - 1;
      double position = maxPosition * state.percentiles.get(0).get();
      result.set(getPercentile(entriesList, position));
      return result;
    }
  }

  /**
   * The evaluator for percentile computation based on long for an array of percentiles.
   */
  public static class PercentileLongArrayEvaluator implements UDAFEvaluator {

    private final State state;

    public PercentileLongArrayEvaluator() {
      state = new State();
    }

    public void init() {
      if (state.counts != null) {
        // We reuse the same hashmap to reduce new object allocation.
        // This means counts can be empty when there is no input data.
        state.counts.clear();
      }
    }

    public boolean iterate(LongWritable o, List<DoubleWritable> percentiles) {
      if (state.percentiles == null) {
        if(percentiles != null) {
          for (int i = 0; i < percentiles.size(); i++) {
            if (percentiles.get(i).get() < 0.0 || percentiles.get(i).get() > 1.0) {
              throw new RuntimeException("Percentile value must be wihin the range of 0 to 1.");
            }
          }
          state.percentiles = new ArrayList<DoubleWritable>(percentiles);
        }
        else {
          state.percentiles = new ArrayList<DoubleWritable>();
        }
      }
      if (o != null) {
        increment(state, o, 1);
      }
      return true;
    }

    public State terminatePartial() {
      return state;
    }

    public boolean merge(State other) {
      if (other == null || other.counts == null || other.percentiles == null) {
        return true;
      }

      if (state.percentiles == null) {
        state.percentiles = new ArrayList<DoubleWritable>(other.percentiles);
      }

      for (Map.Entry<LongWritable, LongWritable> e: other.counts.entrySet()) {
        increment(state, e.getKey(), e.getValue().get());
      }
      return true;
    }


    private List<DoubleWritable> results;

    public List<DoubleWritable> terminate() {
      // No input data
      if (state.counts == null || state.counts.size() == 0) {
        return null;
      }

      // Get all items into an array and sort them
      Set<Map.Entry<LongWritable, LongWritable>> entries = state.counts.entrySet();
      List<Map.Entry<LongWritable, LongWritable>> entriesList =
        new ArrayList<Map.Entry<LongWritable, LongWritable>>(entries);
      Collections.sort(entriesList, new MyComparator());

      // accumulate the counts
      long total = 0;
      for (int i = 0; i < entriesList.size(); i++) {
        LongWritable count = entriesList.get(i).getValue();
        total += count.get();
        count.set(total);
      }

      // maxPosition is the 1.0 percentile
      long maxPosition = total - 1;

      // Initialize the results
      if (results == null) {
        results = new ArrayList<DoubleWritable>();
        for (int i = 0; i < state.percentiles.size(); i++) {
          results.add(new DoubleWritable());
        }
      }
      // Set the results
      for (int i = 0; i < state.percentiles.size(); i++) {
        double position = maxPosition * state.percentiles.get(i).get();
        results.get(i).set(getPercentile(entriesList, position));
      }
      return results;
    }
  }

}
