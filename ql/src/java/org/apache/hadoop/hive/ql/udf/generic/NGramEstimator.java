/*
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
package org.apache.hadoop.hive.ql.udf.generic;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;
import java.util.Iterator;
import java.util.Comparator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A generic, re-usable n-gram estimation class that supports partial aggregations.
 * The algorithm is based on the heuristic from the following paper:
 * Yael Ben-Haim and Elad Tom-Tov, "A streaming parallel decision tree algorithm",
 * J. Machine Learning Research 11 (2010), pp. 849--872.
 *
 * In particular, it is guaranteed that frequencies will be under-counted. With large
 * data and a reasonable precision factor, this undercounting appears to be on the order
 * of 5%.
 */
public class NGramEstimator {
  /* Class private variables */
  private int k;
  private int pf;
  private int n;
  private HashMap<ArrayList<String>, Double> ngrams;


  /**
   * Creates a new n-gram estimator object. The 'n' for n-grams is computed dynamically
   * when data is fed to the object.
   */
  public NGramEstimator() {
    k  = 0;
    pf = 0;
    n  = 0;
    ngrams = new HashMap<ArrayList<String>, Double>();
  }

  /**
   * Returns true if the 'k' and 'pf' parameters have been set.
   */
  public boolean isInitialized() {
    return (k != 0);
  }

  /**
   * Sets the 'k' and 'pf' parameters.
   */
  public void initialize(int pk, int ppf, int pn) throws HiveException {
    assert(pk > 0 && ppf > 0 && pn > 0);
    k = pk;
    pf = ppf;
    n = pn;

    // enforce a minimum precision factor
    if(k * pf < 1000) {
      pf = 1000 / k;
    }
  }

  /**
   * Resets an n-gram estimator object to its initial state.
   */
  public void reset() {
    ngrams.clear();
    n = pf = k = 0;
  }

  /**
   * Returns the final top-k n-grams in a format suitable for returning to Hive.
   */
  public ArrayList<Object[]> getNGrams() throws HiveException {
    trim(true);
    if(ngrams.size() < 1) { // SQL standard - return null for zero elements
      return null;
    }

    // Sort the n-gram list by frequencies in descending order
    ArrayList<Object[]> result = new ArrayList<Object[]>();
    ArrayList<Map.Entry<ArrayList<String>, Double>> list = new ArrayList(ngrams.entrySet());
    Collections.sort(list, new Comparator<Map.Entry<ArrayList<String>, Double>>() {
      public int compare(Map.Entry<ArrayList<String>, Double> o1,
                         Map.Entry<ArrayList<String>, Double> o2) {
        int result = o2.getValue().compareTo(o1.getValue());
        if (result != 0)
          return result;
        
        ArrayList<String> key1 = o1.getKey();
        ArrayList<String> key2 = o2.getKey();
        for (int i = 0; i < key1.size() && i < key2.size(); i++) {
          result = key1.get(i).compareTo(key2.get(i));
          if (result != 0)
            return result;
        }
        
        return key1.size() - key2.size();
      }
    });

    // Convert the n-gram list to a format suitable for Hive
    for(int i = 0; i < list.size(); i++) {
      ArrayList<String> key = list.get(i).getKey();
      Double val = list.get(i).getValue();

      Object[] curGram = new Object[2];
      ArrayList<Text> ng = new ArrayList<Text>();
      for(int j = 0; j < key.size(); j++) {
        ng.add(new Text(key.get(j)));
      }
      curGram[0] = ng;
      curGram[1] = new DoubleWritable(val.doubleValue());
      result.add(curGram);
    }

    return result;
  }

  /**
   * Returns the number of n-grams in our buffer.
   */
  public int size() {
    return ngrams.size();
  }

  /**
   * Adds a new n-gram to the estimation.
   *
   * @param ng The n-gram to add to the estimation
   */
  public void add(ArrayList<String> ng) throws HiveException {
    assert(ng != null && ng.size() > 0 && ng.get(0) != null);
    Double curFreq = ngrams.get(ng);
    if(curFreq == null) {
      // new n-gram
      curFreq = new Double(1.0);
    } else {
      // existing n-gram, just increment count
      curFreq++;
    }
    ngrams.put(ng, curFreq);

    // set 'n' if we haven't done so before
    if(n == 0) {
      n = ng.size();
    } else {
      if(n != ng.size()) {
        throw new HiveException(getClass().getSimpleName() + ": mismatch in value for 'n'"
            + ", which usually is caused by a non-constant expression. Found '"+n+"' and '"
            + ng.size() + "'.");
      }
    }

    // Trim down the total number of n-grams if we've exceeded the maximum amount of memory allowed
    //
    // NOTE: Although 'k'*'pf' specifies the size of the estimation buffer, we don't want to keep
    //       performing N.log(N) trim operations each time the maximum hashmap size is exceeded.
    //       To handle this, we *actually* maintain an estimation buffer of size 2*'k'*'pf', and
    //       trim down to 'k'*'pf' whenever the hashmap size exceeds 2*'k'*'pf'. This really has
    //       a significant effect when 'k'*'pf' is very high.
    if(ngrams.size() > k * pf * 2) {
      trim(false);
    }
  }

  /**
   * Trims an n-gram estimation down to either 'pf' * 'k' n-grams, or 'k' n-grams if
   * finalTrim is true.
   */
  private void trim(boolean finalTrim) throws HiveException {
    ArrayList<Map.Entry<ArrayList<String>,Double>> list = new ArrayList(ngrams.entrySet());
    Collections.sort(list, new Comparator<Map.Entry<ArrayList<String>,Double>>() {
      public int compare(Map.Entry<ArrayList<String>,Double> o1,
                         Map.Entry<ArrayList<String>,Double> o2) {
        return o1.getValue().compareTo(o2.getValue());
      }
    });
    for(int i = 0; i < list.size() - (finalTrim ? k : pf*k); i++) {
      ngrams.remove( list.get(i).getKey() );
    }
  }

  /**
   * Takes a serialized n-gram estimator object created by the serialize() method and merges
   * it with the current n-gram object.
   *
   * @param other A serialized n-gram object created by the serialize() method
   */
  public void merge(List other) throws HiveException {
    if(other == null) {
      return;
    }

    // Get estimation parameters
    int otherK = Integer.parseInt(other.get(0).toString());
    int otherN = Integer.parseInt(other.get(1).toString());
    int otherPF = Integer.parseInt(other.get(2).toString());
    if(k > 0 && k != otherK) {
      throw new HiveException(getClass().getSimpleName() + ": mismatch in value for 'k'"
          + ", which usually is caused by a non-constant expression. Found '"+k+"' and '"
          + otherK + "'.");
    }
    if(n > 0 && otherN != n) {
      throw new HiveException(getClass().getSimpleName() + ": mismatch in value for 'n'"
          + ", which usually is caused by a non-constant expression. Found '"+n+"' and '"
          + otherN + "'.");
    }
    if(pf > 0 && otherPF != pf) {
      throw new HiveException(getClass().getSimpleName() + ": mismatch in value for 'pf'"
          + ", which usually is caused by a non-constant expression. Found '"+pf+"' and '"
          + otherPF + "'.");
    }
    k = otherK;
    pf = otherPF;
    n = otherN;

    // Merge the other estimation into the current one
    for(int i = 3; i < other.size(); i++) {
      ArrayList<String> key = new ArrayList<String>();
      for(int j = 0; j < n; j++) {
        key.add(other.get(i+j).toString());
      }
      i += n;
      double val = Double.parseDouble( other.get(i).toString() );
      Double myval = ngrams.get(key);
      if(myval == null) {
        myval = new Double(val);
      } else {
        myval += val;
      }
      ngrams.put(key, myval);
    }

    trim(false);
  }


  /**
   * In preparation for a Hive merge() call, serializes the current n-gram estimator object into an
   * ArrayList of Text objects. This list is deserialized and merged by the
   * merge method.
   *
   * @return An ArrayList of Hadoop Text objects that represents the current
   * n-gram estimation.
   * @see #merge
   */
  public ArrayList<Text> serialize() throws HiveException {
    ArrayList<Text> result = new ArrayList<Text>();
    result.add(new Text(Integer.toString(k)));
    result.add(new Text(Integer.toString(n)));
    result.add(new Text(Integer.toString(pf)));
    for(Iterator<ArrayList<String> > it = ngrams.keySet().iterator(); it.hasNext(); ) {
      ArrayList<String> mykey = it.next();
      assert(mykey.size() > 0);
      for(int i = 0; i < mykey.size(); i++) {
        result.add(new Text(mykey.get(i)));
      }
      Double myval = ngrams.get(mykey);
      result.add(new Text(myval.toString()));
    }

    return result;
  }
}
