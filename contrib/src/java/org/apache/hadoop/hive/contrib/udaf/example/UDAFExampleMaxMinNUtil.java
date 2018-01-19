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


package org.apache.hadoop.hive.contrib.udaf.example;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * The utility class for UDAFMaxN and UDAFMinN.
 */
public final class UDAFExampleMaxMinNUtil {

  /**
   * This class stores the information during an aggregation.
   * 
   * Note that this class has to have a public constructor, so that Hive can 
   * serialize/deserialize this class using reflection.  
   */
  public static class State {
    ArrayList<Double> a; // This ArrayList holds the max/min N
    int n; // This is the N
  }
  
  /**
   * The base class of the UDAFEvaluator for UDAFMaxN and UDAFMinN.  
   * We just need to override the getAscending function to make it work.
   */
  public abstract static class Evaluator implements UDAFEvaluator {
    
    private State state;
    
    public Evaluator() {
      state = new State();
      init();
    }
    
    /**
     * Reset the state.
     */
    public void init() {    
      state.a = new ArrayList<Double>();
      state.n = 0;
    }
  
    /**
     *  Returns true in UDAFMaxN, and false in UDAFMinN.
     */
    protected abstract boolean getAscending();
    
    /**
     * Iterate through one row of original data.
     * This function will update the internal max/min buffer if the internal buffer is not full,
     * or the new row is larger/smaller than the current max/min n.
     */
    public boolean iterate(Double o, int n) {
      boolean ascending = getAscending();
      state.n = n;
      if (o != null) {
        boolean doInsert = state.a.size() < n;
        if (!doInsert) {
          Double last = state.a.get(state.a.size()-1);
          if (ascending) {
            doInsert = o < last; 
          } else {
            doInsert = o > last;
          }
        }
        if (doInsert) {
          binaryInsert(state.a, o, ascending);   
          if (state.a.size() > n) {
            state.a.remove(state.a.size()-1);
          }
        }
      }
      return true;
    }

    /**
     * Get partial aggregation results.
     */
    public State terminatePartial() {
      // This is SQL standard - max_n of zero items should be null.
      return state.a.size() == 0 ? null : state;
    }

    /** Two pointers are created to track the maximal elements in both o and MaxNArray.
     *  The smallest element is added into tempArrayList
     *  Consider the sizes of o and MaxNArray may be different.
     */
    public boolean merge(State o) {
      if (o != null) {
        state.n = o.n;
        state.a = sortedMerge(o.a, state.a, getAscending(), o.n);
      }      
      return true;
    }
  
    /**
     * Terminates the max N lookup and return the final result.
     */
    public ArrayList<Double> terminate() {
      // This is SQL standard - return state.MaxNArray, or null if the size is zero.
      return state.a.size() == 0 ? null : state.a;
    }
  }

  
  /**
   * Returns a comparator based on whether the order is ascending or not.
   * Has a dummy parameter to make sure generics can infer the type correctly.
   */
  static <T extends Comparable<T>> Comparator<T> getComparator(boolean ascending, T dummy) {
    Comparator<T> comp;
    if (ascending) {
      comp = new Comparator<T>() {
        public int compare(T o1, T o2) {
          return o1.compareTo(o2);
        }
      };
    } else {
      comp = new Comparator<T>() {
        public int compare(T o1, T o2) {
          return o2.compareTo(o1);
        }
      };
    }
    return comp;
  }
  
  /**
   * Insert an element into an ascending/descending array, and keep the order. 
   * @param ascending
   *            if true, the array is sorted in ascending order,
   *            otherwise it is in descending order.
   * 
   */
  static <T extends Comparable<T>> void binaryInsert(List<T> list, T value, boolean ascending) {
    
    int position = Collections.binarySearch(list, value, getComparator(ascending, (T)null));
    if (position < 0) {
      position = (-position) - 1; 
    }
    list.add(position, value);
  }
  
  /**
   * Merge two ascending/descending array and keep the first n elements.
   * @param ascending
   *            if true, the array is sorted in ascending order,
   *            otherwise it is in descending order.
   */
  static <T extends Comparable<T>> ArrayList<T> sortedMerge(List<T> a1, List<T> a2,
      boolean ascending, int n) {

    Comparator<T> comparator = getComparator(ascending, (T)null);
    
    int n1 = a1.size();
    int n2 = a2.size();
    int p1 = 0; // The current element in a1
    int p2 = 0; // The current element in a2
    
    ArrayList<T> output = new ArrayList<T>(n); 
    
    while (output.size() < n && (p1 < n1 || p2 < n2)) {
      if (p1 < n1) {
        if (p2 == n2 || comparator.compare(a1.get(p1), a2.get(p2)) < 0) {
          output.add(a1.get(p1++));
        }
      }
      if (output.size() == n) {
        break;
      }
      if (p2 < n2) {
        if (p1 == n1 || comparator.compare(a2.get(p2), a1.get(p1)) < 0) {
          output.add(a2.get(p2++));
        }
      }
    }

    return output;
  }
  
  // No instantiation.
  private UDAFExampleMaxMinNUtil() {
  }
  
}

