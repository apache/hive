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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * This class contains the bucketing sorting context that is passed
 * while walking the operator tree in inferring bucket/sort columns. The context
 * contains the mappings from operators and files to the columns their output is
 * bucketed/sorted on.
 */
public class BucketingSortingCtx implements NodeProcessorCtx {

  private boolean disableBucketing;

  // A mapping from an operator to the columns by which it's output is bucketed
  private Map<Operator<? extends OperatorDesc>, List<BucketCol>> bucketedColsByOp;
  // A mapping from a directory which a FileSinkOperator writes into to the columns by which that
  // output is bucketed
  private Map<String, List<BucketCol>> bucketedColsByDirectory;

  // A mapping from an operator to the columns by which it's output is sorted
  private Map<Operator<? extends OperatorDesc>, List<SortCol>> sortedColsByOp;
  // A mapping from a directory which a FileSinkOperator writes into to the columns by which that
  // output is sorted
  private Map<String, List<SortCol>> sortedColsByDirectory;

  public BucketingSortingCtx(boolean disableBucketing) {
    this.disableBucketing = disableBucketing;
    this.bucketedColsByOp = new HashMap<Operator<? extends OperatorDesc>, List<BucketCol>>();
    this.bucketedColsByDirectory = new HashMap<String, List<BucketCol>>();
    this.sortedColsByOp = new HashMap<Operator<? extends OperatorDesc>, List<SortCol>>();
    this.sortedColsByDirectory = new HashMap<String, List<SortCol>>();
  }


  public List<BucketCol> getBucketedCols(Operator<? extends OperatorDesc> op) {
    return disableBucketing ? null : bucketedColsByOp.get(op);
  }


  public void setBucketedCols(Operator<? extends OperatorDesc> op, List<BucketCol> bucketCols) {
    if (!disableBucketing) {
      bucketedColsByOp.put(op, bucketCols);
    }
  }

  public Map<String, List<BucketCol>> getBucketedColsByDirectory() {
    return disableBucketing ? null : bucketedColsByDirectory;
  }


  public void setBucketedColsByDirectory(Map<String, List<BucketCol>> bucketedColsByDirectory) {
    if (!disableBucketing) {
      this.bucketedColsByDirectory = bucketedColsByDirectory;
    }
  }


  public List<SortCol> getSortedCols(Operator<? extends OperatorDesc> op) {
    return sortedColsByOp.get(op);
  }


  public void setSortedCols(Operator<? extends OperatorDesc> op, List<SortCol> sortedCols) {
    this.sortedColsByOp.put(op, sortedCols);
  }

  public Map<String, List<SortCol>> getSortedColsByDirectory() {
    return sortedColsByDirectory;
  }


  public void setSortedColsByDirectory(Map<String, List<SortCol>> sortedColsByDirectory) {
    this.sortedColsByDirectory = sortedColsByDirectory;
  }

  /**
   *
   * BucketSortCol.
   *
   * Classes that implement this interface provide a way to store information about equivalent
   * columns as their names and indexes in the schema change going into and out of operators.  The
   * definition of equivalent columns is up to the class which uses these classes, e.g.
   * BucketingSortingOpProcFactory.  For example, two columns are equivalent if they
   * contain exactly the same data.  Though, it's possible that two columns contain exactly the
   * same data and are not known to be equivalent.
   *
   * E.g. SELECT key a, key b FROM (SELECT key, count(*) c FROM src GROUP BY key) s;
   * In this case, assuming this is done in a single map reduce job with the group by operator
   * processed in the reducer, the data coming out of the group by operator will be bucketed
   * by key, which would be at index 0 in the schema, after the outer select operator, the output
   * can be viewed as bucketed by either the column with alias a or the column with alias b.  To
   * represent this, there could be a single BucketSortCol implementation instance whose names
   * include both a and b, and whose indexes include both 0 and 1.
   *
   * Implementations of this interface should maintain the restriction that the alias
   * getNames().get(i) should have index getIndexes().get(i) in the schema.
   */
  public static interface BucketSortCol {
    // Get a list of aliases for the same column
    public List<String> getNames();

    // Get a list of indexes for which the columns in the schema are the same
    public List<Integer> getIndexes();

    // Add an alternative alias for the column this instance represents, and its index in the
    // schema.
    public void addAlias(String name, Integer index);
  }

  /**
   *
   * BucketCol.
   *
   * An implementation of BucketSortCol which contains known aliases/indexes of equivalent columns
   * which data is determined to be bucketed on.
   */
  public static final class BucketCol implements BucketSortCol, Serializable {
    private static final long serialVersionUID = 1L;
    // Equivalent aliases for the column
    private final List<String> names = new ArrayList<String>();
    // Indexes of those equivalent columns
    private final List<Integer> indexes = new ArrayList<Integer>();

    public BucketCol(String name, int index) {
      addAlias(name, index);
    }

    public BucketCol() {

    }

    @Override
    public List<String> getNames() {
      return names;
    }

    @Override
    public List<Integer> getIndexes() {
      return indexes;
    }

    @Override
    public void addAlias(String name, Integer index) {
      names.add(name);
      indexes.add(index);
    }

    @Override
    // Chooses a representative alias and index to use as the String, the first is used because
    // it is set in the constructor
    public String toString() {
      return "name: " + names.get(0) + " index: " + indexes.get(0);
    }
  }

  /**
   *
   * SortCol.
   *
   * An implementation of BucketSortCol which contains known aliases/indexes of equivalent columns
   * which data is determined to be sorted on.  Unlike aliases, and indexes the sort order is known
   * to be constant for all equivalent columns.
   */
  public static final class SortCol implements BucketSortCol, Serializable {

    public SortCol() {
      super();
    }

    private static final long serialVersionUID = 1L;
    // Equivalent aliases for the column
    private List<String> names = new ArrayList<String>();
    // Indexes of those equivalent columns
    private List<Integer> indexes = new ArrayList<Integer>();
    // Sort order (+|-)
    private char sortOrder;

    public SortCol(String name, int index, char sortOrder) {
      this(sortOrder);
      addAlias(name, index);
    }

    public SortCol(char sortOrder) {
      this.sortOrder = sortOrder;
    }


    @Override
    public List<String> getNames() {
      return names;
    }

    @Override
    public List<Integer> getIndexes() {
      return indexes;
    }

    @Override
    public void addAlias(String name, Integer index) {
      names.add(name);
      indexes.add(index);
    }

    public char getSortOrder() {
      return sortOrder;
    }

    @Override
    // Chooses a representative alias, index, and order to use as the String, the first is used
    // because it is set in the constructor
    public String toString() {
      return "name: " + names.get(0) + " index: " + indexes.get(0) + " order: " + sortOrder;
    }
  }
}
