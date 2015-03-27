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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LeafNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeVisitor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;


/**
 * Utility function for generating hbase partition filtering plan representation
 * from ExpressionTree.
 * Optimizations to be done -
 *  - Case where all partition keys are specified. Should use a get
 *
 * {@link PartitionFilterGenerator} is a visitor on the given filter expression tree. After
 * walking it it produces the HBase execution plan represented by {@link FilterPlan}. See
 * their javadocs for more details.
 */
class HBaseFilterPlanUtil {

  /**
   * Compare two byte arrays.
   *
   * @param ar1
   *          first byte array
   * @param ar2
   *          second byte array
   * @return -1 if ar1 < ar2, 0 if == , 1 if >
   */
  static int compare(byte[] ar1, byte[] ar2) {
    // null check is not needed, nulls are not passed here
    for (int i = 0; i < ar1.length; i++) {
      if (i == ar2.length) {
        return 1;
      } else {
        if (ar1[i] == ar2[i]) {
          continue;
        } else if (ar1[i] > ar2[i]) {
          return 1;
        } else {
          return -1;
        }
      }
    }
    // ar2 equal until length of ar1.
    if(ar1.length == ar2.length) {
      return 0;
    }
    // ar2 has more bytes
    return -1;
  }

  /**
   * Represents the execution plan for hbase to find the set of partitions that
   * match given filter expression.
   * If you have an AND or OR of two expressions, you can determine FilterPlan for each
   * children and then call lhs.and(rhs) or lhs.or(rhs) respectively
   * to generate a new plan for the expression.
   *
   * The execution plan has one or more ScanPlan objects. To get the results the set union of all
   * ScanPlan objects needs to be done.
   */
  public static abstract class FilterPlan {
    abstract FilterPlan and(FilterPlan other);
    abstract FilterPlan or(FilterPlan other);
    abstract List<ScanPlan> getPlans();
    @Override
    public String toString() {
      return getPlans().toString();
    }

  }

  /**
   * Represents a union/OR of single scan plans (ScanPlan).
   */
  public static class MultiScanPlan extends FilterPlan {
    final ImmutableList<ScanPlan> scanPlans;

    public MultiScanPlan(List<ScanPlan> scanPlans){
      this.scanPlans = ImmutableList.copyOf(scanPlans);
    }

    @Override
    public FilterPlan and(FilterPlan other) {
      // Convert to disjunctive normal form (DNF), ie OR of ANDs
      // First get a new set of FilterPlans by doing an AND
      // on each ScanPlan in this one with the other FilterPlan
      List<FilterPlan> newFPlans = new ArrayList<FilterPlan>();
      for (ScanPlan splan : getPlans()) {
        newFPlans.add(splan.and(other));
      }
      //now combine scanPlans in multiple new FilterPlans into one
      // MultiScanPlan
      List<ScanPlan> newScanPlans = new ArrayList<ScanPlan>();
      for (FilterPlan fp : newFPlans) {
        newScanPlans.addAll(fp.getPlans());
      }
      return new MultiScanPlan(newScanPlans);
    }

    @Override
    public FilterPlan or(FilterPlan other) {
      // just combine the ScanPlans
      List<ScanPlan> newScanPlans = new ArrayList<ScanPlan>(this.getPlans());
      newScanPlans.addAll(other.getPlans());
      return new MultiScanPlan(newScanPlans);
    }

    @Override
    public List<ScanPlan> getPlans() {
      return scanPlans;
    }
  }

  /**
   * Represents a single Hbase Scan api call
   */
  public static class ScanPlan extends FilterPlan {

    public static class ScanMarker {
      final byte[] bytes;
      /**
       * If inclusive = true, it means that the
       * marker includes those bytes.
       * If it is false, it means the marker starts at the next possible byte array
       * or ends at the next possible byte array
       */
      final boolean isInclusive;
      ScanMarker(byte [] b, boolean i){
        this.bytes = b;
        this.isInclusive = i;
      }
      @Override
      public String toString() {
        return "ScanMarker [bytes=" + Arrays.toString(bytes) + ", isInclusive=" + isInclusive + "]";
      }
      @Override
      public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(bytes);
        result = prime * result + (isInclusive ? 1231 : 1237);
        return result;
      }
      @Override
      public boolean equals(Object obj) {
        if (this == obj)
          return true;
        if (obj == null)
          return false;
        if (getClass() != obj.getClass())
          return false;
        ScanMarker other = (ScanMarker) obj;
        if (!Arrays.equals(bytes, other.bytes))
          return false;
        if (isInclusive != other.isInclusive)
          return false;
        return true;
      }
    }
    // represent Scan start
    private ScanMarker startMarker = new ScanMarker(null, false);
    // represent Scan end
    private ScanMarker endMarker = new ScanMarker(null, false);

    private ScanFilter filter;

    public ScanFilter getFilter() {
      return filter;
    }

    public void setFilter(ScanFilter filter) {
      this.filter = filter;
    }

    public ScanMarker getStartMarker() {
      return startMarker;
    }

    public void setStartMarker(ScanMarker startMarker) {
      this.startMarker = startMarker;
    }
    public void setStartMarker(byte[] start, boolean isInclusive) {
      setStartMarker(new ScanMarker(start, isInclusive));
    }

    public ScanMarker getEndMarker() {
      return endMarker;
    }

    public void setEndMarker(ScanMarker endMarker) {
      this.endMarker = endMarker;
    }
    public void setEndMarker(byte[] end, boolean isInclusive) {
      setEndMarker(new ScanMarker(end, isInclusive));
    }

    @Override
    public FilterPlan and(FilterPlan other) {
      List<ScanPlan> newSPlans = new ArrayList<ScanPlan>();
      for (ScanPlan otherSPlan : other.getPlans()) {
        newSPlans.add(this.and(otherSPlan));
      }
      return new MultiScanPlan(newSPlans);
    }

    private ScanPlan and(ScanPlan other) {
      // create combined FilterPlan based on existing lhs and rhs plan
      ScanPlan newPlan = new ScanPlan();

      // create new scan start
      ScanMarker greaterStartMarker = getComparedMarker(this.getStartMarker(),
          other.getStartMarker(), true);
      newPlan.setStartMarker(greaterStartMarker);

      // create new scan end
      ScanMarker lesserEndMarker = getComparedMarker(this.getEndMarker(), other.getEndMarker(),
          false);
      newPlan.setEndMarker(lesserEndMarker);

      // create new filter plan
      newPlan.setFilter(createCombinedFilter(this.getFilter(), other.getFilter()));

      return newPlan;
    }

    private ScanFilter createCombinedFilter(ScanFilter filter1, ScanFilter filter2) {
      // TODO create combined filter - filter1 && filter2
      return null;
    }

    /**
     * @param lStartMarker
     * @param rStartMarker
     * @param getGreater if true return greater startmarker, else return smaller one
     * @return greater/lesser marker depending on value of getGreater
     */
    @VisibleForTesting
    static ScanMarker getComparedMarker(ScanMarker lStartMarker, ScanMarker rStartMarker,
        boolean getGreater) {
      // if one of them has null bytes, just return other
      if(lStartMarker.bytes == null) {
        return rStartMarker;
      } else if (rStartMarker.bytes == null) {
        return lStartMarker;
      }

      int compareRes = compare(lStartMarker.bytes, rStartMarker.bytes);
      if (compareRes == 0) {
        // bytes are equal, now compare the isInclusive flags
        if (lStartMarker.isInclusive == rStartMarker.isInclusive) {
          // actually equal, so return any one
          return lStartMarker;
        }
        boolean isInclusive = true;
        // one that does not include the current bytes is greater
        if (getGreater) {
          isInclusive = false;
        }
        // else
        return new ScanMarker(lStartMarker.bytes, isInclusive);
      }
      if (getGreater) {
        return compareRes == 1 ? lStartMarker : rStartMarker;
      }
      // else
      return compareRes == -1 ? lStartMarker : rStartMarker;
    }


    @Override
    public FilterPlan or(FilterPlan other) {
      List<ScanPlan> plans = new ArrayList<ScanPlan>(getPlans());
      plans.addAll(other.getPlans());
      return new MultiScanPlan(plans);
    }

    @Override
    public List<ScanPlan> getPlans() {
      return Arrays.asList(this);
    }


    /**
     * @return row suffix - This is appended to db + table, to generate start row for the Scan
     */
    public byte[] getStartRowSuffix() {
      if (startMarker.isInclusive) {
        return startMarker.bytes;
      } else {
        return HBaseUtils.getEndPrefix(startMarker.bytes);
      }
    }

    /**
     * @return row suffix - This is appended to db + table, to generate end row for the Scan
     */
    public byte[] getEndRowSuffix() {
      if (endMarker.isInclusive) {
        return HBaseUtils.getEndPrefix(endMarker.bytes);
      } else {
        return endMarker.bytes;
      }
    }

    @Override
    public String toString() {
      return "ScanPlan [startMarker=" + startMarker + ", endMarker=" + endMarker + ", filter="
          + filter + "]";
    }

  }

  /**
   * represent a plan that can be used to create a hbase filter and then set in
   * Scan.setFilter()
   */
  public static class ScanFilter {
    // TODO: implement this
  }

  /**
   * Visitor for ExpressionTree.
   * It first generates the ScanPlan for the leaf nodes. The higher level nodes are
   * either AND or OR operations. It then calls FilterPlan.and and FilterPlan.or with
   * the child nodes to generate the plans for higher level nodes.
   */
  @VisibleForTesting
  static class PartitionFilterGenerator extends TreeVisitor {
    private FilterPlan curPlan;

    // this tells us if there is a condition that did not get included in the plan
    // such condition would be treated as getting evaluated to TRUE
    private boolean hasUnsupportedCondition = false;

    //Need to cache the left plans for the TreeNode. Use IdentityHashMap here
    // as we don't want to dedupe on two TreeNode that are otherwise considered equal
    Map<TreeNode, FilterPlan> leftPlans = new IdentityHashMap<TreeNode, FilterPlan>();

    // temporary params for current left and right side plans, for AND, OR
    private FilterPlan rPlan;

    private final String firstPartcolumn;
    public PartitionFilterGenerator(String firstPartitionColumn) {
      this.firstPartcolumn = firstPartitionColumn;
    }

    FilterPlan getPlan() {
      return curPlan;
    }

    @Override
    protected void beginTreeNode(TreeNode node) throws MetaException {
      // reset the params
      curPlan = rPlan = null;
    }

    @Override
    protected void midTreeNode(TreeNode node) throws MetaException {
      leftPlans.put(node, curPlan);
      curPlan = null;
    }

    @Override
    protected void endTreeNode(TreeNode node) throws MetaException {
      rPlan = curPlan;
      FilterPlan lPlan = leftPlans.get(node);
      leftPlans.remove(node);

      switch (node.getAndOr()) {
      case AND:
        curPlan = lPlan.and(rPlan);
        break;
      case OR:
        curPlan = lPlan.or(rPlan);
        break;
      default:
        throw new AssertionError("Unexpected logical operation " + node.getAndOr());
      }

    }


    @Override
    public void visit(LeafNode node) throws MetaException {
      ScanPlan leafPlan = new ScanPlan();
      curPlan = leafPlan;
      if (!isFirstParitionColumn(node.keyName)) {
        leafPlan.setFilter(generateScanFilter(node));
        return;
      }
      if (!(node.value instanceof String)) {
        // only string type is supported currently
        // treat conditions on other types as true
        return;
      }

      // this is a condition on first partition column, so might influence the
      // start and end of the scan
      final boolean INCLUSIVE = true;
      switch (node.operator) {
      case EQUALS:
        leafPlan.setStartMarker(toBytes(node.value), INCLUSIVE);
        leafPlan.setEndMarker(toBytes(node.value), INCLUSIVE);
        break;
      case GREATERTHAN:
        leafPlan.setStartMarker(toBytes(node.value), !INCLUSIVE);
        break;
      case GREATERTHANOREQUALTO:
        leafPlan.setStartMarker(toBytes(node.value), INCLUSIVE);
        break;
      case LESSTHAN:
        leafPlan.setEndMarker(toBytes(node.value), !INCLUSIVE);
        break;
      case LESSTHANOREQUALTO:
        leafPlan.setEndMarker(toBytes(node.value), INCLUSIVE);
        break;
      case LIKE:
      case NOTEQUALS:
      case NOTEQUALS2:
        // TODO: create filter plan for these
        hasUnsupportedCondition = true;
        break;
      }
    }

    @VisibleForTesting
    static byte[] toBytes(Object value) {
      // TODO: actually implement this
      // We need to determine the actual type and use appropriate
      // serialization format for that type
      return ((String) value).getBytes(HBaseUtils.ENCODING);
    }

    private ScanFilter generateScanFilter(LeafNode node) {
      // TODO Auto-generated method stub
      hasUnsupportedCondition = true;
      return null;
    }

    private boolean isFirstParitionColumn(String keyName) {
      return keyName.equalsIgnoreCase(firstPartcolumn);
    }

    private boolean hasUnsupportedCondition() {
      return hasUnsupportedCondition;
    }

  }

  public static class PlanResult {
    public final FilterPlan plan;
    public final boolean hasUnsupportedCondition;
    PlanResult(FilterPlan plan, boolean hasUnsupportedCondition) {
      this.plan = plan;
      this.hasUnsupportedCondition = hasUnsupportedCondition;
    }
  }

  public static PlanResult getFilterPlan(ExpressionTree exprTree, String firstPartitionColumn) throws MetaException {
    if (exprTree == null) {
      // TODO: if exprTree is null, we should do what ObjectStore does. See HIVE-10102
      return new PlanResult(new ScanPlan(), true);
    }
    PartitionFilterGenerator pGenerator = new PartitionFilterGenerator(firstPartitionColumn);
    exprTree.accept(pGenerator);
    return new PlanResult(pGenerator.getPlan(), pGenerator.hasUnsupportedCondition());
  }

}
