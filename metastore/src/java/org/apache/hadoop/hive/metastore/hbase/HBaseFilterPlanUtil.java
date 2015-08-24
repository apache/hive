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
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.hbase.PartitionKeyComparator.Operator;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LeafNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeVisitor;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

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
      final String value;
      /**
       * If inclusive = true, it means that the
       * marker includes those bytes.
       * If it is false, it means the marker starts at the next possible byte array
       * or ends at the next possible byte array
       */
      final boolean isInclusive;
      final String type;
      ScanMarker(String obj, boolean i, String type){
        this.value = obj;
        this.isInclusive = i;
        this.type = type;
      }
      @Override
      public String toString() {
        return "ScanMarker [" + "value=" + value.toString() + ", isInclusive=" + isInclusive +
            ", type=" + type + "]";
      }
      @Override
      public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + value.hashCode();
        result = prime * result + (isInclusive ? 1231 : 1237);
        result = prime * result + type.hashCode();
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
        if (!value.equals(other.value))
          return false;
        if (isInclusive != other.isInclusive)
          return false;
        if (type != other.type)
          return false;
        return true;
      }
    }
    public static class ScanMarkerPair {
      public ScanMarkerPair(ScanMarker startMarker, ScanMarker endMarker) {
        this.startMarker = startMarker;
        this.endMarker = endMarker;
      }
      ScanMarker startMarker;
      ScanMarker endMarker;
    }
    // represent Scan start, partition key name -> scanMarkerPair
    Map<String, ScanMarkerPair> markers = new HashMap<String, ScanMarkerPair>();
    List<Operator> ops = new ArrayList<Operator>();

    // Get the number of partition key prefixes which can be used in the scan range.
    // For example, if partition key is (year, month, state)
    // 1. year = 2015 and month >= 1 and month < 5
    //    year + month can be used in scan range, majorParts = 2
    // 2. year = 2015 and state = 'CA'
    //    only year can be used in scan range, majorParts = 1
    // 3. month = 10 and state = 'CA'
    //    nothing can be used in scan range, majorParts = 0
    private int getMajorPartsCount(List<FieldSchema> parts) {
      int majorPartsCount = 0;
      while (majorPartsCount<parts.size() && markers.containsKey(parts.get(majorPartsCount).getName())) {
        ScanMarkerPair pair = markers.get(parts.get(majorPartsCount).getName());
        majorPartsCount++;
        if (pair.startMarker!=null && pair.endMarker!=null && pair.startMarker.value.equals(pair
            .endMarker.value) && pair.startMarker.isInclusive && pair.endMarker.isInclusive) {
          // is equal
          continue;
        } else {
          break;
        }
      }
      return majorPartsCount;
    }
    public Filter getFilter(List<FieldSchema> parts) {
      int majorPartsCount = getMajorPartsCount(parts);
      Set<String> majorKeys = new HashSet<String>();
      for (int i=0;i<majorPartsCount;i++) {
        majorKeys.add(parts.get(i).getName());
      }

      List<String> names = HBaseUtils.getPartitionNames(parts);
      List<PartitionKeyComparator.Range> ranges = new ArrayList<PartitionKeyComparator.Range>();
      for (Map.Entry<String, ScanMarkerPair> entry : markers.entrySet()) {
        if (names.contains(entry.getKey()) && !majorKeys.contains(entry.getKey())) {
          PartitionKeyComparator.Mark startMark = null;
          if (entry.getValue().startMarker != null) {
            startMark = new PartitionKeyComparator.Mark(entry.getValue().startMarker.value,
                entry.getValue().startMarker.isInclusive);
          }
          PartitionKeyComparator.Mark endMark = null;
          if (entry.getValue().endMarker != null) {
            startMark = new PartitionKeyComparator.Mark(entry.getValue().endMarker.value,
                entry.getValue().endMarker.isInclusive);
          }
          PartitionKeyComparator.Range range = new PartitionKeyComparator.Range(
              entry.getKey(), startMark, endMark);
          ranges.add(range);
        }
      }

      if (ranges.isEmpty() && ops.isEmpty()) {
        return null;
      } else {
        return new RowFilter(CompareFilter.CompareOp.EQUAL, new PartitionKeyComparator(
            StringUtils.join(names, ","), StringUtils.join(HBaseUtils.getPartitionKeyTypes(parts), ","),
            ranges, ops));
      }
    }

    public void setStartMarker(String keyName, String keyType, String start, boolean isInclusive) {
      if (markers.containsKey(keyName)) {
        markers.get(keyName).startMarker = new ScanMarker(start, isInclusive, keyType);
      } else {
        ScanMarkerPair marker = new ScanMarkerPair(new ScanMarker(start, isInclusive, keyType), null);
        markers.put(keyName, marker);
      }
    }

    public ScanMarker getStartMarker(String keyName) {
      if (markers.containsKey(keyName)) {
        return markers.get(keyName).startMarker;
      } else {
        return null;
      }
    }

    public void setEndMarker(String keyName, String keyType, String end, boolean isInclusive) {
      if (markers.containsKey(keyName)) {
        markers.get(keyName).endMarker = new ScanMarker(end, isInclusive, keyType);
      } else {
        ScanMarkerPair marker = new ScanMarkerPair(null, new ScanMarker(end, isInclusive, keyType));
        markers.put(keyName, marker);
      }
    }

    public ScanMarker getEndMarker(String keyName) {
      if (markers.containsKey(keyName)) {
        return markers.get(keyName).endMarker;
      } else {
        return null;
      }
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
      newPlan.markers.putAll(markers);

      for (String keyName : other.markers.keySet()) {
        if (newPlan.markers.containsKey(keyName)) {
          // create new scan start
          ScanMarker greaterStartMarker = getComparedMarker(this.getStartMarker(keyName),
              other.getStartMarker(keyName), true);
          if (greaterStartMarker != null) {
            newPlan.setStartMarker(keyName, greaterStartMarker.type, greaterStartMarker.value, greaterStartMarker.isInclusive);
          }

          // create new scan end
          ScanMarker lesserEndMarker = getComparedMarker(this.getEndMarker(keyName), other.getEndMarker(keyName),
              false);
          if (lesserEndMarker != null) {
            newPlan.setEndMarker(keyName, lesserEndMarker.type, lesserEndMarker.value, lesserEndMarker.isInclusive);
          }
        } else {
          newPlan.markers.put(keyName, other.markers.get(keyName));
        }
      }

      newPlan.ops.addAll(ops);
      newPlan.ops.addAll(other.ops);
      return newPlan;
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
      if(lStartMarker == null) {
        return rStartMarker;
      } else if (rStartMarker == null) {
        return lStartMarker;
      }
      TypeInfo expectedType =
          TypeInfoUtils.getTypeInfoFromTypeString(lStartMarker.type);
      ObjectInspector outputOI =
          TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(expectedType);
      Converter lConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector, outputOI);
      Converter rConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector, outputOI);
      Comparable lValue = (Comparable)lConverter.convert(lStartMarker.value);
      Comparable rValue = (Comparable)rConverter.convert(rStartMarker.value);

      int compareRes = lValue.compareTo(rValue);
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
        return new ScanMarker(lStartMarker.value, isInclusive, lStartMarker.type);
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
    public byte[] getStartRowSuffix(String dbName, String tableName, List<FieldSchema> parts) {
      int majorPartsCount = getMajorPartsCount(parts);
      List<String> majorPartTypes = new ArrayList<String>();
      List<String> components = new ArrayList<String>();
      boolean endPrefix = false;
      for (int i=0;i<majorPartsCount;i++) {
        majorPartTypes.add(parts.get(i).getType());
        ScanMarker marker = markers.get(parts.get(i).getName()).startMarker;
        if (marker != null) {
          components.add(marker.value);
          if (i==majorPartsCount-1) {
            endPrefix = !marker.isInclusive;
          }
        } else {
          components.add(null);
          if (i==majorPartsCount-1) {
            endPrefix = false;
          }
        }
      }
      byte[] bytes = HBaseUtils.buildPartitionKey(dbName, tableName, majorPartTypes, components, endPrefix);
      return bytes;
    }

    /**
     * @return row suffix - This is appended to db + table, to generate end row for the Scan
     */
    public byte[] getEndRowSuffix(String dbName, String tableName, List<FieldSchema> parts) {
      int majorPartsCount = getMajorPartsCount(parts);
      List<String> majorPartTypes = new ArrayList<String>();
      List<String> components = new ArrayList<String>();
      boolean endPrefix = false;
      for (int i=0;i<majorPartsCount;i++) {
        majorPartTypes.add(parts.get(i).getType());
        ScanMarker marker = markers.get(parts.get(i).getName()).endMarker;
        if (marker != null) {
          components.add(marker.value);
          if (i==majorPartsCount-1) {
            endPrefix = marker.isInclusive;
          }
        } else {
          components.add(null);
          if (i==majorPartsCount-1) {
            endPrefix = true;
          }
        }
      }
      byte[] bytes = HBaseUtils.buildPartitionKey(dbName, tableName, majorPartTypes, components, endPrefix);
      if (components.isEmpty()) {
        bytes[bytes.length-1]++;
      }
      return bytes;
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("ScanPlan:\n");
      for (Map.Entry<String, ScanMarkerPair> entry : markers.entrySet()) {
        sb.append("key=" + entry.getKey() + "[startMarker=" + entry.getValue().startMarker
            + ", endMarker=" + entry.getValue().endMarker + "]");
      }
      return sb.toString();
    }

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

    private Map<String, String> nameToType = new HashMap<String, String>();

    public PartitionFilterGenerator(List<FieldSchema> parts) {
      for (FieldSchema part : parts) {
        nameToType.put(part.getName(), part.getType());
      }
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

      // this is a condition on first partition column, so might influence the
      // start and end of the scan
      final boolean INCLUSIVE = true;
      switch (node.operator) {
      case EQUALS:
        leafPlan.setStartMarker(node.keyName, nameToType.get(node.keyName), node.value.toString(), INCLUSIVE);
        leafPlan.setEndMarker(node.keyName, nameToType.get(node.keyName), node.value.toString(), INCLUSIVE);
        break;
      case GREATERTHAN:
        leafPlan.setStartMarker(node.keyName, nameToType.get(node.keyName), node.value.toString(), !INCLUSIVE);
        break;
      case GREATERTHANOREQUALTO:
        leafPlan.setStartMarker(node.keyName, nameToType.get(node.keyName), node.value.toString(), INCLUSIVE);
        break;
      case LESSTHAN:
        leafPlan.setEndMarker(node.keyName, nameToType.get(node.keyName), node.value.toString(), !INCLUSIVE);
        break;
      case LESSTHANOREQUALTO:
        leafPlan.setEndMarker(node.keyName, nameToType.get(node.keyName), node.value.toString(), INCLUSIVE);
        break;
      case LIKE:
        leafPlan.ops.add(new Operator(Operator.Type.LIKE, node.keyName, node.value.toString()));
        break;
      case NOTEQUALS:
      case NOTEQUALS2:
        leafPlan.ops.add(new Operator(Operator.Type.NOTEQUALS, node.keyName, node.value.toString()));
        break;
      }
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

  public static PlanResult getFilterPlan(ExpressionTree exprTree, List<FieldSchema> parts) throws MetaException {
    if (exprTree == null) {
      // TODO: if exprTree is null, we should do what ObjectStore does. See HIVE-10102
      return new PlanResult(new ScanPlan(), true);
    }
    PartitionFilterGenerator pGenerator = new PartitionFilterGenerator(parts);
    exprTree.accept(pGenerator);
    return new PlanResult(pGenerator.getPlan(), pGenerator.hasUnsupportedCondition());
  }

}
