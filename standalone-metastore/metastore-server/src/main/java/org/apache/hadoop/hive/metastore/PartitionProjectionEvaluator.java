/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import static org.apache.hadoop.hive.metastore.MetastoreDirectSqlUtils.extractSqlLong;

/**
 * Evaluator for partition projection filters which specify parts of the partition that should be
 * used using dot notation for fields.
 */
public class PartitionProjectionEvaluator {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionProjectionEvaluator.class);
  private final boolean convertMapNullsToEmptyStrings;
  private final boolean isView;
  private final String includeParamKeyPattern;
  private final String excludeParamKeyPattern;
  private Set<String> projectionFields;

  interface PartitionFieldValueSetter<T> {
    void setValue(T part, PartitionFieldNode node, Object value) throws MetaException;
  }

  private final ImmutableMap<String, MutivaluedFieldSetter> multiValuedFieldSetters =
      new ImmutableMap.Builder<String, MutivaluedFieldSetter>()
          .put("values", new PartitionValuesSetter())
          .put("parameters", new PartitionParametersSetter())
          .put("sd.cols", new PartitionSDColsSetter())
          .put("sd.bucketCols", new PartitionSDBucketColsSetter())
          .put("sd.sortCols", new PartitionSortColsSetter())
          .put("sd.parameters", new PartitionSDParametersSetter())
          .put("sd.serdeInfo.parameters", new PartitionSerdeInfoParametersSetter())
          .put("sd.skewedInfo.skewedColNames", new PartitionSkewedColsNamesSetter())
          .put("sd.skewedInfo.skewedColValues", new PartitionSkewedColsValuesSetter())
          .put("sd.skewedInfo.skewedColValueLocationMaps",
              new PartitionSkewedColValLocationMapSetter()).build();

  private static final String PART_ID = "PART_ID";
  private static final String SD_ID = "SD_ID";
  private static final String SERDE_ID = "SERDE_ID";
  private static final String CD_ID = "CD_ID";

  private static final PartitionFieldNode partIdNode = new PartitionFieldNode(PART_ID);
  private static final PartitionFieldNode sdIdNode = new PartitionFieldNode(SD_ID);
  private static final PartitionFieldNode serdeIdNode = new PartitionFieldNode(SERDE_ID);
  private static final PartitionFieldNode cdIdNode = new PartitionFieldNode(CD_ID);

  private final ImmutableMap<String, String> fieldNameToTableName;
  private final Set<PartitionFieldNode> roots;
  private final String PARTITIONS;
  private final String SDS;
  private final String SERDES;
  private final String PARTITION_PARAMS;
  private final PersistenceManager pm;

  @VisibleForTesting static final String SD_PATTERN = "sd|sd\\.";
  @VisibleForTesting static final String SERDE_PATTERN = "sd\\.serdeInfo|sd\\.serdeInfo\\.";
  @VisibleForTesting static final String CD_PATTERN = "sd\\.cols|sd\\.cols\\.";

  private static final int SD_INDEX = 0;
  private static final int CD_INDEX = 1;
  private static final int SERDE_INDEX = 2;
  private static final int PART_INDEX = 3;

  // this map stores all the single valued fields in the Partition class and maps them to the corresponding
  // single-valued fields from the MPartition class. This map is used to parse the given partition fields
  // as well as to convert a given partition field list to a JDO setResult string when direct-SQL
  // is disabled
  private static final ImmutableMap<String, String> allPartitionSingleValuedFields = new ImmutableMap.Builder<String, String>()
      .put("dbName", "table.database.name")
      .put("tableName", "table.tableName")
      .put("createTime", "createTime")
      .put("lastAccessTime", "lastAccessTime")
      .put("sd.location", "sd.location")
      .put("sd.inputFormat", "sd.inputFormat")
      .put("sd.outputFormat", "sd.outputFormat")
      .put("sd.compressed", "sd.isCompressed")
      .put("sd.numBuckets", "sd.numBuckets")
      .put("sd.serdeInfo.name", "sd.serDeInfo.name")
      .put("sd.serdeInfo.serializationLib", "sd.serDeInfo.serializationLib")
      .put("sd.serdeInfo.description", "sd.serDeInfo.description")
      .put("sd.serdeInfo.serializerClass", "sd.serDeInfo.serializerClass")
      .put("sd.serdeInfo.deserializerClass", "sd.serDeInfo.deserializerClass")
      .put("sd.serdeInfo.serdeType", "sd.serDeInfo.serdeType")
      .put("catName", "table.database.catalogName")
      .put("writeId", "writeId")
      //TODO there is no mapping for isStatsCompliant to JDO MPartition
      //.put("isStatsCompliant", "isStatsCompliant")
      .build();

  private static final ImmutableSet<String> allPartitionMultiValuedFields = new ImmutableSet.Builder<String>()
      .add("values")
      .add("sd.cols.name")
      .add("sd.cols.type")
      .add("sd.cols.comment")
      .add("sd.serdeInfo.parameters")
      .add("sd.bucketCols")
      .add("sd.sortCols.col")
      .add("sd.sortCols.order")
      .add("sd.parameters")
      .add("sd.skewedInfo.skewedColNames")
      .add("sd.skewedInfo.skewedColValues")
      .add("sd.skewedInfo.skewedColValueLocationMaps")
      .add("parameters")
      .add("privileges.userPrivileges")
      .add("privileges.groupPrivileges")
      .add("privileges.rolePrivileges")
      .build();

  private static final ImmutableSet<String> allPartitionFields = new ImmutableSet.Builder<String>()
      .addAll(allPartitionSingleValuedFields.keySet())
      .addAll(allPartitionMultiValuedFields)
      .build();

  public PartitionProjectionEvaluator(PersistenceManager pm,
      ImmutableMap<String, String> fieldNameToTableName, List<String> projectionFields,
      boolean convertMapNullsToEmptyStrings, boolean isView, String includeParamKeyPattern,
      String excludeParamKeyPattern) throws MetaException {
    this.pm = pm;
    this.fieldNameToTableName = fieldNameToTableName;
    this.convertMapNullsToEmptyStrings = convertMapNullsToEmptyStrings;
    this.isView = isView;
    this.includeParamKeyPattern = includeParamKeyPattern;
    this.excludeParamKeyPattern = excludeParamKeyPattern;
    this.PARTITIONS =
        fieldNameToTableName.containsKey("PARTITIONS_TABLE_NAME") ? fieldNameToTableName
            .get("PARTITIONS_TABLE_NAME") : "\"PARTITIONS\"";
    this.SDS = fieldNameToTableName.containsKey("SDS_TABLE_NAME") ? fieldNameToTableName
        .get("SDS_TABLE_NAME") : "\"SDS\"";
    this.SERDES = fieldNameToTableName.containsKey("SERDES_TABLE_NAME") ? fieldNameToTableName
        .get("SERDES_TABLE_NAME") : "\"SERDES\"";
    this.PARTITION_PARAMS =
        fieldNameToTableName.containsKey("PARTITION_PARAMS") ? fieldNameToTableName
            .get("PARTITION_PARAMS") : "\"PARTITION_PARAMS\"";

    roots = parse(projectionFields);

    // we always query PART_ID
    roots.add(partIdNode);
    if (find(SD_PATTERN)) {
      roots.add(sdIdNode);
    }
    if (find(SERDE_PATTERN)) {
      roots.add(serdeIdNode);
    }
    if (find(CD_PATTERN)) {
      roots.add(cdIdNode);
    }
  }

  /**
   * Given a Java regex string pattern, checks if the the partitionFieldNode tree
   * has any node whose fieldName matches the given pattern
   * @param searchField
   * @return
   */
  @VisibleForTesting
  boolean find(String searchField) {
    Pattern p = Pattern.compile(searchField);
    for (PartitionFieldNode node : roots) {
      if (find(node, p)) {
        return true;
      }
    }
    return false;
  }

  private static boolean find(PartitionFieldNode root, Pattern p) {
    if (root == null) {
      return false;
    }
    if (p.matcher(root.fieldName).matches()) {
      return true;
    }
    for (PartitionFieldNode child : root.children) {
      if (find(child, p)) {
        return true;
      }
    }
    return false;
  }

  /**
   * if top level field name is given expand the top level field such that all the children
   * of that node are added to the projection list. eg. if only "sd" is provided in the projection
   * list, it means all the nested fields for sd should be added to the projection fields
   * @param projectionList
   * @return
   */
  private static Set<String> expand(Collection<String> projectionList) throws MetaException {
    Set<String> result = new HashSet<>();
    for (String projectedField : projectionList) {
      if (allPartitionFields.contains(projectedField)) {
        result.add(projectedField);
      } else {
        boolean found = false;
        for (String partitionField : allPartitionFields) {
          if (partitionField.startsWith(projectedField)) {
            LOG.debug("Found " + partitionField + " included within given projection field "
                + projectedField);
            result.add(partitionField);
            found = true;
          }
        }
        if (!found) {
          throw new MetaException("Invalid field name " + projectedField);
        }
      }
    }
    return result;
  }

  @VisibleForTesting
  Set<PartitionFieldNode> getRoots() {
    return roots;
  }

  private static void validate(Collection<String> projectionFields) throws MetaException {
    Set<String> verify = new HashSet<>(projectionFields);
    verify.removeAll(allPartitionFields);
    if (verify.size() > 0) {
      throw new MetaException("Invalid partition fields in the projection spec" + Arrays
          .toString(verify.toArray(new String[verify.size()])));
    }
  }

  private Set<PartitionFieldNode> parse(List<String> inputProjectionFields) throws MetaException {
    // in case of dbName and tableName we rely on table object to get their values
    this.projectionFields = new HashSet<>(inputProjectionFields);
    projectionFields.remove("dbName");
    projectionFields.remove("tableName");
    projectionFields.remove("catName");
    if (isView) {
      // if this is a view SDs are not set so can be skipped
      projectionFields.removeIf(
          s -> s.matches(SD_PATTERN) || s.matches(SERDE_PATTERN) || s.matches(CD_PATTERN));
    }
    // remove redundant fields
    projectionFields = PartitionProjectionEvaluator.expand(projectionFields);
    removeUnsupportedFields();
    validate(projectionFields);

    Map<String, PartitionFieldNode> nestedNodes = new HashMap<>();
    Set<PartitionFieldNode> rootNodes = new HashSet<>();

    for (String projectedField : projectionFields) {
      String[] fields = projectedField.split("\\.");
      if (fields.length == 0) {
        LOG.warn("Invalid projected field {}. Ignoring ..", projectedField);
        continue;
      }
      StringBuilder fieldNameBuilder = new StringBuilder(fields[0]);
      PartitionFieldNode currentNode = createIfNotExists(nestedNodes, fieldNameBuilder.toString());
      rootNodes.add(currentNode);
      for (int level = 1; level < fields.length; level++) {
        final String name = fieldNameBuilder.append(".").append(fields[level]).toString();
        PartitionFieldNode childNode = createIfNotExists(nestedNodes, name);
        // all the children of a multi-valued nodes are also multi-valued
        if (currentNode.isMultiValued) {
          childNode.setMultiValued();
        }
        currentNode.addChild(childNode);
        currentNode = childNode;
      }
    }
    return rootNodes;
  }

  // TODO some of the optional partition fields are never set by DirectSQL implementation
  // Removing such fields to keep it consistent with methods in MetastoreDirectSQL class
  private void removeUnsupportedFields() {
    List<String> unsupportedFields = Arrays
        .asList("sd.serdeInfo.serializerClass", "sd.serdeInfo.deserializerClass",
            "sd.serdeInfo.serdeType", "sd.serdeInfo.description");
    for (String unsupportedField : unsupportedFields) {
      if (projectionFields.contains(unsupportedField)) {
        LOG.warn("DirectSQL does not return partitions with the optional field" + unsupportedField
            + " set. Removing it from the projection list");
        projectionFields.remove(unsupportedField);
      }
    }
  }

  private PartitionFieldNode createIfNotExists(Map<String, PartitionFieldNode> nestedNodes,
      String fieldName) {
    PartitionFieldNode currentNode = nestedNodes.computeIfAbsent(fieldName, k -> {
      if (multiValuedFieldSetters.containsKey(fieldName)) {
        return new PartitionFieldNode(fieldName, true);
      } else {
        return new PartitionFieldNode(fieldName);
      }
    });
    return currentNode;
  }

  /**
   * Given a list of partition ids, returns the List of partially filled partitions based on the
   * projection list used to instantiate this PartitionProjectionEvaluator
   * @param partitionIds List of partition ids corresponding to the Partitions objects which are requested
   * @return Partitions where each partition has only the projected fields set
   * @throws MetaException
   */
  public List<Partition> getPartitionsUsingProjectionList(List<Long> partitionIds)
      throws MetaException {
    TreeMap<Long, StorageDescriptor> sds = new TreeMap<>();
    TreeMap<Long, List<FieldSchema>> cds = new TreeMap<>();
    TreeMap<Long, SerDeInfo> serdes = new TreeMap<>();
    TreeMap<Long, Partition> partitions = new TreeMap<>();
    List<Partition> results = setSingleValuedFields(partitionIds, partitions, sds, serdes, cds);
    setMultivaluedFields(partitions, sds, serdes, cds);
    return results;
  }

  private List<Partition> setSingleValuedFields(List<Long> partitionIds,
      final TreeMap<Long, Partition> partitions, final TreeMap<Long, StorageDescriptor> sdIds,
      final TreeMap<Long, SerDeInfo> serdeIds, final TreeMap<Long, List<FieldSchema>> cdIds)
      throws MetaException {

    StringBuilder queryTextBuilder = new StringBuilder();
    int numColumns = buildQueryForSingleValuedFields(partitionIds, queryTextBuilder);
    String queryText = queryTextBuilder.toString();

    Query<?> query = pm.newQuery("javax.jdo.query.SQL", queryText);
    try {
      long start = LOG.isDebugEnabled() ? System.nanoTime() : 0;
      List<Object> sqlResult = MetastoreDirectSqlUtils.executeWithArray(query, null, queryText);
      long queryTime = LOG.isDebugEnabled() ? System.nanoTime() : 0;
      MetastoreDirectSqlUtils.timingTrace(LOG.isDebugEnabled(), queryText, start, queryTime);
      Deadline.checkTimeout();
      final Long[] ids = new Long[4];
      Object[] rowVals = new Object[1];
      // Keep order by name, consistent with JDO.
      ArrayList<Partition> orderedResult = new ArrayList<Partition>(partitionIds.size());
      for (Object row : sqlResult) {
        if (numColumns > 1) {
          rowVals = (Object[])row;
        } else {
          // only one column is selected by query. The result class will be Object
          rowVals[0] = row;
        }
        Partition part = new Partition();
        for (PartitionFieldNode root : roots) {
          traverseAndSetValues(part, root, rowVals, new PartitionFieldValueSetter() {
            @Override
            public void setValue(Object partition, PartitionFieldNode node, Object value)
                throws MetaException {
              if (!node.isMultiValued) {
                // in case of serdeid and sdId node we just collect the sdIds for further processing
                if (node.equals(sdIdNode)) {
                  ids[SD_INDEX] = extractSqlLong(value);
                } else if (node.equals(serdeIdNode)) {
                  ids[SERDE_INDEX] = extractSqlLong(value);
                } else if (node.equals(cdIdNode)) {
                  ids[CD_INDEX] = extractSqlLong(value);
                } else if (node.equals(partIdNode)) {
                  ids[PART_INDEX] = extractSqlLong(value);
                } else {
                  // incase of sd.compressed and sd.storedAsSubDirectories we need special code to convert
                  // string to a boolean value
                  if (node.fieldName.equals("sd.compressed") || node.fieldName.equals("sd.storedAsSubDirectories")) {
                    value = MetastoreDirectSqlUtils.extractSqlBoolean(value);
                  } else if (node.fieldName.equals("lastAccessTime") || node.fieldName.equals("createTime")) {
                    value = MetastoreDirectSqlUtils.extractSqlInt(value);
                  }
                  MetaStoreServerUtils.setNestedProperty(partition, node.fieldName, value, true);
                }
              }
            }
          });
        }
        // PART_ID is always queried
        if (ids[PART_INDEX] == null) {
          throw new MetaException("Could not find PART_ID for partition " + part);
        }
        partitions.put(ids[PART_INDEX], part);
        orderedResult.add(part);
        ids[PART_INDEX] = null;

        if (ids[SD_INDEX] != null) {
          // sd object is initialized if any of the sd single-valued fields are in the projection
          if (part.getSd() == null) {
            part.setSd(new StorageDescriptor());
          }
          sdIds.put(ids[SD_INDEX], part.getSd());
          ids[SD_INDEX] = null;
        }

        if (ids[SERDE_INDEX] != null) {
          // serde object must have already been intialized above in MetaStoreUtils.setNestedProperty call
          if (part.getSd().getSerdeInfo() == null) {
            part.getSd().setSerdeInfo(new SerDeInfo());
          }
          serdeIds.put(ids[SERDE_INDEX], part.getSd().getSerdeInfo());
          ids[SERDE_INDEX] = null;
        }

        if (ids[CD_INDEX] != null) {
          // common case is all the SDs will reuse the same CD
          // allocate List<FieldSchema> only when you see a new CD_ID
          cdIds.putIfAbsent(ids[CD_INDEX], new ArrayList<>(5));
          if (part.getSd().getCols() == null) {
            part.getSd().setCols(cdIds.get(ids[CD_INDEX]));
          }
          ids[CD_INDEX] = null;
        }
        Deadline.checkTimeout();
      }
      return orderedResult;
    } catch (Exception e) {
      LOG.error("Exception received while getting partitions using projected fields", e);
      throw new MetaException(e.getMessage());
    } finally {
      query.closeAll();
    }
  }

  private void setMultivaluedFields(TreeMap<Long, Partition> partitions,
      TreeMap<Long, StorageDescriptor> sds, TreeMap<Long, SerDeInfo> serdes,
      TreeMap<Long, List<FieldSchema>> cds) throws MetaException {
    for (PartitionFieldNode root : roots) {
      traverseAndSetMultiValuedFields(root, partitions, sds, serdes, cds);
    }
  }

  private void traverseAndSetMultiValuedFields(PartitionFieldNode root,
      TreeMap<Long, Partition> partitions, TreeMap<Long, StorageDescriptor> sds,
      TreeMap<Long, SerDeInfo> serdes, TreeMap<Long, List<FieldSchema>> cds) throws MetaException {
    if (root == null) {
      return;
    }
    // if a multi-valued node is found set its value using its value-setters
    // note that once a multi-valued node is found the method does not recurse further
    // this is because the multi-valued setter also sets the values of all its descendents
    if (root.isMultiValued) {
      MutivaluedFieldSetter multiValuedFieldSetter = multiValuedFieldSetters.get(root.fieldName);
      if (multiValuedFieldSetter == null) {
        throw new MetaException(
            "Cannot find multi-valued field setter for field " + root.fieldName);
      }
      multiValuedFieldSetter.setValue(root, partitions, sds, serdes, cds);
    } else {
      for (PartitionFieldNode child : root.children) {
        traverseAndSetMultiValuedFields(child, partitions, sds, serdes, cds);
      }
    }
  }

  private void traverseAndSetValues(Partition part, PartitionFieldNode root, Object[] row,
      PartitionFieldValueSetter valueSetter) throws MetaException {
    // if root is null or is multiValued, do not recurse further
    // multi-valued fields are set separately in setMultiValuedFields method
    if (root == null || root.isMultiValued()) {
      return;
    }
    if (root.isLeafNode()) {
      valueSetter.setValue(part, root, row[root.fieldIndex]);
      return;
    }
    for (PartitionFieldNode child : root.children) {
      traverseAndSetValues(part, child, row, valueSetter);
    }
  }

  private static final String SPACE = " ";

  private int buildQueryForSingleValuedFields(List<Long> partitionIds, StringBuilder queryTextBuilder) {
    queryTextBuilder.append("select ");
    // build projection columns using the ProjectedFields
    // it should not matter if you select all the
    List<String> columnList = getSingleValuedColumnNames(roots);
    queryTextBuilder.append(Joiner.on(',').join(columnList));
    queryTextBuilder.append(SPACE);
    queryTextBuilder.append("from " + PARTITIONS);
    // if SD fields are selected add join clause with SDS
    boolean foundSD = false;
    if (find(SD_PATTERN)) {
      queryTextBuilder.append(SPACE);
      queryTextBuilder.append(
          "left outer join " + SDS + " on " + PARTITIONS + ".\"SD_ID\" = " + SDS + ".\"SD_ID\"");
      foundSD = true;
    }
    // if serde fields are selected add join clause on serdes
    if (foundSD || find(SERDE_PATTERN)) {
      queryTextBuilder.append(SPACE);
      queryTextBuilder.append(
          "  left outer join " + SERDES + " on " + SDS + ".\"SERDE_ID\" = " + SERDES
              + ".\"SERDE_ID\"");
    }
    queryTextBuilder.append(SPACE);
    //add where clause
    queryTextBuilder.append("where \"PART_ID\" in (" + Joiner.on(',').join(partitionIds)
        + ") order by \"PART_NAME\" asc");
    return columnList.size();
  }

  private int getSingleValuedColumnName(PartitionFieldNode root, int fieldId,
      final List<String> columnNames) {
    if (root == null) {
      return fieldId;
    }
    if (root.isLeafNode() && !root.isMultiValued) {
      if (fieldNameToTableName.containsKey(root.fieldName)) {
        columnNames.add(fieldNameToTableName.get(root.fieldName));
        root.setFieldIndex(fieldId++);
        return fieldId;
      }
      throw new RuntimeException(
          "No column name mapping found for partition field " + root.fieldName);
    }
    for (PartitionFieldNode child : root.children) {
      fieldId = getSingleValuedColumnName(child, fieldId, columnNames);
    }
    return fieldId;
  }

  private List<String> getSingleValuedColumnNames(Set<PartitionFieldNode> roots) {
    List<String> columnNames = new ArrayList<>();
    int fieldIndex = 0;
    for (PartitionFieldNode node : roots) {
      fieldIndex = getSingleValuedColumnName(node, fieldIndex, columnNames);
    }
    return columnNames;
  }


  private static void getNestedFieldName(JsonNode jsonNode, String fieldName,
      Collection<String> results) {
    if (jsonNode instanceof ArrayNode) {
      Iterator<JsonNode> elements = ((ArrayNode) jsonNode).elements();
      if (!elements.hasNext()) {
        results.add(fieldName);
        return;
      }
      while (elements.hasNext()) {
        JsonNode element = elements.next();
        getNestedFieldName(element, fieldName, results);
      }
    } else {
      Iterator<Entry<String, JsonNode>> fields = jsonNode.fields();
      if (!fields.hasNext()) {
        results.add(fieldName);
        return;
      }
      while (fields.hasNext()) {
        Entry<String, JsonNode> fieldKV = fields.next();
        String key = fieldKV.getKey();
        getNestedFieldName(fieldKV.getValue(), fieldName.length() == 0 ? key : fieldName + "." + key,
            results);
      }
    }
  }

  static class PartitionFieldNode {
    private String fieldName;
    private Set<PartitionFieldNode> children = new HashSet<>(4);
    private boolean isMultiValued;
    private int fieldIndex;

    PartitionFieldNode(String fieldName) {
      this.fieldName = fieldName;
      isMultiValued = false;
    }

    PartitionFieldNode(String fieldName, boolean isMultiValued) {
      this.fieldName = fieldName;
      this.isMultiValued = isMultiValued;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      PartitionFieldNode that = (PartitionFieldNode) o;
      return Objects.equals(fieldName, that.fieldName);
    }

    boolean isLeafNode() {
      return children == null || children.isEmpty();
    }

    void setFieldIndex(int fieldIndex) {
      this.fieldIndex = fieldIndex;
    }

    @VisibleForTesting
    void addChild(PartitionFieldNode child) {
      children.add(child);
    }

    @VisibleForTesting
    String getFieldName() {
      return fieldName;
    }

    @VisibleForTesting
    Set<PartitionFieldNode> getChildren() {
      return new HashSet<>(children);
    }

    @VisibleForTesting
    boolean isMultiValued() {
      return isMultiValued;
    }

    @Override
    public String toString() {
      return fieldName;
    }

    @Override
    public int hashCode() {
      return Objects.hash(fieldName);
    }

    void setMultiValued() {
      this.isMultiValued = true;
    }
  }

  private interface MutivaluedFieldSetter {
    void setValue(PartitionFieldNode root, TreeMap<Long, Partition> partitions, TreeMap<Long, StorageDescriptor> sds,
        TreeMap<Long, SerDeInfo> serdes, TreeMap<Long, List<FieldSchema>> cds) throws MetaException;
  }

  private class PartitionValuesSetter implements MutivaluedFieldSetter {
    private PartitionValuesSetter() {
      //
    }

    @Override
    public void setValue(PartitionFieldNode root, TreeMap<Long, Partition> partitions, TreeMap<Long, StorageDescriptor> sds,
        TreeMap<Long, SerDeInfo> serdes, TreeMap<Long, List<FieldSchema>> cds)
        throws MetaException {
      final String tableName =
          fieldNameToTableName.containsKey("PARTITION_KEY_VALS") ? fieldNameToTableName
              .get("PARTITION_KEY_VALS") : "\"PARTITION_KEY_VALS\"";
      MetastoreDirectSqlUtils
          .setPartitionValues(tableName, pm, Joiner.on(',').join(partitions.keySet()), partitions);
    }
  }

  private class PartitionParametersSetter implements MutivaluedFieldSetter {
    private PartitionParametersSetter() {
      //
    }

    @Override
    public void setValue(PartitionFieldNode root, TreeMap<Long, Partition> partitions, TreeMap<Long, StorageDescriptor> sds,
        TreeMap<Long, SerDeInfo> serdes, TreeMap<Long, List<FieldSchema>> cds)
        throws MetaException {
      MetastoreDirectSqlUtils
          .setPartitionParametersWithFilter(PARTITION_PARAMS, convertMapNullsToEmptyStrings, pm,
              Joiner.on(',').join(partitions.keySet()), partitions, includeParamKeyPattern,
              excludeParamKeyPattern);
    }
  }

  private class PartitionSDColsSetter implements MutivaluedFieldSetter {
    private PartitionSDColsSetter() {
      // prevent instantiation
    }

    @Override
    public void setValue(PartitionFieldNode root, TreeMap<Long, Partition> partitions, TreeMap<Long, StorageDescriptor> sds,
        TreeMap<Long, SerDeInfo> serdes, TreeMap<Long, List<FieldSchema>> cds)
        throws MetaException {
      // find the fields which are requested for sd.cols
      // children field names would be sd.cols.name, sd.cols.type or sd.cols.description
      List<String> childFields = getChildrenFieldNames(root);
      final String tableName = fieldNameToTableName.containsKey("COLUMNS_V2") ? fieldNameToTableName
          .get("COLUMNS_V2") : "\"COLUMNS_V2\"";
      MetastoreDirectSqlUtils
          .setSDCols(tableName, childFields, pm, cds, Joiner.on(',').join(cds.keySet()));
    }
  }

  private class PartitionSDBucketColsSetter implements MutivaluedFieldSetter {
    private PartitionSDBucketColsSetter() {
      //
    }

    @Override
    public void setValue(PartitionFieldNode root, TreeMap<Long, Partition> partitions, TreeMap<Long, StorageDescriptor> sds,
        TreeMap<Long, SerDeInfo> serdes, TreeMap<Long, List<FieldSchema>> cds)
        throws MetaException {
      final String tableName =
          fieldNameToTableName.containsKey("BUCKETING_COLS") ? fieldNameToTableName
              .get("BUCKETING_COLS") : "\"BUCKETING_COLS\"";
      MetastoreDirectSqlUtils
          .setSDBucketCols(tableName, pm, sds, Joiner.on(',').join(sds.keySet()));
    }
  }

  private class PartitionSortColsSetter implements MutivaluedFieldSetter {
    private PartitionSortColsSetter() {
      //
    }

    @Override
    public void setValue(PartitionFieldNode root, TreeMap<Long, Partition> partitions, TreeMap<Long, StorageDescriptor> sds,
        TreeMap<Long, SerDeInfo> serdes, TreeMap<Long, List<FieldSchema>> cds)
        throws MetaException {
      List<String> childFieldNames = getChildrenFieldNames(root);
      final String tableName = fieldNameToTableName.containsKey("SORT_COLS") ? fieldNameToTableName
          .get("SORT_COLS") : "\"SORT_COLS\"";
      MetastoreDirectSqlUtils
          .setSDSortCols(tableName, childFieldNames, pm, sds, Joiner.on(',').join(sds.keySet()));
    }
  }

  private List<String> getChildrenFieldNames(PartitionFieldNode root) throws MetaException {
    List<String> childFields = new ArrayList<>(3);
    for (PartitionFieldNode child : root.getChildren()) {
      if (child.getFieldName().lastIndexOf(".") < 0) {
        throw new MetaException("Error parsing multi-valued field name " + child.getFieldName());
      }
      childFields.add(child.getFieldName().substring(child.getFieldName().lastIndexOf(".") + 1));
    }
    return childFields;
  }

  private class PartitionSDParametersSetter implements MutivaluedFieldSetter {
    private PartitionSDParametersSetter() {
      //
    }

    @Override
    public void setValue(PartitionFieldNode root, TreeMap<Long, Partition> partitions, TreeMap<Long, StorageDescriptor> sds,
        TreeMap<Long, SerDeInfo> serdes, TreeMap<Long, List<FieldSchema>> cds)
        throws MetaException {
      final String tableName = fieldNameToTableName.containsKey("SD_PARAMS") ? fieldNameToTableName
          .get("SD_PARAMS") : "\"SD_PARAMS\"";
      MetastoreDirectSqlUtils.setSDParameters(tableName, convertMapNullsToEmptyStrings, pm, sds,
          Joiner.on(',').join(sds.keySet()));
    }
  }

  private class PartitionSerdeInfoParametersSetter implements MutivaluedFieldSetter {
    private PartitionSerdeInfoParametersSetter() {
      //
    }

    @Override
    public void setValue(PartitionFieldNode root, TreeMap<Long, Partition> partitions, TreeMap<Long, StorageDescriptor> sds,
        TreeMap<Long, SerDeInfo> serdes, TreeMap<Long, List<FieldSchema>> cds)
        throws MetaException {
      final String tableName =
          fieldNameToTableName.containsKey("SERDE_PARAMS") ? fieldNameToTableName
              .get("SERDE_PARAMS") : "\"SERDE_PARAMS\"";
      MetastoreDirectSqlUtils.setSerdeParams(tableName, convertMapNullsToEmptyStrings, pm, serdes,
          Joiner.on(',').join(serdes.keySet()));
    }
  }

  private class PartitionSkewedColsNamesSetter implements MutivaluedFieldSetter {
    private PartitionSkewedColsNamesSetter() {
      //
    }

    @Override
    public void setValue(PartitionFieldNode root, TreeMap<Long, Partition> partitions, TreeMap<Long, StorageDescriptor> sds,
        TreeMap<Long, SerDeInfo> serdes, TreeMap<Long, List<FieldSchema>> cds)
        throws MetaException {
      final String tableName =
          fieldNameToTableName.containsKey("SKEWED_COL_NAMES") ? fieldNameToTableName
              .get("SKEWED_COL_NAMES") : "\"SKEWED_COL_NAMES\"";
      MetastoreDirectSqlUtils
          .setSkewedColNames(tableName, pm, sds, Joiner.on(',').join(sds.keySet()));
    }
  }

  private class PartitionSkewedColsValuesSetter implements MutivaluedFieldSetter {
    private PartitionSkewedColsValuesSetter() {
      //
    }

    @Override
    public void setValue(PartitionFieldNode root, TreeMap<Long, Partition> partitions, TreeMap<Long, StorageDescriptor> sds,
        TreeMap<Long, SerDeInfo> serdes, TreeMap<Long, List<FieldSchema>> cds)
        throws MetaException {
      final String skewedStringListVals =
          fieldNameToTableName.containsKey("SKEWED_STRING_LIST_VALUES") ? fieldNameToTableName
              .get("SKEWED_STRING_LIST_VALUES") : "\"SKEWED_STRING_LIST_VALUES\"";
      final String skewedVals =
          fieldNameToTableName.containsKey("SKEWED_VALUES") ? fieldNameToTableName
              .get("SKEWED_VALUES") : "\"SKEWED_VALUES\"";
      MetastoreDirectSqlUtils.setSkewedColValues(skewedStringListVals, skewedVals, pm, sds,
          Joiner.on(',').join(sds.keySet()));
    }
  }

  private class PartitionSkewedColValLocationMapSetter implements MutivaluedFieldSetter {
    private PartitionSkewedColValLocationMapSetter() {
      //
    }

    @Override
    public void setValue(PartitionFieldNode root, TreeMap<Long, Partition> partitions, TreeMap<Long, StorageDescriptor> sds,
        TreeMap<Long, SerDeInfo> serdes, TreeMap<Long, List<FieldSchema>> cds)
        throws MetaException {
      final String skewedStringListVals =
          fieldNameToTableName.containsKey("SKEWED_STRING_LIST_VALUES") ? fieldNameToTableName
              .get("SKEWED_STRING_LIST_VALUES") : "\"SKEWED_STRING_LIST_VALUES\"";
      final String skewedColValLocMap =
          fieldNameToTableName.containsKey("SKEWED_COL_VALUE_LOC_MAP") ? fieldNameToTableName
              .get("SKEWED_COL_VALUE_LOC_MAP") : "\"SKEWED_COL_VALUE_LOC_MAP\"";
      MetastoreDirectSqlUtils
          .setSkewedColLocationMaps(skewedColValLocMap, skewedStringListVals, pm, sds,
              Joiner.on(',').join(sds.keySet()));
    }
  }

  /**
   * Given a list of partition fields, checks if all the fields requested are single-valued. If all
   * the fields are single-valued returns list of equivalent MPartition fieldnames
   * which can be used in the setResult clause of a JDO query
   *
   * @param partitionFields List of partitionFields in the projection
   * @return List of JDO field names which can be used in setResult clause
   * of a JDO query. Returns null if input partitionFields cannot be used in a setResult clause
   */
  public static List<String> getMPartitionFieldNames(List<String> partitionFields)
      throws MetaException {
    // if there are no partitionFields requested, it means all the fields are requested which include
    // multi-valued fields.
    if (partitionFields == null || partitionFields.isEmpty()) {
      return null;
    }
    // throw exception if there are invalid field names
    PartitionProjectionEvaluator.validate(partitionFields);
    // else, check if all the fields are single-valued. In case there are multi-valued fields requested
    // return null since setResult in JDO doesn't support multi-valued fields
    if (!allPartitionSingleValuedFields.keySet().containsAll(partitionFields)) {
      return null;
    }
    List<String> jdoFields = new ArrayList<>(partitionFields.size());
    for (String partitionField : partitionFields) {
      jdoFields.add(allPartitionSingleValuedFields.get(partitionField));
    }
    return jdoFields;
  }
}
