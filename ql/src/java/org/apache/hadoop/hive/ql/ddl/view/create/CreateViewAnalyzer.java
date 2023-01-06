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

package org.apache.hadoop.hive.ql.ddl.view.create;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for create view commands.
 */
@DDLType(types = HiveParser.TOK_CREATEVIEW)
public class CreateViewAnalyzer extends AbstractCreateViewAnalyzer {
  public CreateViewAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    TableName viewName = getQualifiedTableName((ASTNode) root.getChild(0));
    String fqViewName = viewName.getNotEmptyDbTable();
    LOG.info("Creating view " + fqViewName + " position=" + root.getCharPositionInLine());

    Map<Integer, ASTNode> children = new HashMap<>();
    for (int i = 1; i < root.getChildCount(); i++) {
      ASTNode child = (ASTNode) root.getChild(i);
      children.put(child.getToken().getType(), child);
    }

    List<FieldSchema> imposedSchema = children.containsKey(HiveParser.TOK_TABCOLNAME) ?
        getColumns((ASTNode) children.remove(HiveParser.TOK_TABCOLNAME)) : null;
    boolean ifNotExists = children.remove(HiveParser.TOK_IFNOTEXISTS) != null;
    boolean orReplace = children.remove(HiveParser.TOK_ORREPLACE) != null;
    String comment = children.containsKey(HiveParser.TOK_TABLECOMMENT) ?
        unescapeSQLString(children.remove(HiveParser.TOK_TABLECOMMENT).getChild(0).getText()) : null;
    ASTNode select = children.remove(HiveParser.TOK_QUERY);
    Map<String, String> properties = children.containsKey(HiveParser.TOK_TABLEPROPERTIES) ?
        getProps((ASTNode) children.remove(HiveParser.TOK_TABLEPROPERTIES).getChild(0)) : null;
    List<String> partitionColumnNames = children.containsKey(HiveParser.TOK_VIEWPARTCOLS) ?
        getColumnNames((ASTNode) children.remove(HiveParser.TOK_VIEWPARTCOLS).getChild(0)) : null;

    assert children.isEmpty();

    if (ifNotExists && orReplace) {
      throw new SemanticException("Can't combine IF NOT EXISTS and OR REPLACE.");
    }

    String originalText = ctx.getTokenRewriteStream().toString(select.getTokenStartIndex(), select.getTokenStopIndex());

    SemanticAnalyzer analyzer = analyzeQuery(select, fqViewName);

    schema = new ArrayList<FieldSchema>(analyzer.getResultSchema());
    ParseUtils.validateColumnNameUniqueness(
        analyzer.getOriginalResultSchema() == null ? schema : analyzer.getOriginalResultSchema());

    String expandedText = getExpandedText(imposedSchema, select, viewName);

    List<FieldSchema> partitionColumns = getPartitionColumns(imposedSchema, select, viewName,
        partitionColumnNames);
    setColumnAccessInfo(analyzer.getColumnAccessInfo());
    CreateViewDesc desc = new CreateViewDesc(fqViewName, schema, comment, properties, partitionColumnNames,
        ifNotExists, orReplace, originalText, expandedText, partitionColumns);
    validateCreateView(desc, analyzer);

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
    DDLUtils.addDbAndTableToOutputs(getDatabase(viewName.getDb()), viewName, TableType.VIRTUAL_VIEW, false,
        properties, outputs);
  }

  private String getExpandedText(List<FieldSchema> imposedSchema, ASTNode select, TableName viewName)
      throws SemanticException {
    if (imposedSchema != null) {
      if (imposedSchema.size() != schema.size()) {
        throw new SemanticException(SemanticAnalyzer.generateErrorMessage(select, ErrorMsg.VIEW_COL_MISMATCH.getMsg()));
      }
    }

    String expandedText = ctx.getTokenRewriteStream().toString(select.getTokenStartIndex(), select.getTokenStopIndex());

    if (imposedSchema != null) {
      // Merge the names from the imposed schema into the types from the derived schema.
      StringBuilder sb = new StringBuilder();
      sb.append("SELECT ");
      for (int i = 0; i < schema.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        // Modify a copy, not the original
        FieldSchema fieldSchema = new FieldSchema(schema.get(i));
        // TODO: there's a potential problem here if some table uses external schema like Avro,
        //       with a very large type name. It seems like the view does not derive the SerDe from
        //       the table, so it won't be able to just get the type from the deserializer like the
        //       table does; we won't be able to properly store the type in the RDBMS metastore.
        //       Not sure if these large cols could be in resultSchema. Ignore this for now 0_o
        schema.set(i, fieldSchema);
        sb.append(HiveUtils.unparseIdentifier(fieldSchema.getName(), conf));
        sb.append(" AS ");
        String imposedName = imposedSchema.get(i).getName();
        sb.append(HiveUtils.unparseIdentifier(imposedName, conf));
        fieldSchema.setName(imposedName);
        // We don't currently allow imposition of a type
        fieldSchema.setComment(imposedSchema.get(i).getComment());
      }
      sb.append(" FROM (");
      sb.append(expandedText);
      sb.append(") ");
      sb.append(HiveUtils.unparseIdentifier(viewName.getTable(), conf));

      expandedText = sb.toString();
    }
    return expandedText;
  }

  private List<FieldSchema> getPartitionColumns(List<FieldSchema> imposedSchema, ASTNode select, TableName viewName,
      List<String> partitionColumnNames) throws SemanticException {
    if (partitionColumnNames == null) {
      return null;
    }

    // Make sure all partitioning columns referenced actually exist and are in the correct order at the end of the
    // list of columns produced by the view. Also move the field schema descriptors from derivedSchema to the
    // partitioning key descriptor.
    if (partitionColumnNames.size() > schema.size()) {
      throw new SemanticException(ErrorMsg.VIEW_PARTITION_MISMATCH.getMsg());
    }

    // Get the partition columns from the end of derivedSchema.
    List<FieldSchema> partitionColumns =
        schema.subList(schema.size() - partitionColumnNames.size(), schema.size());

    // Verify that the names match the PARTITIONED ON clause.
    Iterator<String> columnNameIterator = partitionColumnNames.iterator();
    Iterator<FieldSchema> schemaIterator = partitionColumns.iterator();
    while (columnNameIterator.hasNext()) {
      String columnName = columnNameIterator.next();
      FieldSchema fieldSchema = schemaIterator.next();
      if (!fieldSchema.getName().equals(columnName)) {
        throw new SemanticException(ErrorMsg.VIEW_PARTITION_MISMATCH.getMsg());
      }
    }

    // Boundary case: require at least one non-partitioned column for consistency with tables.
    if (partitionColumnNames.size() == schema.size()) {
      throw new SemanticException(ErrorMsg.VIEW_PARTITION_TOTAL.getMsg());
    }

    // Now make a copy, and remove the partition columns from the end of derivedSchema.
    // (Clearing the subList writes through to the underlying derivedSchema ArrayList.)
    List<FieldSchema> partitionColumnsCopy = new ArrayList<FieldSchema>(partitionColumns);
    partitionColumns.clear();
    return partitionColumnsCopy;
  }

  private void validateCreateView(CreateViewDesc desc, SemanticAnalyzer analyzer) throws SemanticException {
    try {
      validateTablesUsed(analyzer);

      //replace view
      Table oldView = getTable(desc.getViewName(), false);
      if (desc.isReplace() && oldView != null) {
        if (oldView.getTableType().equals(TableType.MATERIALIZED_VIEW)) {
          throw new SemanticException(ErrorMsg.REPLACE_MATERIALIZED_WITH_VIEW, oldView.getTableName());
        }

        // Existing table is not a view
        if (!oldView.getTableType().equals(TableType.VIRTUAL_VIEW) &&
            !oldView.getTableType().equals(TableType.MATERIALIZED_VIEW)) {
          String tableNotViewErrorMsg = "The following is an existing table, not a view: " + desc.getViewName();
          throw new SemanticException(ErrorMsg.EXISTING_TABLE_IS_NOT_VIEW.getMsg(tableNotViewErrorMsg));
        }

        validateReplaceWithPartitions(desc.getViewName(), oldView, desc.getPartitionColumns());
      }
    } catch (HiveException e) {
      throw new SemanticException(e.getMessage(), e);
    }
  }
}
