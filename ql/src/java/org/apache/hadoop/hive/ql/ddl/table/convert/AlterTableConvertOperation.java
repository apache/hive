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

package org.apache.hadoop.hive.ql.ddl.table.convert;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.Map;

import static org.apache.hadoop.hive.metastore.TransactionalValidationListener.DEFAULT_TRANSACTIONAL_PROPERTY;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_TRANSACTIONAL;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES;

/**
 * Operation process of ALTER TABLE ... CONVERT command
 */
public class AlterTableConvertOperation extends AbstractAlterTableOperation<AlterTableConvertDesc> {

  public enum ConversionFormats {
    ICEBERG(ImmutableMap.of(META_TABLE_STORAGE, "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler")),
    ACID(ImmutableMap.of(TABLE_IS_TRANSACTIONAL, "true", TABLE_TRANSACTIONAL_PROPERTIES,
        DEFAULT_TRANSACTIONAL_PROPERTY));

    private final Map<String, String> properties;

    ConversionFormats(Map<String, String> properties) {
      this.properties = properties;
    }


    public Map<String, String> properties() {
      return properties;
    }
  }

  public AlterTableConvertOperation(DDLOperationContext context, AlterTableConvertDesc desc) {
    super(context, desc);
  }

  @Override
  protected void doAlteration(Table table, Partition partition) throws HiveException {
    // Add the covert type
    String convertType = desc.getConvertSpec().getTargetType();
    ConversionFormats format = ConversionFormats.valueOf(convertType.toUpperCase());

    // Check the properties don't already exist, in that case we need not do any conversion
    validatePropertiesAlreadyExist(format, table.getParameters());

    // Add the conversion related table properties.
    table.getParameters().putAll(format.properties());

    // Add any additional table properties, if specified with the convert command.
    if (desc.getConvertSpec().getTblProperties() != null) {
      table.getParameters().putAll(desc.getConvertSpec().getTblProperties());
    }
  }

  private void validatePropertiesAlreadyExist(ConversionFormats targetFormat, Map<String, String> originalParameters)
      throws SemanticException {
    boolean needsMigration = false;
    for (Map.Entry<String, String> entry : targetFormat.properties().entrySet()) {
      String originalParam = originalParameters.get(entry.getKey());
      if (originalParam == null || !originalParam.equalsIgnoreCase(entry.getValue())) {
        needsMigration = true;
        break;
      }
    }

    if (!needsMigration) {
      throw new SemanticException("Can not convert table to " + targetFormat + " ,Table is already of that format");
    }
  }
}
