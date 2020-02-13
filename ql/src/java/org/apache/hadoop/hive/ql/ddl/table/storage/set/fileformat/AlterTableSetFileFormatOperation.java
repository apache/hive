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

package org.apache.hadoop.hive.ql.ddl.table.storage.set.fileformat;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableUtils;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableOperation;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Operation process of setting the file format.
 */
public class AlterTableSetFileFormatOperation extends AbstractAlterTableOperation<AlterTableSetFileFormatDesc> {
  public AlterTableSetFileFormatOperation(DDLOperationContext context, AlterTableSetFileFormatDesc desc) {
    super(context, desc);
  }

  @Override
  protected void doAlteration(Table table, Partition partition) throws HiveException {
    StorageDescriptor sd = getStorageDescriptor(table, partition);
    // if orc table, restrict changing the file format as it can break schema evolution
    if (AlterTableUtils.isSchemaEvolutionEnabled(table, context.getConf()) &&
        sd.getInputFormat().equals(OrcInputFormat.class.getName())
        && !desc.getInputFormat().equals(OrcInputFormat.class.getName())) {
      throw new HiveException(ErrorMsg.CANNOT_CHANGE_FILEFORMAT, "ORC", desc.getDbTableName());
    }

    sd.setInputFormat(desc.getInputFormat());
    sd.setOutputFormat(desc.getOutputFormat());
    if (desc.getSerdeName() != null) {
      sd.getSerdeInfo().setSerializationLib(desc.getSerdeName());
    }
  }
}
