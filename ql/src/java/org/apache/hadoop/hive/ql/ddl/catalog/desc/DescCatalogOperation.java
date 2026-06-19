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

package org.apache.hadoop.hive.ql.ddl.catalog.desc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.io.DataOutputStream;

/**
 * Operation process of describing a catalog.
 */
public class DescCatalogOperation extends DDLOperation<DescCatalogDesc> {
  public DescCatalogOperation(DDLOperationContext context, DescCatalogDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws Exception {
    try (DataOutputStream outStream = ShowUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      Catalog catalog = context.getDb().getMSC().getCatalog(desc.getCatName());
      if (catalog == null) {
        throw new HiveException(ErrorMsg.CATALOG_NOT_EXISTS, desc.getCatName());
      }
      int createTime = 0;
      if (desc.isExtended()) {
        createTime = catalog.getCreateTime();
      }
      DescCatalogFormatter formatter = DescCatalogFormatter.getFormatter(context.getConf());
      formatter.showCatalogDescription(outStream, catalog.getName(), catalog.getDescription(),
          catalog.getLocationUri(), createTime);
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR);
    }
    return 0;
  }
}
