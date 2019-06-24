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

package org.apache.hadoop.hive.ql.ddl.table.info;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;

import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.io.IOUtils;

/**
 * Operation process showing the table status.
 */
public class ShowTableStatusOperation extends DDLOperation<ShowTableStatusDesc> {
  public ShowTableStatusOperation(DDLOperationContext context, ShowTableStatusDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    // get the tables for the desired pattern - populate the output stream
    List<Table> tbls = new ArrayList<Table>();
    Map<String, String> part = desc.getPartSpec();
    Partition par = null;
    if (part != null) {
      Table tbl = context.getDb().getTable(desc.getDbName(), desc.getPattern());
      par = context.getDb().getPartition(tbl, part, false);
      if (par == null) {
        throw new HiveException("Partition " + part + " for table " + desc.getPattern() + " does not exist.");
      }
      tbls.add(tbl);
    } else {
      LOG.debug("pattern: {}", desc.getPattern());
      List<String> tblStr = context.getDb().getTablesForDb(desc.getDbName(), desc.getPattern());
      SortedSet<String> sortedTbls = new TreeSet<String>(tblStr);
      Iterator<String> iterTbls = sortedTbls.iterator();
      while (iterTbls.hasNext()) {
        // create a row per table name
        String tblName = iterTbls.next();
        Table tbl = context.getDb().getTable(desc.getDbName(), tblName);
        tbls.add(tbl);
      }
      LOG.info("Found {} table(s) matching the SHOW TABLE EXTENDED statement.", tblStr.size());
    }

    // write the results in the file
    DataOutputStream outStream = DDLUtils.getOutputStream(new Path(desc.getResFile()), context);
    try {
      context.getFormatter().showTableStatus(outStream, context.getDb(), context.getConf(), tbls, part, par);
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, "show table status");
    } finally {
      IOUtils.closeStream(outStream);
    }
    return 0;
  }
}
