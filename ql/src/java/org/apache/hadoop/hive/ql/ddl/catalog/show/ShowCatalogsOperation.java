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

package org.apache.hadoop.hive.ql.ddl.catalog.show;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.io.IOUtils;

import java.io.DataOutputStream;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Operation process of locking a catalog.
 */
public class ShowCatalogsOperation extends DDLOperation<ShowCatalogsDesc> {

  public ShowCatalogsOperation(DDLOperationContext context, ShowCatalogsDesc desc) {
    super(context, desc);
  }

  @Override public int execute() throws Exception {
    List<String> catalogs = context.getDb().getMSC().getCatalogs();
    if (desc.getPattern() != null) {
      LOG.debug("pattern: {}", desc.getPattern());
      Pattern pattern = Pattern.compile(UDFLike.likePatternToRegExp(desc.getPattern()), Pattern.CASE_INSENSITIVE);
      catalogs = catalogs.stream().filter(name -> pattern.matcher(name).matches()).collect(Collectors.toList());
    }

    LOG.info("Found {} catalog(s) matching the SHOW CATALOGS statement.", catalogs.size());

    // write the results in the file
    DataOutputStream outStream = ShowUtils.getOutputStream(new Path(desc.getResFile()), context);
    try {
      ShowCatalogsFormatter formatter = ShowCatalogsFormatter.getFormatter(context.getConf());
      formatter.showCatalogs(outStream, catalogs);
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, "show catalogs");
    } finally {
      IOUtils.closeStream(outStream);
    }

    return 0;
  }
}
