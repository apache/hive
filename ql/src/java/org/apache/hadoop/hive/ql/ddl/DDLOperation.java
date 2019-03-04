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

package org.apache.hadoop.hive.ql.ddl;

import java.io.DataOutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract ancestor class of all DDL Operation classes.
 */
public abstract class DDLOperation {
  protected static final Logger LOG = LoggerFactory.getLogger("hive.ql.exec.DDLTask");

  protected final DDLOperationContext context;

  public DDLOperation(DDLOperationContext context) {
    this.context = context;
  }

  public abstract int execute() throws HiveException;

  protected DataOutputStream getOutputStream(Path outputFile) throws HiveException {
    try {
      FileSystem fs = outputFile.getFileSystem(context.getConf());
      return fs.create(outputFile);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }
}
