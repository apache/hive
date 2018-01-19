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
package org.apache.hadoop.hive.conf.valcoersion;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * Enforces absolute paths to be used for the java.io.tmpdir system variable.
 * @see VariableCoercion
 * @see org.apache.hadoop.hive.conf.SystemVariables
 */
public class JavaIOTmpdirVariableCoercion extends VariableCoercion {
  private static final Log LOG = LogFactory.getLog(JavaIOTmpdirVariableCoercion.class);
  private static final String NAME = "system:java.io.tmpdir";
  private static final FileSystem LOCAL_FILE_SYSTEM = new LocalFileSystem();

  public static final JavaIOTmpdirVariableCoercion INSTANCE = new JavaIOTmpdirVariableCoercion();

  private JavaIOTmpdirVariableCoercion() {
    super(NAME);
  }

  private String coerce(String originalValue) {
    if (originalValue == null || originalValue.isEmpty()) return originalValue;

    try {
      Path originalPath = new Path(originalValue);
      Path absolutePath = FileUtils.makeAbsolute(LOCAL_FILE_SYSTEM, originalPath);
      return absolutePath.toString();
    } catch (IOException exception) {
      LOG.warn(String.format("Unable to resolve 'java.io.tmpdir' for absolute path '%s'", originalValue));
      return originalValue;
    }
  }

  @Override
  public String getCoerced(String originalValue) {
    return coerce(originalValue);
  }

}
