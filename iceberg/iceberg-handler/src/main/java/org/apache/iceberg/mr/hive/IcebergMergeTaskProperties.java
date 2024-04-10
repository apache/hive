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

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatFactory;
import org.apache.hadoop.hive.ql.plan.MergeTaskProperties;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.mr.Catalogs;

public class IcebergMergeTaskProperties implements MergeTaskProperties {

  private final Properties properties;
  private static final StorageFormatFactory storageFormatFactory = new StorageFormatFactory();

  IcebergMergeTaskProperties(Properties properties) {
    this.properties = properties;
  }

  public Path getTmpLocation() {
    String location = properties.getProperty(Catalogs.LOCATION);
    return new Path(location + "/data/");
  }

  public StorageFormatDescriptor getStorageFormatDescriptor() throws IOException {
    FileFormat fileFormat = FileFormat.fromString(properties.getProperty(TableProperties.DEFAULT_FILE_FORMAT,
            TableProperties.DEFAULT_FILE_FORMAT_DEFAULT));
    StorageFormatDescriptor descriptor = storageFormatFactory.get(fileFormat.name());
    if (descriptor == null) {
      throw new IOException("Unsupported storage format descriptor");
    }
    return descriptor;
  }

}
