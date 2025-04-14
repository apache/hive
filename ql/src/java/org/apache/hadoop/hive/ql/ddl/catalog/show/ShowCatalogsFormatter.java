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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.formatting.MapBuilder;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Formats SHOW CATALOGS results.
 */
abstract class ShowCatalogsFormatter {
  public static ShowCatalogsFormatter getFormatter(HiveConf conf) {
    if (MetaDataFormatUtils.isJson(conf)) {
      return new JsonShowCatalogsFormatter();
    } else {
      return new TextShowCatalogsFormatter();
    }
  }

  abstract void showCatalogs(DataOutputStream out, List<String> catalogs) throws HiveException;


  // ------ Implementations ------

  static class JsonShowCatalogsFormatter extends ShowCatalogsFormatter {
    @Override
    void showCatalogs(DataOutputStream out, List<String> catalogs) throws HiveException {
      ShowUtils.asJson(out, MapBuilder.create().put("catalogs", catalogs).build());
    }
  }

  static class TextShowCatalogsFormatter extends ShowCatalogsFormatter {
    @Override
    void showCatalogs(DataOutputStream out, List<String> catalogs) throws HiveException {
      try {
        for (String catalog : catalogs) {
          out.write(catalog.getBytes(StandardCharsets.UTF_8));
          out.write(Utilities.newLineCode);
        }
      } catch (IOException e) {
        throw new HiveException(e);
      }
    }
  }

}
