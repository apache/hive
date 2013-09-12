/**
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;

public class DataReaderMaster {

  public static void main(String[] args) throws FileNotFoundException, IOException {

    // This config contains all the configuration that master node wants to provide
    // to the HCatalog.
    Properties externalConfigs = new Properties();
    externalConfigs.load(new FileReader(args[0]));
    Map<String, String> config = new HashMap<String, String>();

    for (Entry<Object, Object> kv : externalConfigs.entrySet()) {
      config.put((String) kv.getKey(), (String) kv.getValue());
    }

    // This piece of code runs in master node and gets necessary context.
    ReaderContext context = runsInMaster(config);

    ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(args[1])));
    oos.writeObject(context);
    oos.flush();
    oos.close();
    // Master node will serialize readercontext and will make it available  at slaves.
  }

  private static ReaderContext runsInMaster(Map<String, String> config) throws HCatException {

    ReadEntity.Builder builder = new ReadEntity.Builder();
    ReadEntity entity = builder.withTable(config.get("table")).build();
    HCatReader reader = DataTransferFactory.getHCatReader(entity, config);
    ReaderContext cntxt = reader.prepareRead();
    return cntxt;
  }
}
