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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatWriter;
import org.apache.hive.hcatalog.data.transfer.WriteEntity;
import org.apache.hive.hcatalog.data.transfer.WriterContext;

public class DataWriterMaster {

  public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException {

    // This config contains all the configuration that master node wants to provide
    // to the HCatalog.
    Properties externalConfigs = new Properties();
    externalConfigs.load(new FileReader(args[0]));
    Map<String, String> config = new HashMap<String, String>();

    for (Entry<Object, Object> kv : externalConfigs.entrySet()) {
      System.err.println("k: " + kv.getKey() + "\t v: " + kv.getValue());
      config.put((String) kv.getKey(), (String) kv.getValue());
    }

    if (args.length == 3 && "commit".equalsIgnoreCase(args[2])) {
      // Then, master commits if everything goes well.
      ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(args[1])));
      WriterContext cntxt = (WriterContext) ois.readObject();
      commit(config, true, cntxt);
      System.exit(0);
    }
    // This piece of code runs in master node and gets necessary context.
    WriterContext cntxt = runsInMaster(config);


    // Master node will serialize writercontext and will make it available at slaves.
    File f = new File(args[1]);
    f.delete();
    ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(f));
    oos.writeObject(cntxt);
    oos.flush();
    oos.close();
  }

  private static WriterContext runsInMaster(Map<String, String> config) throws HCatException {

    WriteEntity.Builder builder = new WriteEntity.Builder();
    WriteEntity entity = builder.withTable(config.get("table")).build();
    HCatWriter writer = DataTransferFactory.getHCatWriter(entity, config);
    WriterContext info = writer.prepareWrite();
    return info;
  }

  private static void commit(Map<String, String> config, boolean status, WriterContext cntxt) throws HCatException {

    WriteEntity.Builder builder = new WriteEntity.Builder();
    WriteEntity entity = builder.withTable(config.get("table")).build();
    HCatWriter writer = DataTransferFactory.getHCatWriter(entity, config);
    if (status) {
      writer.commit(cntxt);
    } else {
      writer.abort(cntxt);
    }
  }
}
