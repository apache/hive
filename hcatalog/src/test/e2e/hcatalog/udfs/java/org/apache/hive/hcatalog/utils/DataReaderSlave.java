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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;

public class DataReaderSlave {

  public static void main(String[] args) throws IOException, ClassNotFoundException {

    ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(args[0])));
    ReaderContext cntxt = (ReaderContext) ois.readObject();
    ois.close();

    String[] inpSlitsToRead = args[1].split(",");
    List<InputSplit> splits = cntxt.getSplits();

    for (int i = 0; i < inpSlitsToRead.length; i++) {
      InputSplit split = splits.get(Integer.parseInt(inpSlitsToRead[i]));
      HCatReader reader = DataTransferFactory.getHCatReader(split, cntxt.getConf());
      Iterator<HCatRecord> itr = reader.read();
      File f = new File(args[2] + "-" + i);
      f.delete();
      BufferedWriter outFile = new BufferedWriter(new FileWriter(f));
      while (itr.hasNext()) {
        String rec = itr.next().toString().replaceFirst("\\s+$", "");
        System.err.println(rec);
        outFile.write(rec + "\n");
      }
      outFile.close();
    }
  }
}
