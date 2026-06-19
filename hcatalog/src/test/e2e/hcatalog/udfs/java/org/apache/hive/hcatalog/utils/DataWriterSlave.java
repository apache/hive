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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatWriter;
import org.apache.hive.hcatalog.data.transfer.WriterContext;

public class DataWriterSlave {

  public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException {

    ObjectInputStream ois = new ObjectInputStream(new FileInputStream(args[0]));
    WriterContext cntxt = (WriterContext) ois.readObject();
    ois.close();

    HCatWriter writer = DataTransferFactory.getHCatWriter(cntxt);
    writer.write(new HCatRecordItr(args[1]));

  }

  private static class HCatRecordItr implements Iterator<HCatRecord> {

    BufferedReader reader;
    String curLine;

    public HCatRecordItr(String fileName) throws FileNotFoundException {
      reader = new BufferedReader(new FileReader(new File(fileName)));
    }

    @Override
    public boolean hasNext() {
      try {
        curLine = reader.readLine();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null == curLine ? false : true;
    }

    @Override
    public HCatRecord next() {

      String[] fields = curLine.split("\t");
      List<Object> data = new ArrayList<Object>(3);
      data.add(fields[0]);
      data.add(Integer.parseInt(fields[1]));
      data.add(Double.parseDouble(fields[2]));
      return new DefaultHCatRecord(data);
    }

    @Override
    public void remove() {
      // TODO Auto-generated method stub

    }
  }
}
