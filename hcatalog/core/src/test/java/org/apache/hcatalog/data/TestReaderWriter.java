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

package org.apache.hcatalog.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hcatalog.data.transfer.HCatReader;
import org.apache.hcatalog.data.transfer.HCatWriter;
import org.apache.hcatalog.data.transfer.ReadEntity;
import org.apache.hcatalog.data.transfer.ReaderContext;
import org.apache.hcatalog.data.transfer.WriteEntity;
import org.apache.hcatalog.data.transfer.WriterContext;
import org.apache.hcatalog.mapreduce.HCatBaseTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.data.TestReaderWriter} instead
 */
public class TestReaderWriter extends HCatBaseTest {

  @Test
  public void test() throws MetaException, CommandNeedRetryException,
      IOException, ClassNotFoundException {

    driver.run("drop table mytbl");
    driver.run("create table mytbl (a string, b int)");
    Iterator<Entry<String, String>> itr = hiveConf.iterator();
    Map<String, String> map = new HashMap<String, String>();
    while (itr.hasNext()) {
      Entry<String, String> kv = itr.next();
      map.put(kv.getKey(), kv.getValue());
    }

    WriterContext cntxt = runsInMaster(map);

    File writeCntxtFile = File.createTempFile("hcat-write", "temp");
    writeCntxtFile.deleteOnExit();

    // Serialize context.
    ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(writeCntxtFile));
    oos.writeObject(cntxt);
    oos.flush();
    oos.close();

    // Now, deserialize it.
    ObjectInputStream ois = new ObjectInputStream(new FileInputStream(writeCntxtFile));
    cntxt = (WriterContext) ois.readObject();
    ois.close();

    runsInSlave(cntxt);
    commit(map, true, cntxt);

    ReaderContext readCntxt = runsInMaster(map, false);

    File readCntxtFile = File.createTempFile("hcat-read", "temp");
    readCntxtFile.deleteOnExit();
    oos = new ObjectOutputStream(new FileOutputStream(readCntxtFile));
    oos.writeObject(readCntxt);
    oos.flush();
    oos.close();

    ois = new ObjectInputStream(new FileInputStream(readCntxtFile));
    readCntxt = (ReaderContext) ois.readObject();
    ois.close();

    for (InputSplit split : readCntxt.getSplits()) {
      runsInSlave(split, readCntxt.getConf());
    }
  }

  private WriterContext runsInMaster(Map<String, String> config) throws HCatException {

    WriteEntity.Builder builder = new WriteEntity.Builder();
    WriteEntity entity = builder.withTable("mytbl").build();
    HCatWriter writer = DataTransferFactory.getHCatWriter(entity, config);
    WriterContext info = writer.prepareWrite();
    return info;
  }

  private ReaderContext runsInMaster(Map<String, String> config, boolean bogus)
    throws HCatException {
    ReadEntity entity = new ReadEntity.Builder().withTable("mytbl").build();
    HCatReader reader = DataTransferFactory.getHCatReader(entity, config);
    ReaderContext cntxt = reader.prepareRead();
    return cntxt;
  }

  private void runsInSlave(InputSplit split, Configuration config) throws HCatException {

    HCatReader reader = DataTransferFactory.getHCatReader(split, config);
    Iterator<HCatRecord> itr = reader.read();
    int i = 1;
    while (itr.hasNext()) {
      HCatRecord read = itr.next();
      HCatRecord written = getRecord(i++);
      // Argh, HCatRecord doesnt implement equals()
      Assert.assertTrue("Read: " + read.get(0) + "Written: " + written.get(0),
        written.get(0).equals(read.get(0)));
      Assert.assertTrue("Read: " + read.get(1) + "Written: " + written.get(1),
        written.get(1).equals(read.get(1)));
      Assert.assertEquals(2, read.size());
    }
    //Assert.assertFalse(itr.hasNext());
  }

  private void runsInSlave(WriterContext context) throws HCatException {

    HCatWriter writer = DataTransferFactory.getHCatWriter(context);
    writer.write(new HCatRecordItr());
  }

  private void commit(Map<String, String> config, boolean status,
      WriterContext context) throws IOException {

    WriteEntity.Builder builder = new WriteEntity.Builder();
    WriteEntity entity = builder.withTable("mytbl").build();
    HCatWriter writer = DataTransferFactory.getHCatWriter(entity, config);
    if (status) {
      writer.commit(context);
    } else {
      writer.abort(context);
    }
  }

  private static HCatRecord getRecord(int i) {
    List<Object> list = new ArrayList<Object>(2);
    list.add("Row #: " + i);
    list.add(i);
    return new DefaultHCatRecord(list);
  }

  private static class HCatRecordItr implements Iterator<HCatRecord> {

    int i = 0;

    @Override
    public boolean hasNext() {
      return i++ < 100 ? true : false;
    }

    @Override
    public HCatRecord next() {
      return getRecord(i);
    }

    @Override
    public void remove() {
      throw new RuntimeException();
    }
  }
}
