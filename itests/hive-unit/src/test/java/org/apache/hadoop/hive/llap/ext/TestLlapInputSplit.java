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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.ext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;

import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.hive.llap.Schema;
import org.apache.hadoop.hive.llap.FieldDesc;
import org.apache.hadoop.hive.llap.TypeDesc;

import org.apache.hadoop.mapred.SplitLocationInfo;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestLlapInputSplit {

  @Test
  public void testWritable() throws Exception {
    int splitNum = 88;
    byte[] planBytes = "0123456789987654321".getBytes();
    byte[] fragmentBytes = "abcdefghijklmnopqrstuvwxyz".getBytes();
    SplitLocationInfo[] locations = {
        new SplitLocationInfo("location1", false),
        new SplitLocationInfo("location2", false),
    };

    ArrayList<FieldDesc> colDescs = new ArrayList<FieldDesc>();
    colDescs.add(new FieldDesc("col1", new TypeDesc(TypeDesc.Type.STRING)));
    colDescs.add(new FieldDesc("col2", new TypeDesc(TypeDesc.Type.INT)));
    Schema schema = new Schema(colDescs);

    byte[] tokenBytes = new byte[] { 1 };
    LlapInputSplit split1 = new LlapInputSplit(splitNum, planBytes, fragmentBytes, null,
        locations, schema, "hive", tokenBytes);
    ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(byteOutStream);
    split1.write(dataOut);
    ByteArrayInputStream byteInStream = new ByteArrayInputStream(byteOutStream.toByteArray());
    DataInputStream dataIn = new DataInputStream(byteInStream);
    LlapInputSplit split2 = new LlapInputSplit();
    split2.readFields(dataIn);

    // Did we read all the data?
    assertEquals(0, byteInStream.available());

    checkLlapSplits(split1, split2);
  }

  static void checkLlapSplits(LlapInputSplit split1, LlapInputSplit split2) throws Exception {

    assertEquals(split1.getSplitNum(), split2.getSplitNum());
    assertArrayEquals(split1.getPlanBytes(), split2.getPlanBytes());
    assertArrayEquals(split1.getFragmentBytes(), split2.getFragmentBytes());
    assertArrayEquals(split1.getTokenBytes(), split2.getTokenBytes());
    SplitLocationInfo[] locationInfo1 = split1.getLocationInfo();
    SplitLocationInfo[] locationInfo2 = split2.getLocationInfo();
    for (int idx = 0; idx < locationInfo1.length; ++idx) {
      assertEquals(locationInfo1[idx].getLocation(), locationInfo2[idx].getLocation());
      assertEquals(locationInfo1[idx].isInMemory(), locationInfo2[idx].isInMemory());
      assertEquals(locationInfo1[idx].isOnDisk(), locationInfo2[idx].isOnDisk());
    }
    assertArrayEquals(split1.getLocations(), split2.getLocations());
    assertEquals(split1.getSchema().toString(), split2.getSchema().toString());
    assertEquals(split1.getLlapUser(), split2.getLlapUser());
  }

}
