package org.apache.hadoop.hive.llap.ext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.hive.llap.Schema;
import org.apache.hadoop.hive.llap.FieldDesc;
import org.apache.hadoop.hive.llap.TypeDesc;

import org.apache.hadoop.mapred.SplitLocationInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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

    LlapInputSplit split1 = new LlapInputSplit(
        splitNum,
        planBytes,
        fragmentBytes,
        locations,
        schema,
        "hive");
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

  static void checkLlapSplits(
      LlapInputSplit split1,
      LlapInputSplit split2) throws Exception {

    assertEquals(split1.getSplitNum(), split2.getSplitNum());
    assertArrayEquals(split1.getPlanBytes(), split2.getPlanBytes());
    assertArrayEquals(split1.getFragmentBytes(), split2.getFragmentBytes());
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
