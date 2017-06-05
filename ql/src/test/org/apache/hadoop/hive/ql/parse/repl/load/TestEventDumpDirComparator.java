package org.apache.hadoop.hive.ql.parse.repl.load;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.parse.repl.load.EventDumpDirComparator;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestEventDumpDirComparator {

  @Test
  public void fileStatusArraySortingWithEventDumpDirComparator() {
    FileStatus[] dirList = new FileStatus[30];
    for (int i = 0; i < 30; i+=2){
      dirList[i] = new FileStatus(5, true, 1, 64, 100,
              new Path("hdfs://tmp/"+Integer.toString(i+1)));
    }
    for (int i = 1; i < 30; i+=2){
      dirList[i] = new FileStatus(5, true, 1, 64, 100,
              new Path("hdfs://tmp/"+Integer.toString(i-1)));
    }

    Arrays.sort(dirList, new EventDumpDirComparator());
    for (int i = 0; i < 30; i++){
      assertTrue(dirList[i].getPath().getName().equalsIgnoreCase(Integer.toString(i)));
    }
  }
}
