package org.apache.hadoop.hive.ql.plan;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestMapWork {
  @Test
  public void testGetAndSetConsistency() {
    MapWork mw = new MapWork();
    LinkedHashMap<Path, ArrayList<String>> pathToAliases = new LinkedHashMap<>();
    pathToAliases.put(new Path("p0"), Lists.newArrayList("a1", "a2"));
    mw.setPathToAliases(pathToAliases);

    LinkedHashMap<Path, ArrayList<String>> pta = mw.getPathToAliases();
    assertEquals(pathToAliases, pta);

  }

  @Test
  public void testPath() {
    Path p1 = new Path("hdfs://asd/asd");
    Path p2 = new Path("hdfs://asd/asd/");

    assertEquals(p1, p2);
  }

}
