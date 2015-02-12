package org.apache.hadoop.hive.ql;

import static org.junit.Assert.*;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCreateUdfEntities {
  private Driver driver;
  private String funcName = "print_test";

  @Before
  public void setUp() throws Exception {

    HiveConf conf = new HiveConf(Driver.class);
    SessionState.start(conf);
    driver = new Driver(conf);
    driver.init();
  }

  @After
  public void tearDown() throws Exception {
    driver.run("drop function " + funcName);
    driver.close();
    SessionState.get().close();
  }

  @Test
  public void testUdfWithLocalResource() throws Exception {
    int rc = driver.compile("CREATE FUNCTION " + funcName + " AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf' "
            + " using file '" + "file:///tmp/udf1.jar'");
    assertEquals(0, rc);
    WriteEntity outputEntities[] = driver.getPlan().getOutputs().toArray(new WriteEntity[] {});
    assertEquals(outputEntities.length, 3);

    assertEquals(Entity.Type.DATABASE, outputEntities[0].getType());
    assertEquals("default", outputEntities[0].getDatabase().getName());

    assertEquals(Entity.Type.FUNCTION, outputEntities[1].getType());
    assertEquals(funcName, outputEntities[1].getFunctionName());

    assertEquals(Entity.Type.LOCAL_DIR, outputEntities[2].getType());
    assertEquals("file:///tmp/udf1.jar", outputEntities[2].getLocation().toString());
  }

  @Test
  public void testUdfWithDfsResource() throws Exception {
    int rc = driver.compile("CREATE FUNCTION default." + funcName + " AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf' "
            + " using file '" + "hdfs:///tmp/udf1.jar'");
    assertEquals(0, rc);
    WriteEntity outputEntities[] = driver.getPlan().getOutputs().toArray(new WriteEntity[] {});
    assertEquals(outputEntities.length, 3);

    assertEquals(Entity.Type.DATABASE, outputEntities[0].getType());
    assertEquals("default", outputEntities[0].getDatabase().getName());

    assertEquals(Entity.Type.FUNCTION, outputEntities[1].getType());
    assertEquals(funcName, outputEntities[1].getFunctionName());

    assertEquals(Entity.Type.DFS_DIR, outputEntities[2].getType());
    assertEquals("hdfs:///tmp/udf1.jar", outputEntities[2].getLocation().toString());
  }

}
