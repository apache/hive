package org.apache.hadoop.hive.ql.processors;

import java.io.File;

import junit.framework.Assert;

import org.junit.Test;

public class TestCompileProcessor {

  @Test
  public void testSyntax() throws Exception {
    CompileProcessor cp = new CompileProcessor();
    Assert.assertEquals(0, cp.run("` public class x { \n }` AS GROOVY NAMED x.groovy").getResponseCode());
    Assert.assertEquals("GROOVY", cp.getLang());
    Assert.assertEquals(" public class x { \n }", cp.getCode());
    Assert.assertEquals("x.groovy", cp.getNamed());
    Assert.assertEquals(1, cp.run("").getResponseCode());
    Assert.assertEquals(1, cp.run("bla bla ").getResponseCode());
    CompileProcessor cp2 = new CompileProcessor();
    CommandProcessorResponse response = cp2.run(
        "` import org.apache.hadoop.hive.ql.exec.UDF \n public class x { \n }` AS GROOVY NAMED x.groovy");
    Assert.assertEquals(0, response.getResponseCode());
    File f = new File(response.getErrorMessage());
    Assert.assertTrue(f.exists());
    f.delete();
  }

}
