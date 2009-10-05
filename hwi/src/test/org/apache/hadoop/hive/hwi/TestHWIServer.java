package org.apache.hadoop.hive.hwi;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

import junit.framework.TestCase;
import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.hadoop.hive.shims.JettyShims;
import org.apache.hadoop.hive.shims.ShimLoader;

public class TestHWIServer extends TestCase{

  public TestHWIServer(String name) {
    super(name);

  }

  protected void setUp() throws Exception {
    super.setUp();

  }

  protected void tearDown() throws Exception {
    super.tearDown();

  }

  public final void testServerInit() throws Exception {
    
    JettyShims.Server webServer;
    webServer = ShimLoader.getJettyShims().startServer("0.0.0.0", 9999);
    assertNotNull(webServer);
    webServer.addWar("../build/hwi/hive_hwi.war", "/hwi");
    webServer.start();
 //   webServer.join();
    webServer.stop();
    assert(true);
  }

  
}
