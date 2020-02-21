package org.apache.hive.jdbc;

import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.Service;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TCLIService.Iface;

public class EmbeddedCLIServicePortal {

  public static Iface get(Map<String, String> hiveConfs) {
    TCLIService.Iface embeddedClient;
    try {
      Class<TCLIService.Iface> clazz =
          (Class<Iface>) Class.forName("org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService");
      embeddedClient = clazz.newInstance();
      ((Service) embeddedClient).init(buildOverlayedConf(hiveConfs));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Please Load hive-service jar to the classpath to enable embedded mode");
    } catch (Exception e) {
      throw new RuntimeException("Error initializing embedded mode", e);
    }
    return embeddedClient;
  }

  private static HiveConf buildOverlayedConf(Map<String, String> confOverlay) {
    HiveConf conf = new HiveConf();
    if (confOverlay != null && !confOverlay.isEmpty()) {
      // apply overlay query specific settings, if any
      for (Map.Entry<String, String> confEntry : confOverlay.entrySet()) {
        try {
          conf.set(confEntry.getKey(), confEntry.getValue());
        } catch (IllegalArgumentException e) {
          throw new RuntimeException("Error applying statement specific settings", e);
        }
      }
    }
    return conf;
  }

}
