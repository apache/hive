package org.apache.iceberg.mr.hive;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

public class TestHiveIcebergStorageHandler {

  @Test
  public void testAuthzURI() throws URISyntaxException {
    Map<String, String> props = ImmutableMap.of(
        Catalogs.LOCATION, "hdfs://abcd/汉字123/"
    );

    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    URI uriForAuth = storageHandler.getURIForAuth(props);

    Assert.assertEquals("iceberg://hdfs://abcd/汉字123/", uriForAuth.toString());
  }

}
