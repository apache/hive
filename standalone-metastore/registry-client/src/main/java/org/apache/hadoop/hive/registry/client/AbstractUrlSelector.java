package org.apache.hadoop.hive.registry.client;

import java.util.Collections;
import java.util.Map;

public abstract class AbstractUrlSelector implements UrlSelector {

  protected final String[] urls;
  protected Map<String, Object> conf;

  public AbstractUrlSelector(String clusterUrl) {
    urls = clusterUrl.split(",");
  }

  @Override
  public void init(Map<String, Object> conf) {
    this.conf = Collections.unmodifiableMap(conf);
  }
}
