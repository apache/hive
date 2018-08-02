package org.apache.hadoop.hive.registry.client;

import java.util.Map;

/**
 * This interface can be implemented to design a strategy to choose a URL for a given list of urls.
 */
public interface UrlSelector {

  public void init(Map<String, Object> conf);

  /**
   * Returns the current url chosen among the given cluster urls.
   */
  String select();

  /**
   * Sets the given {@code url} is failed with the given Exception {@code ex}.
   * @param url failed url
   * @param ex exception encountered
   */
  void urlWithError(String url, Exception ex);
}
