package org.apache.cassandra.contrib.utils.service;

import java.net.URL;
import java.net.URLClassLoader;

public class CassandraTestClassLoader extends URLClassLoader {

  public CassandraTestClassLoader(URL[] urls) {
    super(urls, null);
  }

  @Override
  public Class<?> loadClass(String className) throws ClassNotFoundException {
    Class<?> c = null;

    if (className.contains("log4j")) {
      c = Thread.currentThread().getContextClassLoader().loadClass(className);
    } else {
        c = super.loadClass(className);
    }

    return c;
  }
}
