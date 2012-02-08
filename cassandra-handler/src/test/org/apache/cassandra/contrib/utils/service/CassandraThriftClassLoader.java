package org.apache.cassandra.contrib.utils.service;

import java.net.URL;
import java.net.URLClassLoader;

public class CassandraThriftClassLoader extends URLClassLoader {


  public CassandraThriftClassLoader(URL[] urls) {
    super(urls, null);
  }

  @Override
  public Class<?> loadClass(String className) throws ClassNotFoundException {
    Class<?> c = null;


    System.err.println("LOADING CLASS: "+className);

    if (className.contains("thrift") || className.contains("cassandra")) {
      c = super.loadClass(className);
    } else {
      c = Thread.currentThread().getContextClassLoader().loadClass(className);
    }

    return c;
  }
}
