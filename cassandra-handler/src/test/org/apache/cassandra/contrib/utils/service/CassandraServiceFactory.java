package org.apache.cassandra.contrib.utils.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

public class CassandraServiceFactory {
  public final URL[] urls;
  public final ClassLoader urlClassLoader;

  public CassandraServiceFactory(URL[] urls)
  {
    this.urls = urls;
    urlClassLoader = new URLClassLoader(urls);
  }


  public CassandraServiceFactory(String delim, String jarList) throws IOException
  {
    byte[] buffer = new byte[(int)new File(jarList).length()];
    FileInputStream f = new FileInputStream(jarList);
    f.read(buffer);

    List<URL> urlList = new ArrayList<URL>();

    String[] files = new String(buffer).split(delim);

    for (String file : files) {
      urlList.add(new URL("file://"+file));
    }

    this.urls = urlList.toArray(new URL[]{});
    urlClassLoader = new CassandraTestClassLoader(urls);
  }


  public CassandraEmbeddedTestSetup getEmbeddedCassandraService() throws ClassNotFoundException, InstantiationException, IllegalAccessException
  {
    return (CassandraEmbeddedTestSetup) Class.forName(CassandraEmbeddedTestSetup.class.getName(), true, urlClassLoader).newInstance();
  }
}
