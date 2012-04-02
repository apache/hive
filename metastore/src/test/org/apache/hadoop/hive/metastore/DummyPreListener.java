package org.apache.hadoop.hive.metastore;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.events.PreEventContext;

/**
 *
 * DummyPreListener.
 *
 * An implemenation of MetaStorePreEventListener which stores the Events it's seen in a list.
 */
public class DummyPreListener extends MetaStorePreEventListener {

  public static final List<PreEventContext> notifyList = new ArrayList<PreEventContext>();

  public DummyPreListener(Configuration config) {
    super(config);
  }

  @Override
  public void onEvent(PreEventContext context) throws MetaException, NoSuchObjectException,
      InvalidOperationException {
    notifyList.add(context);
  }

}
