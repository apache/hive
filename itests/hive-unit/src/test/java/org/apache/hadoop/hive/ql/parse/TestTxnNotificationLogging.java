package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hive.common.repl.ReplScope;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetastoreTaskThread;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.messaging.AbortTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.OpenTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.event.filters.AndFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.CatalogFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.EventBoundaryFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.ReplEventFilter;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.thrift.TException;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TestTxnNotificationLogging {

  HiveConf hiveConf = new HiveConf();
  Hive hiveDb;
  ObjectStore objectStore;
  MetastoreTaskThread acidTxnCleanerService;
  MetastoreTaskThread acidHouseKeeperService;
  IMetaStoreClient hive;

  int getNumNotifications(List<Long> txnIds, String eventType) throws TException {
    int numNotifications = 0;
    IMetaStoreClient.NotificationFilter evFilter = new AndFilter(new ReplEventFilter(new ReplScope()),
        new CatalogFilter(MetaStoreUtils.getDefaultCatalog(hiveConf)), new EventBoundaryFilter(0, 100));
    NotificationEventResponse rsp = hive.getNextNotification(new NotificationEventRequest(), true, evFilter);
    if (rsp.getEvents() == null) {
      return numNotifications;
    }
    Iterator<NotificationEvent> eventIterator = rsp.getEvents().iterator();
    MessageDeserializer deserializer;
    while (eventIterator.hasNext()) {
      NotificationEvent ev = eventIterator.next();
      if (eventType.equals(ev.getEventType())) {
        deserializer = ReplUtils.getEventDeserializer(ev);
        switch (ev.getEventType()) {
        case MessageBuilder.OPEN_TXN_EVENT:
          OpenTxnMessage openTxnMessage = deserializer.getOpenTxnMessage(ev.getMessage());
          if (txnIds.contains(openTxnMessage.getTxnIds().get(0))) {
            numNotifications++;
          }
          break;
        case MessageBuilder.ABORT_TXN_EVENT:
          AbortTxnMessage abortTxnMessage = deserializer.getAbortTxnMessage(ev.getMessage());
          if (txnIds.contains(abortTxnMessage.getTxnId())) {
            numNotifications++;
          }
        }
      }
    }
    return numNotifications;
  }

  List<Long> openTxns(int txnCounter, TxnType txnType) throws TException {
    List<Long> txnIds = new LinkedList<>();
    for (; txnCounter > 0; txnCounter--) {
      if (txnType == TxnType.REPL_CREATED) {
        Long srcTxn = (long) (11 + txnCounter);
        List<Long> srcTxns = Collections.singletonList(srcTxn);
        txnIds.addAll(hive.replOpenTxn("testPolicy", srcTxns, "hive", txnType));
      } else {
        txnIds.add(hive.openTxn("hive", txnType));
      }
    }
    return txnIds;
  }

  int getNumberOfTxnsWithTxnState(List<Long> txnIds, TxnState txnState) throws TException {
    AtomicInteger numTxns = new AtomicInteger();
    hive.showTxns().getOpen_txns().forEach(txnInfo -> {
      if (txnInfo.getState() == txnState && txnIds.contains(txnInfo.getId())) {
        numTxns.incrementAndGet();
      }
    });
    return numTxns.get();
  }

  void runCleanerServices() {
    objectStore.cleanNotificationEvents(0);
    acidTxnCleanerService.run(); //this will remove empty aborted txns
  }
}
