/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.hadoop.hive.ql.ddl.policy.show;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ErasurePolicy;
import org.apache.hadoop.hive.metastore.api.ErasurePolicyLifecycleEvent;
import org.apache.hadoop.hive.metastore.api.ErasurePolicyVersion;
import org.apache.hadoop.hive.metastore.api.PolicyLifecycleEventType;
import org.apache.hadoop.hive.metastore.api.PolicyVersionStatus;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.IOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Lists the lifecycle journal of a named erasure policy as one row per
 * recorded event, ordered by event timestamp. Joins each event with its
 * version row so the operator sees the version label and version status
 * alongside the bare event type.
 */
public class ShowErasurePolicyHistoryOperation
    extends DDLOperation<ShowErasurePolicyHistoryDesc> {

  public ShowErasurePolicyHistoryOperation(DDLOperationContext context,
      ShowErasurePolicyHistoryDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    final Hive db = context.getDb();
    final String policyName = desc.getPolicyName();

    final ErasurePolicy header = db.getErasurePolicy(policyName);
    if (header == null) {
      throw new HiveException(ErrorMsg.GENERIC_ERROR,
          "SHOW ERASURE POLICY HISTORY: policy not found: " + policyName);
    }

    // Build versionId -> version row map so each event row can carry the
    // human-readable version label and current per-row status.
    final List<ErasurePolicyVersion> versions = db.listErasurePolicyVersions(policyName);
    final Map<Long, ErasurePolicyVersion> versionById = new HashMap<>();
    if (versions != null) {
      for (final ErasurePolicyVersion v : versions) {
        versionById.put(v.getVersionId(), v);
      }
    }

    // Pull the full event journal for this policy. The metastore returns
    // events in insertion order; sort by event_ts so output is stable
    // regardless of storage-side iteration order.
    final List<ErasurePolicyLifecycleEvent> events =
        db.getLifecycleEventsForPolicy(policyName, 0L, Long.MAX_VALUE);
    if (events != null) {
      events.sort(Comparator.comparingLong(ErasurePolicyLifecycleEvent::getEventTs)
          .thenComparingLong(ErasurePolicyLifecycleEvent::getEventId));
    }

    final DataOutputStream out = ShowUtils.getOutputStream(new Path(desc.getResFile()), context);
    try {
      if (events != null) {
        for (final ErasurePolicyLifecycleEvent e : events) {
          final ErasurePolicyVersion v = versionById.get(e.getVersionId());
          writeRow(out, e, v);
        }
      }
    } catch (IOException ioe) {
      throw new HiveException(ioe, ErrorMsg.GENERIC_ERROR, "SHOW ERASURE POLICY HISTORY");
    } finally {
      IOUtils.closeStream(out);
    }
    return 0;
  }

  private static void writeRow(final DataOutputStream out,
      final ErasurePolicyLifecycleEvent e,
      final ErasurePolicyVersion v) throws IOException {
    out.writeBytes(Long.toString(e.getVersionId()));
    out.write(Utilities.tabCode);
    out.writeBytes(safe(v == null ? null : v.getVersionLabel()));
    out.write(Utilities.tabCode);
    out.writeBytes(safe(v == null || v.getStatus() == null
        ? null : statusName(v.getStatus())));
    out.write(Utilities.tabCode);
    out.writeBytes(safe(e.getEventType() == null ? null : eventName(e.getEventType())));
    out.write(Utilities.tabCode);
    out.writeBytes(Long.toString(e.getEventTs()));
    out.write(Utilities.tabCode);
    out.writeBytes(safe(e.getPrincipal()));
    out.write(Utilities.tabCode);
    out.writeBytes(safe(e.isSetNote() ? e.getNote() : null));
    out.write(Utilities.newLineCode);
  }

  private static String safe(final String s) {
    return s == null ? "" : s;
  }

  private static String eventName(final PolicyLifecycleEventType t) {
    return t.name();
  }

  private static String statusName(final PolicyVersionStatus s) {
    return s.name();
  }
}
