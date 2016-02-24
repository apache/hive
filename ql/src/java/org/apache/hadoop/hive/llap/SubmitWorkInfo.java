package org.apache.hadoop.hive.llap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.runtime.api.impl.TaskSpec;

public class SubmitWorkInfo implements Writable {

  private TaskSpec taskSpec;
  private ApplicationId fakeAppId;

  public SubmitWorkInfo(TaskSpec taskSpec, ApplicationId fakeAppId) {
    this.taskSpec = taskSpec;
    this.fakeAppId = fakeAppId;
  }

  // Empty constructor for writable etc.
  public SubmitWorkInfo() {
  }

  public TaskSpec getTaskSpec() {
    return taskSpec;
  }

  public ApplicationId getFakeAppId() {
    return fakeAppId;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    taskSpec.write(out);
    out.writeLong(fakeAppId.getClusterTimestamp());
    out.writeInt(fakeAppId.getId());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    taskSpec = new TaskSpec();
    taskSpec.readFields(in);
    long appIdTs = in.readLong();
    int appIdId = in.readInt();
    fakeAppId = ApplicationId.newInstance(appIdTs, appIdId);
  }

  public static byte[] toBytes(SubmitWorkInfo submitWorkInfo) throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    submitWorkInfo.write(dob);
    return dob.getData();
  }

  public SubmitWorkInfo fromBytes(byte[] submitWorkInfoBytes) throws IOException {
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(submitWorkInfoBytes, 0, submitWorkInfoBytes.length);
    SubmitWorkInfo submitWorkInfo = new SubmitWorkInfo();
    submitWorkInfo.readFields(dib);
    return submitWorkInfo;
  }

}
