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
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.anon.io;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IndexSplit extends InputSplit implements Writable {

  private List<String> paths;
  private Path indexPath;

  public IndexSplit() {
  }

  public IndexSplit(final Path path) {
    paths = new ArrayList<>();
    indexPath = path;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return 1;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[]{""};
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    out.writeUTF(indexPath.toString());

    out.writeInt(paths.size());
    for (String path : paths) {
      out.writeUTF(path);
    }
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    indexPath = new Path(in.readUTF());

    int numPaths = in.readInt();
    paths = new ArrayList<>();
    for (int i = 0; i < numPaths; i++) {
      paths.add(in.readUTF());
    }
  }

  public Path getPath() {
    return indexPath;
  }

}
