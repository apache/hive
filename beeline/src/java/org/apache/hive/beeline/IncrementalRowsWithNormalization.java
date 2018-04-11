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

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * See SQLLine notice in LICENSE
 */
package org.apache.hive.beeline;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;

import com.google.common.base.Optional;


/**
 * Extension of {@link IncrementalRows} which buffers "x" number of rows in memory at a time. It
 * uses the {@link BufferedRows} class to do its buffering. The value of "x" is determined  by the
 * Beeline option <code>--incrementalBufferRows</code>, which defaults to
 * {@link BeeLineOpts#DEFAULT_INCREMENTAL_BUFFER_ROWS}. Once the initial set of rows are buffered, it
 * will allow the {@link #next()} method to drain the buffer. Once the buffer is empty the next
 * buffer will be fetched until the {@link ResultSet} is empty. The width of the rows are normalized
 * within each buffer using the {@link BufferedRows#normalizeWidths()} method.
 */
public class IncrementalRowsWithNormalization extends IncrementalRows {

  private final int incrementalBufferRows;
  private BufferedRows buffer;

  IncrementalRowsWithNormalization(BeeLine beeLine, ResultSet rs) throws SQLException {
    super(beeLine, rs);

    this.incrementalBufferRows = beeLine.getOpts().getIncrementalBufferRows();
    this.buffer = new BufferedRows(beeLine, rs, Optional.of(this.incrementalBufferRows));
    this.buffer.normalizeWidths();
  }

  @Override
  public boolean hasNext() {
    try {
      if (this.buffer.hasNext()) {
        return true;
      } else {
        this.buffer = new BufferedRows(this.beeLine, this.rs,
                Optional.of(this.incrementalBufferRows));
        if (this.normalizingWidths) {
          this.buffer.normalizeWidths();
        }

        // Drain the first Row, which just contains column names
        if (!this.buffer.hasNext()) {
          return false;
        }
        this.buffer.next();

        return this.buffer.hasNext();
      }
    } catch (SQLException ex) {
      throw new RuntimeException(ex.toString());
    }
  }

  @Override
  public Object next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return this.buffer.next();
  }
}
