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
package org.apache.hadoop.hive.registry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Preconditions;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SerDesInfo implements Serializable {
    private static final long serialVersionUID = -3756866955883733874L;

    private Long id;
    private Long timestamp;
    private SerDesPair serDesPair;

    protected SerDesInfo() {
    }

    public SerDesInfo(Long id, Long timestamp, SerDesPair serDesPair) {
        this.id = id;
        this.timestamp = timestamp;
        this.serDesPair = serDesPair;
        Preconditions.checkNotNull(id, "id can not be null");
        Preconditions.checkNotNull(timestamp, "timestamp can not be null");
        Preconditions.checkNotNull(serDesPair, "serDesPair can not be null");
    }

    public Long getId() {
        return id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public SerDesPair getSerDesPair() {
        return serDesPair;
    }

    @Override
    public String toString() {
        return "SerDesInfo{" +
                "id=" + id +
                ", timestamp=" + timestamp +
                ", serDesPair=" + serDesPair +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SerDesInfo that = (SerDesInfo) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) return false;
        return serDesPair != null ? serDesPair.equals(that.serDesPair) : that.serDesPair == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + (serDesPair != null ? serDesPair.hashCode() : 0);
        return result;
    }
}
