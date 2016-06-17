/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class OrcTail {
    // postscript + footer - Serialized in OrcSplit
    private final OrcProto.FileTail fileTail;
    // serialized representation of metadata, footer and postscript
    private final ByteBuffer serializedTail;
    // used to invalidate cache entries
    private final long fileModificationTime;
    private OrcProto.Metadata metadata;

    public OrcTail(OrcProto.FileTail fileTail, ByteBuffer serializedTail) {
        this(fileTail, serializedTail, -1);
    }

    public OrcTail(OrcProto.FileTail fileTail, ByteBuffer serializedTail, long fileModificationTime) {
        this.fileTail = fileTail;
        this.serializedTail = serializedTail;
        this.fileModificationTime = fileModificationTime;
        this.metadata = null; // lazily deserialized
    }

    public ByteBuffer getSerializedTail() {
        return serializedTail;
    }

    public long getFileModificationTime() {
        return fileModificationTime;
    }

    public OrcProto.Footer getFooter() {
        return fileTail.getFooter();
    }

    public OrcProto.PostScript getPostScript() {
        return fileTail.getPostscript();
    }

    public long getFileLength() {
        return fileTail.getFileLength();
    }

    public OrcFile.WriterVersion getWriterVersion() {
        OrcProto.PostScript ps = fileTail.getPostscript();
        return ReaderImpl.getWriterVersion(ps.getWriterVersion());
    }

    public List<StripeInformation> getStripes() {
        List<StripeInformation> result = new ArrayList<>();
        for (OrcProto.StripeInformation stripeProto : fileTail.getFooter().getStripesList()) {
            result.add(new ReaderImpl.StripeInformationImpl(stripeProto));
        }
        return result;
    }

    public CompressionKind getCompressionKind() {
        return CompressionKind.valueOf(fileTail.getPostscript().getCompression().name());
    }

    public CompressionCodec getCompressionCodec() {
        return WriterImpl.createCodec(getCompressionKind());
    }

    public int getCompressionBufferSize() {
        return (int) fileTail.getPostscript().getCompressionBlockSize();
    }

    public List<StripeStatistics> getStripeStatistics() throws IOException {
        List<StripeStatistics> result = new ArrayList<>();
        List<OrcProto.StripeStatistics> ssProto = getStripeStatisticsProto();
        if (ssProto != null) {
            for (OrcProto.StripeStatistics ss : ssProto) {
                result.add(new StripeStatistics(ss.getColStatsList()));
            }
        }
        return result;
    }

    public List<OrcProto.StripeStatistics> getStripeStatisticsProto() throws IOException {
        if (getMetadata() == null) {
            return null;
        }
        return getMetadata().getStripeStatsList();
    }

    public int getMetadataSize() {
        return (int) getPostScript().getMetadataLength();
    }

    public List<OrcProto.Type> getTypes() {
        return getFooter().getTypesList();
    }

    public OrcProto.FileTail getFileTail() {
        return fileTail;
    }

    public OrcProto.FileTail getMinimalFileTail() {
        OrcProto.FileTail.Builder fileTailBuilder = OrcProto.FileTail.newBuilder(fileTail);
        OrcProto.Footer.Builder footerBuilder = OrcProto.Footer.newBuilder(fileTail.getFooter());
        footerBuilder.clearStatistics();
        fileTailBuilder.setFooter(footerBuilder.build());
        OrcProto.FileTail result = fileTailBuilder.build();
        return result;
    }

    public OrcProto.Metadata getMetadata() throws IOException {
        if (serializedTail == null) return null;
        if (metadata == null) {
            metadata = ReaderImpl.extractMetadata(serializedTail, 0,
                    (int) fileTail.getPostscript().getMetadataLength(),
                    getCompressionCodec(), getCompressionBufferSize());
        }
        return metadata;
    }
}