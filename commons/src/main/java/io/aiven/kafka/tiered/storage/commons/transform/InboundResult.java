/*
 * Copyright 2023 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.tiered.storage.commons.transform;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Enumeration;
import java.util.Objects;

import io.aiven.kafka.tiered.storage.commons.chunkindex.AbstractChunkIndexBuilder;
import io.aiven.kafka.tiered.storage.commons.chunkindex.ChunkIndex;
import io.aiven.kafka.tiered.storage.commons.chunkindex.FixedSizeChunkIndexBuilder;
import io.aiven.kafka.tiered.storage.commons.chunkindex.VariableSizeChunkIndexBuilder;

/**
 * The transformation finisher.
 *
 * <p>It converts enumeration of {@code byte[]} into enumeration of {@link InputStream},
 * so that it could be used in {@link SequenceInputStream}.
 *
 * <p>It's responsible for building the chunk index.
 */
public class InboundResult implements Enumeration<InputStream> {
    private final ChunkInboundTransform inner;
    private final AbstractChunkIndexBuilder chunkIndexBuilder;
    private ChunkIndex chunkIndex = null;

    public InboundResult(final ChunkInboundTransform inner, final int originalFileSize) {
        this.inner = Objects.requireNonNull(inner, "inner cannot be null");

        if (originalFileSize < 0) {
            throw new IllegalArgumentException(
                "originalFileSize must be non-negative, " + originalFileSize + " given");
        }

        final Integer transformedChunkSize = inner.transformedChunkSize();
        if (transformedChunkSize == null) {
            this.chunkIndexBuilder = new VariableSizeChunkIndexBuilder(inner.originalChunkSize(), originalFileSize);
        } else {
            this.chunkIndexBuilder = new FixedSizeChunkIndexBuilder(
                inner.originalChunkSize(), originalFileSize, transformedChunkSize);
        }
    }

    @Override
    public boolean hasMoreElements() {
        return inner.hasMoreElements();
    }

    @Override
    public InputStream nextElement() {
        final var chunk = inner.nextElement();
        if (hasMoreElements()) {
            this.chunkIndexBuilder.addChunk(chunk.length);
        } else {
            this.chunkIndex = this.chunkIndexBuilder.finish(chunk.length);
        }

        return new ByteArrayInputStream(chunk);
    }

    public ChunkIndex chunkIndex() {
        if (chunkIndex == null) {
            throw new IllegalStateException("Chunk index was not built, was finisher used?");
        }
        return this.chunkIndex;
    }
}