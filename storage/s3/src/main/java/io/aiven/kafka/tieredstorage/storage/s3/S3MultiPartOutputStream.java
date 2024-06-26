/*
 * Copyright 2021 Aiven Oy
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

package io.aiven.kafka.tieredstorage.storage.s3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.CompletedPart;

/**
 * S3 multipart output stream.
 * Enable uploads to S3 with unknown size by feeding input bytes to multiple parts and upload them on close.
 *
 * <p>Requires S3 client and starts a multipart transaction when instantiated. Do not reuse.
 *
 * <p>{@link S3MultiPartOutputStream} is not thread-safe.
 */
public class S3MultiPartOutputStream extends OutputStream {

    private static final Logger log = LoggerFactory.getLogger(S3MultiPartOutputStream.class);

    private final MultiPartUpload multiPartUpload;
    private final ByteBuffer partBuffer;
    private final List<CompletedPart> completedParts = new ArrayList<>();
    final int partSize;

    private String uploadId;
    private boolean closed;
    private long processedBytes;

    public S3MultiPartOutputStream(final MultiPartUpload multiPartUpload, final int partSize) {
        this.partSize = partSize;
        this.partBuffer = ByteBuffer.allocate(partSize);
        this.multiPartUpload = multiPartUpload;
    }

    @Override
    public void write(final int b) throws IOException {
        write(new byte[] {(byte) b}, 0, 1);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        if (isClosed()) {
            throw new IllegalStateException("Already closed");
        }
        if (b.length == 0) {
            return;
        }
        try {
            createMultiPartUploadIfNeeded();
            final ByteBuffer source = ByteBuffer.wrap(b, off, len);
            while (source.hasRemaining()) {
                final int transferred = Math.min(partBuffer.remaining(), source.remaining());
                final int offset = source.arrayOffset() + source.position();
                // TODO: get rid of this array copying
                partBuffer.put(source.array(), offset, transferred);
                processedBytes += transferred;
                source.position(source.position() + transferred);
                if (!partBuffer.hasRemaining()) {
                    flushBuffer(0, partSize);
                }
            }
        } catch (final RuntimeException e) {
            log.error("Failed to write to stream on upload {}, aborting transaction", uploadId, e);
            abortUpload();
            throw new IOException(e);
        }
    }

    private void createMultiPartUploadIfNeeded() {
        if (uploadId == null) {
            uploadId = multiPartUpload.create();
        }
    }

    @Override
    public void close() throws IOException {
        if (!isClosed()) {
            if (partBuffer.position() > 0) {
                try {
                    flushBuffer(partBuffer.arrayOffset(), partBuffer.position());
                } catch (final RuntimeException e) {
                    log.error("Failed to upload last part {}, aborting transaction", uploadId, e);
                    abortUpload();
                    throw new IOException(e);
                }
            }
            if (!completedParts.isEmpty()) {
                try {
                    completeUpload();
                    log.debug("Completed multipart upload {}", uploadId);
                } catch (final RuntimeException e) {
                    log.error("Failed to complete multipart upload {}, aborting transaction", uploadId, e);
                    abortUpload();
                    throw new IOException(e);
                }
            } else {
                abortUpload();
            }
        }
    }

    public boolean isClosed() {
        return closed;
    }

    private void completeUpload() {
        multiPartUpload.complete(uploadId, completedParts);
        closed = true;
    }

    private void abortUpload() {
        if (uploadId != null) {
            multiPartUpload.abort(uploadId);
        }
        closed = true;
    }

    private void flushBuffer(final int offset,
                             final int actualPartSize) {
        final ByteArrayInputStream in = new ByteArrayInputStream(partBuffer.array(), offset, actualPartSize);
        uploadPart(in, actualPartSize);
        partBuffer.clear();
    }

    private void uploadPart(final InputStream in, final int actualPartSize) {
        final int partNumber = completedParts.size() + 1;
        final var completedPart = multiPartUpload.uploadPart(uploadId, partNumber, in, actualPartSize);
        completedParts.add(completedPart);
    }

    long processedBytes() {
        return processedBytes;
    }

    public interface MultiPartUpload {
        String create();
        CompletedPart uploadPart(String uploadId, int partNumber, InputStream in, int partSize);
        void complete(String uploadId, List<CompletedPart> completedParts);
        void abort(String uploadId);
    }
}
