package io.aiven.kafka.tieredstorage.benchs.s3;

import io.aiven.kafka.tieredstorage.storage.s3.S3MultiPartOutputStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import software.amazon.awssdk.services.s3.model.CompletedPart;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 4)
@Measurement(iterations = 16)
@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class S3MultiPartOutputStreamBench {
    static Path segmentPath;
    @Param({"10485760", "104857600", "1073741824"})
    public int contentLength; // 10MiB, 100MiB, 1GiB

    @Setup(Level.Trial)
    public void setup() throws IOException {
        segmentPath = Files.createTempFile("segment", ".log");
        // to fill with random bytes.
        final SecureRandom secureRandom = new SecureRandom();
        try (final var out = Files.newOutputStream(segmentPath)) {
            final byte[] bytes = new byte[contentLength];
            secureRandom.nextBytes(bytes);
            out.write(bytes);
        }
    }

    @TearDown
    public void teardown() throws IOException {
        Files.deleteIfExists(segmentPath);
    }

    @Benchmark
    public long test() throws IOException {
        S3MultiPartOutputStream.MultiPartUpload multiPartUpload = new NoOpMultiPartOutputStream();
        try (final var out = new S3MultiPartOutputStream(multiPartUpload, 16 * 1024 * 1024);
             final var in = Files.newInputStream(segmentPath)) {
            return in.transferTo(out);
        }
    }

    private static class NoOpMultiPartOutputStream implements S3MultiPartOutputStream.MultiPartUpload {
        @Override
        public String create() {
            return "";
        }

        @Override
        public CompletedPart uploadPart(String uploadId, int partNumber, InputStream in, int partSize) {
            try {
                in.readAllBytes();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return CompletedPart.builder()
                .partNumber(partNumber)
                .build();
        }

        @Override
        public void complete(String uploadId, List<CompletedPart> completedParts) {

        }

        @Override
        public void abort(String uploadId) {

        }
    }
}
