package io.pulseforge.common.metrics;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.zip.DataFormatException;
import org.HdrHistogram.Histogram;

/**
 * Wire format for latency distributions: HdrHistogram's compressed encoding, base64 wrapped.
 *
 * <p>Shared between the worker that produces snapshots and the ingestor that merges them, so the
 * two can never disagree about the encoding. The parameters live here as well — a histogram decoded
 * with different bounds than it was recorded with is silently wrong.
 */
public final class HistogramCodec {

    /** One hour. A request slower than this is pathological and clamping it costs nothing. */
    public static final long MAX_TRACKABLE_MICROS = 3_600_000_000L;

    /** ~0.1 % value error, a few kilobytes per histogram. The precision/size trade-off. */
    public static final int SIGNIFICANT_DIGITS = 3;

    private HistogramCodec() {
        throw new AssertionError("utility class");
    }

    public static Histogram newHistogram() {
        return new Histogram(1, MAX_TRACKABLE_MICROS, SIGNIFICANT_DIGITS);
    }

    public static String encode(Histogram histogram) {
        ByteBuffer buffer = ByteBuffer.allocate(histogram.getNeededByteBufferCapacity());
        int written = histogram.encodeIntoCompressedByteBuffer(buffer);
        byte[] payload = new byte[written];
        buffer.rewind();
        buffer.get(payload);
        return Base64.getEncoder().encodeToString(payload);
    }

    public static Histogram decode(String base64) {
        try {
            byte[] payload = Base64.getDecoder().decode(base64);
            return Histogram.decodeFromCompressedByteBuffer(ByteBuffer.wrap(payload), 0);
        } catch (DataFormatException | IllegalArgumentException e) {
            throw new IllegalArgumentException("corrupt histogram payload", e);
        }
    }
}
