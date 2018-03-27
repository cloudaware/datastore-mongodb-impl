package com.cloudaware.store.model;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Represents a timestamp with nanosecond precision. Timestamps cover the range [0001-01-01,
 * 9999-12-31].
 * <p>
 * <p>{@code Timestamp} instances are immutable.
 */
public final class Timestamp implements Comparable<Timestamp>, Serializable {

    /**
     * The smallest legal timestamp ("0001-01-01T00:00:00Z").
     */
    public static final Timestamp MIN_VALUE = new Timestamp(-62135596800L, 0);
    /**
     * The largest legal timestamp ("9999-12-31T23:59:59Z").
     */
    public static final Timestamp MAX_VALUE =
            new Timestamp(253402300799L, (int) TimeUnit.SECONDS.toNanos(1) - 1);
    private static final long serialVersionUID = 5152143600571559844L;
    private static final DateTimeFormatter FORMAT =
            DateTimeFormatter.ISO_LOCAL_DATE_TIME.withChronology(IsoChronology.INSTANCE);
    private static final int THOUSAND = 1000;

    private final long seconds;
    private final int nanos;

    private Timestamp(final long seconds, final int nanos) {
        this.seconds = seconds;
        this.nanos = nanos;
    }

    /**
     * Creates an instance representing the value of {@code seconds} and {@code nanos} since January
     * 1, 1970, 00:00:00 UTC.
     *
     * @param seconds seconds since January 1, 1970, 00:00:00 UTC. A negative value is the number of
     *                seconds before January 1, 1970, 00:00:00 UTC.
     * @param nanos   the fractional seconds component, in the range 0..999999999.
     * @throws IllegalArgumentException if the timestamp is outside the representable range
     */
    public static Timestamp ofTimeSecondsAndNanos(final long seconds, final int nanos) {
//        checkArgument(Timestamps.isValid(seconds, nanos), "timestamp out of range: %s, %s", seconds, nanos);
        return new Timestamp(seconds, nanos);
    }

    /**
     * Creates an instance representing the value of {@code microseconds}.
     *
     * @throws IllegalArgumentException if the timestamp is outside the representable range
     */
    public static Timestamp ofTimeMicroseconds(final long microseconds) {
        final long seconds = TimeUnit.MICROSECONDS.toSeconds(microseconds);
        final int nanos = (int) TimeUnit.MICROSECONDS.toNanos(microseconds - TimeUnit.SECONDS.toMicros(seconds));
//        checkArgument(                Timestamps.isValid(seconds, nanos), "timestamp out of range: %s, %s", seconds, nanos);
        return new Timestamp(seconds, nanos);
    }

    /**
     * Creates an instance representing the value of {@code Date}.
     *
     * @throws IllegalArgumentException if the timestamp is outside the representable range
     */
    public static Timestamp of(final Date date) {
        return ofTimeMicroseconds(TimeUnit.MILLISECONDS.toMicros(date.getTime()));
    }

    /**
     * Creates an instance representing the value of {@code timestamp}.
     *
     * @throws IllegalArgumentException if the timestamp is outside the representable range
     */
    public static Timestamp of(final java.sql.Timestamp timestamp) {
        return ofTimeSecondsAndNanos(timestamp.getTime() / THOUSAND, timestamp.getNanos());
    }

    /**
     * Creates an instance with current time.
     */
    public static Timestamp now() {
        final java.sql.Timestamp date = new java.sql.Timestamp(System.currentTimeMillis());
        return of(date);
    }

    /**
     * Creates a Timestamp instance from the given string. String is in the RFC 3339 format without
     * the timezone offset (always ends in "Z").
     */
    public static Timestamp parseTimestamp(final String timestamp) {
        final Instant instant = Instant.parse(timestamp);
        return ofTimeSecondsAndNanos(instant.getEpochSecond(), instant.getNano());
    }

    /**
     * Returns the number of seconds since January 1, 1970, 00:00:00 UTC. A negative value is the
     * number of seconds before January 1, 1970, 00:00:00 UTC.
     */
    public long getSeconds() {
        return seconds;
    }

    /**
     * Returns the fractional seconds component, in nanoseconds.
     */
    public int getNanos() {
        return nanos;
    }

    /**
     * Returns a JDBC timestamp initialized to the same point in time as {@code this}.
     */
    public java.sql.Timestamp toSqlTimestamp() {
        final java.sql.Timestamp ts = new java.sql.Timestamp(seconds * 1000);
        ts.setNanos(nanos);
        return ts;
    }

//    /** Creates an instance of Timestamp from {@code com.google.protobuf.Timestamp}. */
//    public static Timestamp fromProto(com.google.protobuf.Timestamp proto) {
//        return new Timestamp(proto.getSeconds(), proto.getNanos());
//    }
//
//    /**
//     * Returns a {@code com.google.protobuf.Timestamp} initialized to the same point in time as {@code
//     * this}.
//     */
//    public com.google.protobuf.Timestamp toProto() {
//        return com.google.protobuf.Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
//    }

    @Override
    public int compareTo(final Timestamp other) {
        int r = Long.compare(seconds, other.seconds);
        if (r == 0) {
            r = Integer.compare(nanos, other.nanos);
        }
        return r;
    }

    private StringBuilder toString(final StringBuilder b) {
        FORMAT.formatTo(LocalDateTime.ofEpochSecond(seconds, 0, ZoneOffset.UTC), b);
        if (nanos != 0) {
            b.append(String.format(".%09d", nanos));
        }
        b.append('Z');
        return b;
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Timestamp that = (Timestamp) o;
        return seconds == that.seconds && nanos == that.nanos;
    }

    @Override
    public int hashCode() {
        return Objects.hash(seconds, nanos);
    }

    // TODO(user): Consider adding math operations.
}
