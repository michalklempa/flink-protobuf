package com.michalklempa.flink.protobuf.test;

import com.google.type.LatLng;
import com.michalklempa.flink.protobuf.ProtoFactory;
import org.apache.flink.api.common.typeinfo.TypeInfo;

import java.time.Instant;

public class PositionEvent {

    public long timestamp;

    @TypeInfo(ProtoFactory.class)
    public LatLng latLng;

    public PositionEvent() {
    }

    public PositionEvent(long timestamp, LatLng latLng) {
        this.timestamp = timestamp;
        this.latLng = latLng;
    }

    @Override
    public String toString() {
        return "PositionEvent{" +
                "timestamp=" + Instant.ofEpochMilli(timestamp) +
                ", position=" + latLng +
                '}';
    }
}
