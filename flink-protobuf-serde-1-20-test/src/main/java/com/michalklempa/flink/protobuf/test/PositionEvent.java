package com.michalklempa.flink.protobuf.test;

import com.google.type.LatLng;

public class PositionEvent {

    public long timestamp;

    //    @TypeInfo(ProtoFactory.class)
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
                "timestamp=" + timestamp +
                ", position=" + latLng +
                '}';
    }
}
