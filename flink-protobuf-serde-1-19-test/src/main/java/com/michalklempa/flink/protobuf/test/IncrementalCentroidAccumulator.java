package com.michalklempa.flink.protobuf.test;

import com.google.type.LatLng;
import com.michalklempa.flink.protobuf.ProtoFactory;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.flink.api.common.typeinfo.TypeInfo;

public class IncrementalCentroidAccumulator {

    public long n;

    @TypeInfo(ProtoFactory.class)
    public LatLng sum;

    public long timestamp;

    public IncrementalCentroidAccumulator() {
    }

    public IncrementalCentroidAccumulator(long n, LatLng sum, long timestamp) {
        this.n = n;
        this.sum = sum;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("n", n)
                .append("center", sum)
                .append("timestamp", timestamp)
                .toString();
    }
}
