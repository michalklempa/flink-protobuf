package com.michalklempa.flink.protobuf;

import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtoSerializerTest {

    private <T extends com.google.protobuf.Message> T roundTrip(ProtoSerializer<T> serializer, T value) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(value, out);
        return serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
    }

    @Test
    void roundTripsTimestamp() throws Exception {
        ProtoSerializer<Timestamp> serializer = new ProtoSerializer<>(Timestamp.class);
        Timestamp value = Timestamp.newBuilder().setSeconds(1751000000).setNanos(42).build();
        assertEquals(value, roundTrip(serializer, value));
    }

    @Test
    void roundTripsStringValue() throws Exception {
        ProtoSerializer<StringValue> serializer = new ProtoSerializer<>(StringValue.class);
        StringValue value = StringValue.of("sail-event");
        assertEquals(value, roundTrip(serializer, value));
    }

    @Test
    void handlesNull() throws Exception {
        ProtoSerializer<Timestamp> serializer = new ProtoSerializer<>(Timestamp.class);
        assertNull(roundTrip(serializer, null));
    }

    @Test
    void copyRelaysBytes() throws Exception {
        ProtoSerializer<Timestamp> serializer = new ProtoSerializer<>(Timestamp.class);
        Timestamp value = Timestamp.newBuilder().setSeconds(7).build();
        DataOutputSerializer source = new DataOutputSerializer(64);
        serializer.serialize(value, source);

        DataOutputSerializer target = new DataOutputSerializer(64);
        serializer.copy(new DataInputDeserializer(source.getCopyOfBuffer()), target);

        assertEquals(value, serializer.deserialize(new DataInputDeserializer(target.getCopyOfBuffer())));
    }

    @Test
    void snapshotsConfiguration() {
        ProtoSerializer<Timestamp> serializer = new ProtoSerializer<>(Timestamp.class);
        assertTrue(serializer.snapshotConfiguration() instanceof ProtoSerializerSnapshot);
    }

    @Test
    void stateDescriptorSnapshotsConfiguration() {
        ValueStateDescriptor<Timestamp> descriptor =
                new ValueStateDescriptor<>("state", ProtoTypeInfo.proto(Timestamp.class));
        descriptor.initializeSerializerUnlessSet(new ExecutionConfig());
        assertTrue(descriptor.getSerializer().snapshotConfiguration() instanceof ProtoSerializerSnapshot);
    }

    @Test
    void typeInfoCreatesSerializer() {
        assertEquals(new ProtoSerializer<>(Timestamp.class),
                ProtoTypeInfo.proto(Timestamp.class).createSerializer(new ExecutionConfig()));
    }
}
