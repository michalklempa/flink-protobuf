package com.michalklempa.flink.protobuf;

import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtoFactoryTest {

    @Test
    void createsTypeInfo() {
        assertEquals(ProtoTypeInfo.proto(Timestamp.class),
                new ProtoFactory<Timestamp>().createTypeInfo(Timestamp.class, Collections.emptyMap()));
    }

    @Test
    void rejectsOther() {
        assertThrows(IllegalArgumentException.class,
                () -> new ProtoFactory<>().createTypeInfo(String.class, Collections.emptyMap()));
    }

    @Test
    void registerRequiresNewerFlink() {
        assertThrows(UnsupportedOperationException.class, Proto::register);
    }
}
