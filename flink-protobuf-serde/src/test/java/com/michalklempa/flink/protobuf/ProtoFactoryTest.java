package com.michalklempa.flink.protobuf;

import com.google.protobuf.Timestamp;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

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
    void registers() {
        Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.SERIALIZATION_CONFIG, List.of(
                "com.google.protobuf.AbstractMessage: {type: typeinfo, class: com.michalklempa.flink.protobuf.ProtoFactory}"));
        new SerializerConfigImpl().configure(configuration, ProtoFactoryTest.class.getClassLoader());
        assertEquals(ProtoTypeInfo.proto(Timestamp.class), TypeInformation.of(Timestamp.class));
    }
}
