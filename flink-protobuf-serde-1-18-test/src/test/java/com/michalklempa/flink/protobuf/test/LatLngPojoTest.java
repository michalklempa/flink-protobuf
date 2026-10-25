package com.michalklempa.flink.protobuf.test;

import com.google.type.LatLng;
import org.apache.flink.types.PojoTestUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertThrows;

class LatLngPojoTest {

    private static final Logger LOG = LoggerFactory.getLogger(LatLngPojoTest.class);

    @Test
    void latLngIsNotSerializedAsPojo() {
        AssertionError e = assertThrows(AssertionError.class, () -> PojoTestUtils.assertSerializedAsPojo(LatLng.class));
        LOG.warn("assertSerializedAsPojo failed as expected", e);
    }

    @Test
    void latLngIsNotSerializedAsPojoWithoutKryo() {
        AssertionError e = assertThrows(AssertionError.class, () -> PojoTestUtils.assertSerializedAsPojoWithoutKryo(LatLng.class));
        LOG.warn("assertSerializedAsPojoWithoutKryo failed as expected", e);
    }
}
