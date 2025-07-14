package com.sverdiyev;

import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collection;

import static com.sverdiyev.AvroBugTest.*;
import static org.assertj.core.api.Assertions.assertThat;

// this test should pass, but currently does not.
public class ControlTest {

    @Test
    void controlTest_shouldPassAfterTheFix() {
        GenericData genericData = new GenericData();

        // current way how the array is created, according to source code:
        // https://github.com/apache/avro/blob/main/lang/java/avro/src/main/java/org/apache/avro/generic/GenericDatumReader.java#L296

        Object array = genericData.newArray(null, 1, SCHEMA);

        // adding to array, as per source code. Should not throw, but it does.
        // https://github.com/apache/avro/blob/main/lang/java/avro/src/main/java/org/apache/avro/generic/GenericDatumReader.java#L339

        Collection castedArray = (Collection) array;
        castedArray.add(INSTANT); // <--- fails here
        // java.lang.ClassCastException: class java.time.Instant cannot be cast to class java.lang.Long (java.time.Instant and java.lang.Long are in module java.base of loader 'bootstrap')

        assertThat(castedArray).containsExactly(INSTANT);
        assertThat(castedArray).hasSize(1);

        Object o = castedArray.stream().findFirst().get();

        assertThat(o).isEqualTo(INSTANT);
        assertThat(o).isInstanceOf(Instant.class);
        assertThat(o.toString()).isEqualTo(INSTANT_STRING);
    }
}
