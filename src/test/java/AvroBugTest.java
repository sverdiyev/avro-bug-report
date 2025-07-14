import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.PrimitivesArrays;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AvroBugTest {

    public static final String SCHEMA_STR = """
            {
              "type": "array",
              "items": {
                "type": "long",
                "logicalType": "timestamp-millis"
              }
            }
            """;
    public static final Schema SCHEMA = Schema.parse(SCHEMA_STR);

    public static final String INSTANT_STRING = "2023-10-01T00:00:00Z";
    public static final Instant INSTANT = Instant.parse(INSTANT_STRING);


    @Test
    void arrayCreationPerCurrentSourceCode(){

        GenericData genericData = new GenericData();

        // current way how the array is created, according to source code:
        // https://github.com/apache/avro/blob/main/lang/java/avro/src/main/java/org/apache/avro/generic/GenericDatumReader.java#L296

        Object array = genericData.newArray(null, 1, SCHEMA);

        // it generates a PrimitivesArrays.LongArray, even though the schema is for a array with timestamps as logical types
        assertThat(array).isInstanceOf(PrimitivesArrays.LongArray.class);

        // this later means, that it is not possible to insert an instant into it, even though the schema is for a timestamp-millis

        // adding to array, as per source code. Should not throw, but it does.
        // https://github.com/apache/avro/blob/main/lang/java/avro/src/main/java/org/apache/avro/generic/GenericDatumReader.java#L339
        assertThatThrownBy(() -> ((Collection) array).add(Instant.now()));

    }


    @Test
    void simplifiedTest_primitiveArrayDoesNotProperlyAddInstantsToArray() {
        // this is what basically happends under the hood:
        var longArray = (Collection) new PrimitivesArrays.LongArray(1, SCHEMA);

        assertThatThrownBy(() -> longArray.add(Instant.now()));
    }

    @Test
    void simplifiedTest_genericArrayWorksJustFine() {
        // if genericData.newArray returns a GenericData.Array for arrays of time timestamp, then it works just fine.

        var genericArray = (Collection) new GenericData.Array(1, SCHEMA);

        genericArray.add(INSTANT);

        assertThat(genericArray).containsExactly(INSTANT);
        assertThat(genericArray).hasSize(1);

        Object o = genericArray.stream().findFirst().get();

        assertThat(o).isEqualTo(INSTANT);
        assertThat(o).isInstanceOf(Instant.class);
        assertThat(o.toString()).isEqualTo(INSTANT_STRING);
    }
}
