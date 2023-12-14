package um.si;


import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.*;
public class ReservationStreamer {
    private static Properties setupApp() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "GroupReservations");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());
        props.put("schema.registry.url", "http://0.0.0.0:8081");
        props.put("input.topic.name", "kafka-reservations");
        props.put("default.deserialization.exception.handler", "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");

        return props;
    }

    public static void main(String[] args) throws Exception {

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                "http://0.0.0.0:8081");

        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, GenericRecord> inputStream = builder.stream("kafka-reservations", Consumed.with(Serdes.Integer(), valueGenericAvroSerde));

        // group reservations by user_id and counts the number of reservations for each user
        inputStream.map((k, v) -> {
                    Integer userId = (Integer) v.get("user_id");
                    String reservationId = v.get("reservation_id").toString();
                    Double reservationCost = (Double) v.get("total_reservation_cost");

                    return new KeyValue<>(userId, reservationId + ", cost: " + reservationCost);
                })
                .groupByKey(Grouped.with(Serdes.Integer(), Serdes.String()))
                .count()
                .toStream()
                .mapValues(value -> value.toString())
                .to("kafka-grouped-reservations", Produced.with(Serdes.Integer(), Serdes.String()));

        inputStream.print(Printed.<Integer, GenericRecord>toSysOut().withLabel("kafka-reservations"));

        // aggregates the total reservation cost per user_id
        KStream<Integer, Double> userTotalCostStream = inputStream
                .filter((key, value) -> value != null && value.get("total_reservation_cost") != null && value.get("user_id") != null)
                .map((key, value) -> new KeyValue<>(Integer.valueOf(value.get("user_id").toString()), Double.valueOf(value.get("total_reservation_cost").toString())))
                .filter((userId, totalCost) -> userId > 0 && totalCost >= 100.0 && totalCost <= 500.0) // Example: Filter based on a cost range
                .groupByKey(Grouped.with(Serdes.Integer(), Serdes.Double()))
                .reduce(Double::sum)
                .toStream();

        userTotalCostStream.print(Printed.<Integer, Double>toSysOut().withLabel("kafka-user-total-cost"));

        // aggregates the total reservation cost per room_id with a time window
        KStream<Integer, Double> roomTotalCostStream = inputStream
                .filter((key, value) -> value != null && value.get("total_reservation_cost") != null && value.get("room_id") != null)
                .map((key, value) -> new KeyValue<>(Integer.valueOf(value.get("room_id").toString()), Double.valueOf(value.get("total_reservation_cost").toString())))
                .filter((roomId, totalCost) -> roomId > 0 && totalCost >= 100.0 && totalCost <= 500.0) // Example: Filter based on a cost range
                .groupByKey(Grouped.with(Serdes.Integer(), Serdes.Double()))
                .windowedBy(TimeWindows.of(Duration.ofHours(1)))
                .reduce(Double::sum)
                .toStream()
                .map((windowedKey, value) -> new KeyValue<>(windowedKey.key(), value));

        roomTotalCostStream.print(Printed.<Integer, Double>toSysOut().withLabel("kafka-room-total-cost"));


        Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, setupApp());
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
