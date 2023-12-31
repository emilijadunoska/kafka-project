package um.si;


import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class ReservationsConsumer {

    private final static String TOPIC = "kafka-reservations";

    private static KafkaConsumer<String, GenericRecord> createConsumer() {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "AvroConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-reservations");
        // Configure the KafkaAvroDeserializer.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        // Schema Registry location.
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer(props);
    }

    public static void main(String[] args) throws Exception {
       

        KafkaConsumer consumer = createConsumer()   ;
        consumer.subscribe(Arrays.asList(TOPIC));
        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(2000);
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.println("[CONSUMER RECORD]");
                    System.out.println("  Offset: " + record.offset());
                    System.out.println("  Key: " + record.key());
                    System.out.println("  Value:");

                    record.value().getSchema().getFields().forEach(field -> {
                        System.out.println("    " + field.name() + ": " + record.value().get(field.name()));
                    });

                    System.out.println();
                }

            }
        } finally {
            consumer.close();
        }
    }
}
