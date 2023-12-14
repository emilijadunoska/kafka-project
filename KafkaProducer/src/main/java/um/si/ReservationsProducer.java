package um.si;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class ReservationsProducer {

    private final static String TOPIC = "kafka-reservations";

    private static KafkaProducer createProducer() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // Configure the KafkaAvroSerializer.
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        // Schema Registry location.
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        return new KafkaProducer(props);
    }

    private static ProducerRecord<Object,Object> generateRecord(Schema schema) {
        Random rand = new Random();
        GenericRecord avroRecord = new GenericData.Record(schema);

        String reservationId = String.valueOf((int)(Math.random() * (10000 - 0 + 1) + 1));
        avroRecord.put("reservation_id", reservationId);
        avroRecord.put("user_id", Integer.valueOf(rand.nextInt((9-1)) + 1));
        avroRecord.put("user_username", "user_" + String.valueOf(rand.nextInt(1000)));
        avroRecord.put("room_id", Integer.valueOf(rand.nextInt((9-1)) + 1));
        String[] roomTypes = {"STANDARD", "DELUXE", "SUITE", "SINGLE", "DOUBLE", "TWIN"};
        avroRecord.put("room_type", roomTypes[rand.nextInt(roomTypes.length)]);
        avroRecord.put("room_pricePerNight", Double.valueOf(rand.nextDouble() * (100 - 50) + 50));
        avroRecord.put("reservation_cost_without_services", Double.valueOf(rand.nextDouble() * (500 - 200) + 200));
        avroRecord.put("booked_services", "your_booked_services");
        avroRecord.put("additional_services_price", Double.valueOf(rand.nextDouble() * (100 - 10) + 10));

        ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(TOPIC, reservationId, avroRecord);
        return producerRecord;
    }


    public static void main(String[] args) throws Exception {
        Schema schema = SchemaBuilder.record("Reservation")
                .fields()
                .requiredString("reservation_id")
                .requiredInt("user_id")
                .requiredString("user_username")
                .requiredInt("room_id")
                .requiredString("room_type")
                .requiredDouble("room_pricePerNight")
                .requiredDouble("reservation_cost_without_services")
                .requiredString("booked_services")
                .requiredDouble("additional_services_price")
                .endRecord();

        KafkaProducer producer = createProducer();


        while(true){
            ProducerRecord record = generateRecord(schema);
            producer.send(record);

            System.out.println("[RECORD] Sent new reservation object.");
            Thread.sleep(10000);
        }
    }

}
