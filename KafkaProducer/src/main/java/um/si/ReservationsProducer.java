package um.si;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
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
        String randomDate = generateRandomDate();


        avroRecord.put("reservation_id", reservationId);
        avroRecord.put("date", randomDate);
        avroRecord.put("user_id", Integer.valueOf(rand.nextInt((9-1)) + 1));
        avroRecord.put("room_id", Integer.valueOf(rand.nextInt((9-1)) + 1));
        avroRecord.put("room_pricePerNight", Double.valueOf(rand.nextDouble() * (100 - 50) + 50));

        double roomPricePerNight = (Double) avroRecord.get("room_pricePerNight");
        double totalReservationCost = calculateTotalReservationCost(roomPricePerNight);
        avroRecord.put("total_reservation_cost", totalReservationCost);

        ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(TOPIC, reservationId, avroRecord);
        return producerRecord;
    }


    public static void main(String[] args) throws Exception {
        Schema schema = SchemaBuilder.record("Reservation")
                .fields()
                .requiredString("reservation_id")
                .requiredString("date")
                .requiredInt("user_id")
                .requiredInt("room_id")
                .requiredDouble("room_pricePerNight")
                .requiredDouble("total_reservation_cost")
                .endRecord();

        KafkaProducer producer = createProducer();


        while(true){
            ProducerRecord record = generateRecord(schema);
            producer.send(record);

            System.out.println("[RECORD] Sent new reservation object.");
            Thread.sleep(10000);
        }
    }

    private static String generateRandomDate() {
        long startDateMillis = System.currentTimeMillis() - (365 * 24 * 60 * 60 * 1000L);
        long endDateMillis = System.currentTimeMillis();
        long randomDateMillis = startDateMillis + (long) (Math.random() * (endDateMillis - startDateMillis));

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(new Date(randomDateMillis));
    }

    private static double calculateTotalReservationCost(double roomPricePerNight) {

        return roomPricePerNight * 3;
    }


}
