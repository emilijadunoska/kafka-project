package um.si;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class ReservationsProducer {

    private static final String CSV_FILE_PATH = "C:\\Users\\ID\\kafka-project\\KafkaProducer\\src\\main\\java\\um\\si\\reservations.csv";

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

    private static ProducerRecord<Object, Object> generateRecord(Schema schema) {
        try (BufferedReader reader = new BufferedReader(new FileReader(CSV_FILE_PATH))) {
            reader.readLine();

            String line;
            while ((line = reader.readLine()) != null) {
                String[] columns = line.split(";");
                if (columns.length == 8) {
                    GenericRecord avroRecord = new GenericData.Record(schema);

                    avroRecord.put("reservation_id", columns[0]);
                    avroRecord.put("date", columns[1]);
                    avroRecord.put("user_id", Integer.valueOf(columns[6]));
                    avroRecord.put("room_id", Integer.valueOf(columns[5]));
                    avroRecord.put("room_pricePerNight", Double.valueOf(columns[7]));

                    double roomPricePerNight = (Double) avroRecord.get("room_pricePerNight");
                    double totalReservationCost = calculateTotalReservationCost(roomPricePerNight);
                    avroRecord.put("total_reservation_cost", totalReservationCost);

                    ProducerRecord<Object, Object> producerRecord =
                            new ProducerRecord<>(TOPIC, columns[0], avroRecord);


                    return producerRecord;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
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

        try (BufferedReader reader = new BufferedReader(new FileReader(CSV_FILE_PATH))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] columns = line.split(";");
                if (columns.length == 8) {
                    ProducerRecord<Object, Object> record = generateRecord(schema);
                    producer.send(record);
                    System.out.println("[RECORD] Sent new reservation object.");
                    Thread.sleep(10000);
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static double calculateTotalReservationCost(double roomPricePerNight) {

        return roomPricePerNight * 3;
    }


}
