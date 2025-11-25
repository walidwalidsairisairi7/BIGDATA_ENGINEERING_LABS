package edu.supmti.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class EventProducer {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Entrer le nom du topic");
            return;
        }

        String topicName = args[0];

        // Configuration du producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Créer le producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Envoyer quelques messages
        for (int i = 1; i <= 10; i++) {
            String key = "Key" + i;
            String value = "Message " + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
            RecordMetadata metadata = producer.send(record).get(); // envoie synchrone
            System.out.printf("Envoyé -> topic: %s, partition: %d, offset: %d, key: %s, value: %s%n",
                    metadata.topic(), metadata.partition(), metadata.offset(), key, value);
        }

        // Fermer le producer
        producer.close();
    }
}
