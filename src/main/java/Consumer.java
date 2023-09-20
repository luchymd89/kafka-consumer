import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.io.IOException;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

    private static final Logger log = LogManager.getLogger(Consumer.class);

    private static final String TOPIC = "primer-topic";


    private static Consumer consumer;
    private KafkaConsumer<String, String> kafkaConsumer;

    private Consumer(){
        try {

            Properties conf = new Properties();
            // Se le pasa el lugar donde estan las propiedades
            conf.load(new FileReader("src/main/resources/consumer.properties"));

            this.kafkaConsumer = new KafkaConsumer<String, String>(conf);

        }catch (IOException ioe){
            log.error(ioe.getMessage());
        }
    }

    public static Consumer getInstance(){

        if (consumer == null){
            consumer = new Consumer();
        }


        return consumer;

    }

    public void start(){
        // Va a escuchar 100 mensajes y despues va a cerrar
        int count = 0;

        do{
            try{
                kafkaConsumer.subscribe(Arrays.asList(TOPIC));
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(20));
                //Cada 20 segundos devuelve un objeto con todos los datos que escucho en ese tiempo

                records.forEach(r -> {
                    String msg = String.format("offset %s. partition %s, key %s, value %s", r.offset(), r.partition(), r.key(), r.value());
                    log.info(msg);
                });



            }catch (KafkaException e){
                log.error(e.getMessage());
            }

            count ++;

        }while(count <= 100);


    }

    public void close(){
        this.kafkaConsumer.close();
    }

}
