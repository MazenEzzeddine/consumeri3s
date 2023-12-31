import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;


public class Consumer {

    private static final Logger log = LogManager.getLogger(Consumer.class);
    public static KafkaConsumer<String, Customer> consumer = null;
    static double eventsViolating = 0;
    static double eventsNonViolating = 0;
    static double totalEvents = 0;
    static float maxConsumptionRatePerConsumer = 0.0f;



    static ArrayList<TopicPartition> tps;
    static KafkaProducer<String, Customer> producer;
    static float latency;


    public Consumer() throws
            IOException, URISyntaxException, InterruptedException {
    }


    public static void main(String[] args)
            throws IOException, URISyntaxException, InterruptedException {


        KafkaConsumerConfig config = KafkaConsumerConfig.fromEnv();
        log.info(KafkaConsumerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaConsumerConfig.createProperties(config);

        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                StickyAssignor.class.getName());

        consumer = new KafkaConsumer<String, Customer>(props);
        consumer.subscribe(Collections.singletonList(config.getTopic())/*, new RebalanceListener()*/);
        producer = Producer.producerFactory();

        log.info("Subscribed to topic {}", config.getTopic());

       // PrometheusUtils.initPrometheus();
        addShutDownHook();
        //startServer();
        tps = new ArrayList<>();
        tps.add(new TopicPartition("testtopic1", 0));
        tps.add(new TopicPartition("testtopic1", 1));
        tps.add(new TopicPartition("testtopic1", 2));
        tps.add(new TopicPartition("testtopic1", 3));
        tps.add(new TopicPartition("testtopic1", 4));





        int percentTopic2 = 1;

        try {
            while (true) {
                ConsumerRecords<String, Customer> records = consumer.poll
                        (Duration.ofMillis(Long.MAX_VALUE));
                //max = 0;
                if (records.count() != 0) {
                    log.info("received {}", records.count());
                    for (TopicPartition tp : tps) {

                        for (ConsumerRecord<String, Customer> record : records.records(tp)) {
                            totalEvents++;
                            //TODO sleep per record or per batch
                            try {
                                Thread.sleep(Long.parseLong(config.getSleep()));
                                if (System.currentTimeMillis() - record.timestamp() <= 500 /*1500*/) {
                                    eventsNonViolating++;
                                } else {
                                    eventsViolating++;
                                }

                             /*   producer.send(new ProducerRecord<String, Customer>
                                        ("testtopic22",
                                                tp.partition(), record.timestamp(),
                                                record.key(), record.value()));*/

                               /* PrometheusUtils.latencygaugemeasure
                                        .setDuration(System.currentTimeMillis() - record.timestamp());*/
                                log.info(" latency is {}", System.currentTimeMillis() - record.timestamp());

                                //function to do object detection...
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    consumer.commitSync();
                    log.info("maxConsumptionRatePerConsumer {}", maxConsumptionRatePerConsumer);
                    double percentViolating = eventsViolating / totalEvents;
                    double percentNonViolating = eventsNonViolating / totalEvents;
                    log.info("Percent violating so far {}", percentViolating);
                    log.info("Events Non Violating {}", eventsNonViolating);
                    log.info("Total Events {}", totalEvents);
                    log.info("Percent non violating so far {}", percentNonViolating);
                    log.info("total events {}", totalEvents);
                }
            }

        } catch (WakeupException e) {
            // e.printStackTrace();
        } finally {
            consumer.close();
            log.info("Closed consumer and we are done");
        }
    }


    private static void addShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Starting exit...");
                consumer.wakeup();
                try {
                    this.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }


    private static void startServer() {
        Thread server = new Thread(new ServerThread());
        server.start();
    }
}