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
    static float ConsumptionRatePerConsumerInThisPoll = 0.0f;
    static float averageRatePerConsumerForGrpc = 0.0f;
    static long pollsSoFar = 0;

   static  ArrayList<TopicPartition> tps;
    static KafkaProducer<String, Customer> producer;


    public Consumer() throws
            IOException, URISyntaxException, InterruptedException {
    }


    public static void main(String[] args)
            throws IOException, URISyntaxException, InterruptedException {

        log.info("Hello Ashwini");

        KafkaConsumerConfig config = KafkaConsumerConfig.fromEnv();
        log.info(KafkaConsumerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaConsumerConfig.createProperties(config);

        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                StickyAssignor.class.getName());

        boolean commit = !Boolean.parseBoolean(config.getEnableAutoCommit());
        consumer = new KafkaConsumer<String, Customer>(props);
        consumer.subscribe(Collections.singletonList(config.getTopic()));
        log.info("Subscribed to topic {}", config.getTopic());

        addShutDownHook();
        tps = new ArrayList<>();
        tps.add(new TopicPartition("testtopic1", 0));
        tps.add(new TopicPartition("testtopic1", 1));
        tps.add(new TopicPartition("testtopic1", 2));
        tps.add(new TopicPartition("testtopic1", 3));
        tps.add(new TopicPartition("testtopic1", 4));

        try {
            while (true) {
                Long timeBeforePolling = System.currentTimeMillis();
                ConsumerRecords<String, Customer> records = consumer.poll
                        (Duration.ofMillis(Long.MAX_VALUE));

                if (records.count() != 0) {
                    for (TopicPartition tp : tps) {

                        for (ConsumerRecord<String, Customer> record : records.records(tp)) {
                            totalEvents++;
                            if (System.currentTimeMillis() - record.timestamp() <= 5000) {
                                eventsNonViolating++;
                            } else {
                                eventsViolating++;
                            }
                            //TODO sleep per record or per batch
                            try {
                                Thread.sleep(Long.parseLong(config.getSleep()));

                                //function to do object detection...

                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                   }


                    consumer.commitSync();

                    log.info("In this poll, received {} events", records.count());
                    Long timeAfterPollingProcessingAndCommit = System.currentTimeMillis();
                    ConsumptionRatePerConsumerInThisPoll = ((float) records.count() /
                            (float) (timeAfterPollingProcessingAndCommit - timeBeforePolling)) * 1000.0f;
                    pollsSoFar += 1;
                    averageRatePerConsumerForGrpc = averageRatePerConsumerForGrpc +
                            (ConsumptionRatePerConsumerInThisPoll -
                                    averageRatePerConsumerForGrpc) / (float) (pollsSoFar);

                    if (maxConsumptionRatePerConsumer < ConsumptionRatePerConsumerInThisPoll) {
                        maxConsumptionRatePerConsumer = ConsumptionRatePerConsumerInThisPoll;
                    }
                    log.info("ConsumptionRatePerConsumerInThisPoll in this poll {}",
                            ConsumptionRatePerConsumerInThisPoll);
                    log.info("maxConsumptionRatePerConsumer {}", maxConsumptionRatePerConsumer);
                    double percentViolating = (double) eventsViolating / (double) totalEvents;
                    double percentNonViolating = (double) eventsNonViolating / (double) totalEvents;
                    log.info("Percent violating so far {}", percentViolating);
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
}