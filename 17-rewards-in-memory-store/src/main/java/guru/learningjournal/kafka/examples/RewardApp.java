package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class RewardApp {

    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, AppConfig.applicationID);
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.bootstrapServers);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, PosInvoice> KS0 = builder.stream(AppConfig.posTopicName,
                Consumed.with(AppSerdes.String(), AppSerdes.PosInvoice()))
                .filter((k, v) ->
                        v.getCustomerType().equalsIgnoreCase(AppConfig.CUSTOMER_TYPE_PRIME));

        /*Map<String, String> changeLogConfig = new HashMap<>();
        changeLogConfig.put("min.insync.replicas", "2");*/

        StoreBuilder kvStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(AppConfig.REWARDS_STORE_NAME),
                AppSerdes.String(), AppSerdes.Double())
                //.withLoggingDisabled() // If we want to disable stateStore changeLog topic to backup.
                //.withLoggingEnabled(changeLogConfig) // To customize changeLog using topic config as shown above
                ;

        builder.addStateStore(kvStoreBuilder);

        // Custom Repartitioner is used to make use of customerId instead of storeId while doing partitioning
        KS0.through(AppConfig.REWARDS_TEMP_TOPIC, Produced.with(AppSerdes.String(), AppSerdes.PosInvoice(), new RewardsPartitioner()))
                .transformValues(() -> new RewardsTransformer(), AppConfig.REWARDS_STORE_NAME)
                .to(AppConfig.notificationTopic, Produced.with(AppSerdes.String(), AppSerdes.Notification()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping streams");
            kafkaStreams.cleanUp();
        }));
    }
}
