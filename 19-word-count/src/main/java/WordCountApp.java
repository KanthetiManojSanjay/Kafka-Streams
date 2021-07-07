import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
    private static final Logger logger = LogManager.getLogger();

    public static void main(final String[] args) {

        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, String.valueOf(Serdes.String().getClass()));
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, String.valueOf(Serdes.String().getClass()));
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreLocation);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> KS0 = streamsBuilder.stream(AppConfigs.topicName);
        KStream<String, String> KS1 = KS0.flatMapValues(v -> Arrays.asList(v.toLowerCase().split("")));

        KGroupedStream<String, String> KGS1 = KS1.groupBy((k, v) -> v);
        KTable<String, Long> KT1 = KGS1.count();

        KT1.toStream().print(Printed.<String, Long>toSysOut().withLabel("KT1"));

        Topology topology = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

        QueryServer queryServer = new QueryServer(kafkaStreams, AppConfigs.queryServerHost, AppConfigs.queryServerPort);
        kafkaStreams.setStateListener((newState, oldState) -> {
            queryServer.setActive(newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING);
        });
        queryServer.start();
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            queryServer.stop();
            kafkaStreams.close();
        }));


    }
}
