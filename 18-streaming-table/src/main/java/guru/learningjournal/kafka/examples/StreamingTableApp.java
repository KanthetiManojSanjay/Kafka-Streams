package guru.learningjournal.kafka.examples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;


public class StreamingTableApp {

    private static Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreName);
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, String.valueOf(Serdes.String().getClass()));
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, String.valueOf(Serdes.String().getClass()));
        // To customise KTable Caching
        //props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, String.valueOf(1000));
        //props.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, String.valueOf(10 * 1024 * 1024L));

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> KT0 = builder.table(AppConfigs.topicName);
        KT0.toStream().print(Printed.<String, String>toSysOut().withLabel("KT0"));

        KTable<String, String> KT1 = KT0.filter((k, v) -> k.matches(AppConfigs.regExSymbol) && !v.isEmpty(),
                Materialized.as(AppConfigs.stateStoreName))
                //Emit rate
                //.suppress(Suppressed.untilTimeLimit(Duration.ofMinutes(5),Suppressed.BufferConfig.maxBytes(1000000L).emitEarlyWhenFull()))
                ;
        KT1.toStream().print(Printed.<String, String>toSysOut().withLabel("KT1"));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        QueryServer queryServer = new QueryServer(streams, AppConfigs.queryServerHost, AppConfigs.queryServerPort);
        streams.setStateListener((newState, oldState) -> {
            logger.info("State changing to " + newState + " from " + oldState);
            queryServer.setActive(newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING);
        });
        streams.start();
        queryServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down streams");
            queryServer.stop();
            streams.close();
        }));


    }
}
