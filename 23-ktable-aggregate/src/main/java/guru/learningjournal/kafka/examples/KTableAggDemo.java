package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.DepartmentAggregate;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class KTableAggDemo {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        String hostName = (args.length < 2) ? "localhost" : args[0];
        int portNumber = (args.length < 2) ? 7010 : Integer.parseInt(args[1]);
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreLocation);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, hostName + ":" + portNumber);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.table(AppConfigs.topicName,
                Consumed.with(AppSerdes.String(), AppSerdes.Employee()))
                .groupBy((k, v) -> KeyValue.pair(v.getDepartment(), v), Grouped.with(AppSerdes.String(), AppSerdes.Employee()))
                .aggregate(
                        //Initializer
                        () -> new DepartmentAggregate().withEmployeeCount(1).withTotalSalary(0).withAvgSalary(0D),
                        //Adder
                        (k, v, aggValue) -> new DepartmentAggregate().withEmployeeCount(aggValue.getEmployeeCount() + 1)
                                .withTotalSalary(aggValue.getTotalSalary() + v.getSalary())
                                .withAvgSalary((aggValue.getTotalSalary() + v.getSalary()) / (aggValue.getEmployeeCount() + 1D)),
                        //Substractor
                        (k, v, aggValue) -> new DepartmentAggregate().withEmployeeCount(aggValue.getEmployeeCount() - 1)
                                .withTotalSalary(aggValue.getTotalSalary() - v.getSalary())
                                .withAvgSalary((aggValue.getTotalSalary() - v.getSalary()) / (aggValue.getEmployeeCount() - 1D)),
                        //Serializer
                        Materialized.<String, DepartmentAggregate, KeyValueStore<Bytes, byte[]>>as(AppConfigs.stateStoreName)
                                .withKeySerde(AppSerdes.String())
                                .withValueSerde(AppSerdes.DepartmentAggregate())
                ).toStream().print(Printed.<String, DepartmentAggregate>toSysOut().withLabel("Department Aggregate"));
        ;


        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
        AppRestService queryServer = new AppRestService(streams, hostName, portNumber);
        streams.setStateListener((currentState, prevState) -> {
            logger.info("State Changing to " + currentState + "from previous state " + prevState);
            queryServer.setActive(currentState == KafkaStreams.State.RUNNING && prevState == KafkaStreams.State.REBALANCING);
        });

        streams.start();
        queryServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            queryServer.stop();
            streams.close();
        }));
    }
}
