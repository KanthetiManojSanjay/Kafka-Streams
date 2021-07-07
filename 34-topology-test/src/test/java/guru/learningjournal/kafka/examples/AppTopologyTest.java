package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.AdImpression;
import guru.learningjournal.kafka.examples.types.CampaignPerformance;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.*;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AppTopologyTest {

    private static TopologyTestDriver topologyDriver;

    @BeforeAll
    static void setAll() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreNameUT);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        AppTopology.withBuilder(streamsBuilder);
        Topology topology = streamsBuilder.build();

        topologyDriver = new TopologyTestDriver(topology, props);
    }

    @AfterAll
    static void cleanUpAll() {
        try {
            topologyDriver.close();
        } catch (Exception e) {
            //FileUtils.deleteDirectory(new File(AppConfigs.stateStoreNameUT));
        }

    }

    @Test
    @Order(1)
    @DisplayName("Test the impression flow from the source topic to the final output topic")
    void impressionFlowTest() {
        AdImpression impression = new AdImpression().withImpressionID("100001").withCampaigner("ABC Ltd");
        ConsumerRecordFactory consumerRecordFactory = new ConsumerRecordFactory(AppSerdes.String().serializer(),
                AppSerdes.AdImpression().serializer());

        topologyDriver.pipeInput(consumerRecordFactory.create(
                AppConfigs.impressionTopic,
                "100001",
                impression

        ));
        ProducerRecord<String, CampaignPerformance> record = topologyDriver.readOutput(AppConfigs.outputTopic,
                AppSerdes.String().deserializer(), AppSerdes.CampaignPerformance().deserializer());

        assertAll(
                () -> assertEquals("", record.value().getCampaigner()),
                () -> assertEquals("", record.value().getAdImpressions().toString())
        );
    }

    @Test
    @Order(2)
    @DisplayName("Test the state store holds the correct state")
    void stateStoreTest() {
        KeyValueStore<String, CampaignPerformance> store = topologyDriver.getKeyValueStore(AppConfigs.stateStoreName);
        CampaignPerformance cpValue = store.get("ABC Ltd");

        assertAll(
                () -> assertEquals("ABC Ltd", cpValue.getCampaigner()),
                () -> assertEquals("2", cpValue.getAdImpressions().toString()),
                () -> assertEquals("1", cpValue.getAdClicks().toString())
        );

    }
}