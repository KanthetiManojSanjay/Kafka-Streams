package guru.learningjournal.kafka.examples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.DepartmentAggregate;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Spark;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AppRestService {
    private static final Logger logger = LogManager.getLogger();
    private static final String NOT_FOUND = "{\"code\":204,\"message\":\"No Content\"}";
    private static final String SERVICE_UNAVAILABLE = "{\"code\":503,\"message\":\"Service Unavailable\"}";
    private static ObjectMapper objectMapper = new ObjectMapper();
    private final KafkaStreams streams;
    private final HostInfo hostInfo;
    private Client client;
    private Boolean isActive = false;

    public AppRestService(KafkaStreams kafkaStreams, String hostName, int port) {
        this.streams = kafkaStreams;
        this.hostInfo = new HostInfo(hostName, port);
        client = ClientBuilder.newClient();
    }

    void setActive(Boolean state) {
        isActive = state;
    }

    void start() {
        logger.info("Starting KTableAppDemo Query Server " + "http://" + hostInfo.host() + ":" + hostInfo.port());
        Spark.port(hostInfo.port());
        Spark.get("/getAggregate/:deptName", (req, res) -> {
            String results;
            String searchKey = req.params(":deptName");
            if (!isActive) {
                return SERVICE_UNAVAILABLE;
            } else {
                StreamsMetadata metadata = streams.metadataForKey(AppConfigs.stateStoreName, searchKey, AppSerdes.String().serializer());
                if (metadata.hostInfo().equals(hostInfo)) {
                    logger.info("Retrieving key/value from local");
                    results = getValue(searchKey);
                } else {
                    logger.info("Retrieving key/value from remote");
                    results = getValueFromRemote(searchKey, metadata.hostInfo());
                }

            }
            return results;
        });

        Spark.get("/getAggregate/all", (req, res) -> {
            List<KeyValue<String, DepartmentAggregate>> allResults = new ArrayList<>();
            String results;
            if (!isActive) {
                return SERVICE_UNAVAILABLE;
            } else {
                Collection<StreamsMetadata> streamsMetadata = streams.allMetadataForStore(AppConfigs.stateStoreName);
                for (StreamsMetadata metadata : streamsMetadata) {
                    if (metadata.hostInfo().equals(hostInfo)) {
                        logger.info("Retrieving key/value from local");
                        allResults.add((KeyValue<String, DepartmentAggregate>) getAllValues());
                    } else {
                        allResults.add(getAllValuesFromRemote(metadata.hostInfo()));
                    }
                }
                results = (allResults.size() == 0) ? NOT_FOUND : objectMapper.writeValueAsString(allResults);
            }
            return results;
        });

        Spark.get("/getAggregate/all", (req, res) -> {
            List<KeyValue<String, DepartmentAggregate>> allResults = new ArrayList<>();
            String results;
            if (!isActive) {
                return SERVICE_UNAVAILABLE;
            } else {
                logger.info("Retrieving all key/value from local");
                allResults = getAllValues();
                results = (allResults.size() == 0) ? NOT_FOUND : objectMapper.writeValueAsString(allResults);
            }
            return results;
        });
    }


    private KeyValue<String, DepartmentAggregate> getAllValuesFromRemote(HostInfo hostInfo) throws IOException {
        List<KeyValue<String, DepartmentAggregate>> results = new ArrayList<>();
        String result;
        String targetHost = String.format("http://%s:%d/dept/local", hostInfo.host(), hostInfo.port());
        result = client.target(targetHost).request(MediaType.APPLICATION_JSON).get(String.class);
        if (!result.equals(NOT_FOUND)) {
            results = objectMapper.readValue(result, results.getClass());
        }
        return (KeyValue<String, DepartmentAggregate>) results;
    }

    private String getValueFromRemote(String searchKey, HostInfo hostInfo) {
        String result;
        String targetHost = String.format("http://%s:%d/kv/%s", hostInfo.host(), hostInfo.port(), searchKey);
        result = client.target(targetHost).request(MediaType.APPLICATION_JSON).get(String.class);
        return result;
    }

    private String getValue(String searchKey) throws JsonProcessingException {
        ReadOnlyKeyValueStore<String, DepartmentAggregate> departmentStore = streams.store(AppConfigs.stateStoreName,
                QueryableStoreTypes.keyValueStore());
        DepartmentAggregate result = departmentStore.get(searchKey);
        return (result == null) ? NOT_FOUND : objectMapper.writeValueAsString(result);
    }

    private List<KeyValue<String, DepartmentAggregate>> getAllValues() {
        List<KeyValue<String, DepartmentAggregate>> results = new ArrayList<>();
        ReadOnlyKeyValueStore<String, DepartmentAggregate> departmentStore = streams.store(AppConfigs.stateStoreName,
                QueryableStoreTypes.keyValueStore());
        departmentStore.all().forEachRemaining(results::add);
        return results;
    }

    void stop() {
        client.close();
        Spark.stop();
    }
}
