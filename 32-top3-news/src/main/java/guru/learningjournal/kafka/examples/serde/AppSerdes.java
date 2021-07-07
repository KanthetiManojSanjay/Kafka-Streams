package guru.learningjournal.kafka.examples.serde;

import guru.learningjournal.kafka.examples.Top3NewsType;
import guru.learningjournal.kafka.examples.types.AdClick;
import guru.learningjournal.kafka.examples.types.AdInventories;
import guru.learningjournal.kafka.examples.types.ClicksByNewsType;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;

public class AppSerdes extends Serdes {

    static final class AdInventoriesSerde extends WrapperSerde<AdInventories> {
        AdInventoriesSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<AdInventories> AdInventories() {
        AdInventoriesSerde serde = new AdInventoriesSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, AdInventories.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

    static final class AdClickSerde extends WrapperSerde<AdClick> {
        AdClickSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<AdClick> AdClick() {
        AdClickSerde serde = new AdClickSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, AdClick.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

    static final class ClicksByNewsTypeSerde extends WrapperSerde<ClicksByNewsType> {
        ClicksByNewsTypeSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<ClicksByNewsType> ClicksByNewsType() {
        ClicksByNewsTypeSerde serde = new ClicksByNewsTypeSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, ClicksByNewsType.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

    static final class Top3NewsTypeSerde extends WrapperSerde<Top3NewsType> {
        Top3NewsTypeSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<Top3NewsType> Top3NewsType() {
        Top3NewsTypeSerde serde = new Top3NewsTypeSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Top3NewsType.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

}
