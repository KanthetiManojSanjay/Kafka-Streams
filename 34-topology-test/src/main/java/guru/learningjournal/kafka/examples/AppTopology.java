package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.AdClick;
import guru.learningjournal.kafka.examples.types.AdImpression;
import guru.learningjournal.kafka.examples.types.CampaignPerformance;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AppTopology {

    private static final Logger logger = LogManager.getLogger();

    static void withBuilder(StreamsBuilder streamsBuilder) {

        KStream<String, AdImpression> KS0 = streamsBuilder.stream(AppConfigs.impressionTopic,
                Consumed.with(AppSerdes.String(), AppSerdes.AdImpression()));

        KTable<String, Long> adImpressionCount = KS0.groupBy((k, v) -> v.getCampaigner(),
                Grouped.with(AppSerdes.String(), AppSerdes.AdImpression()))
                .count();

        KStream<String, AdClick> KS1 = streamsBuilder.stream(AppConfigs.clicksTopic,
                Consumed.with(AppSerdes.String(), AppSerdes.AdClick()));

        KTable<String, Long> adClickCount = KS1.groupBy((k, v) -> v.getCampaigner(),
                Grouped.with(AppSerdes.String(), AppSerdes.AdClick()))
                .count();

        KTable<String, CampaignPerformance> campaignPerformance = adImpressionCount.leftJoin(
                adClickCount, (impCount, clkCount) -> new CampaignPerformance()
                        .withAdImpressions(impCount)
                        .withAdClicks(clkCount))
                .mapValues((k, v) -> v.withCampaigner(k),
                        Materialized.<String, CampaignPerformance, KeyValueStore<Bytes, byte[]>>as(
                                AppConfigs.stateStoreName).withKeySerde(AppSerdes.String())
                                .withValueSerde(AppSerdes.CampaignPerformance()));

        //campaignPerformance.toStream().foreach((k, v) -> logger.info(v));
        campaignPerformance.toStream().to(AppConfigs.outputTopic,
                Produced.with(AppSerdes.String(), AppSerdes.CampaignPerformance()));
    }

}
