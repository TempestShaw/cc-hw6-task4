package com.cloudcomputing.samza.nycabs;

import com.cloudcomputing.samza.nycabs.application.AdPriceTaskApplication;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;

public class TestAdPriceTask {
    @Test
    public void testAdPriceTask() throws Exception {
        Map<String, String> confMap = new HashMap<>();
        confMap.put("stores.ad-store.factory", "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory");
        confMap.put("stores.ad-store.key.serde", "string");
        confMap.put("stores.ad-store.msg.serde", "integer");

        confMap.put("serializers.registry.json.class", "org.apache.samza.serializers.JsonSerdeFactory");
        confMap.put("serializers.registry.string.class", "org.apache.samza.serializers.StringSerdeFactory");
        confMap.put("serializers.registry.integer.class", "org.apache.samza.serializers.IntegerSerdeFactory");

        InMemorySystemDescriptor isd = new InMemorySystemDescriptor("kafka");

        InMemoryInputDescriptor imClicks = isd.getInputDescriptor("ad-click", new NoOpSerde<>());
        InMemoryOutputDescriptor outputAdPrice = isd.getOutputDescriptor("ad-price", new NoOpSerde<>());

        TestRunner
                .of(new AdPriceTaskApplication())
                .addInputStream(imClicks, TestUtils.genStreamData("ad-click"))
                .addOutputStream(outputAdPrice, 1)
                .addConfig(confMap)
                .run(Duration.ofSeconds(5));

        ListIterator<Object> resultIter = TestRunner.consumeStream(outputAdPrice, Duration.ofSeconds(5)).get(0).listIterator();

        if (resultIter.hasNext()) {
            Map<String, Object> clickTrue = (Map<String, Object>) resultIter.next();
            Assert.assertEquals("Ad revenue mismatch for clicked:true", 80, ((Number) clickTrue.get("ad")).intValue());
            Assert.assertEquals("Cab revenue mismatch for clicked:true", 20, ((Number) clickTrue.get("cab")).intValue());
        }

        if (resultIter.hasNext()) {
            Map<String, Object> clickFalse = (Map<String, Object>) resultIter.next();
            Assert.assertEquals("Ad revenue mismatch for clicked:false", 50, ((Number) clickFalse.get("ad")).intValue());
            Assert.assertEquals("Cab revenue mismatch for clicked:false", 50, ((Number) clickFalse.get("cab")).intValue());
        }
    }
}