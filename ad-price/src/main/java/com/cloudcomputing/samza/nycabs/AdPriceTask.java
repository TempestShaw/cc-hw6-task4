package com.cloudcomputing.samza.nycabs;

import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import org.codehaus.jackson.map.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Consumes the stream of ad-click.
 * Outputs a stream which handles static file and one stream
 * and gives a stream of revenue distribution.
 */
public class AdPriceTask implements StreamTask, InitableTask {

    /*
       Define per task state here. (kv stores etc)
       READ Samza API part in Writeup to understand how to start
    */
    private KeyValueStore<String, Integer> storeAdPrices;
    private ObjectMapper mapper = new ObjectMapper();
    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        storeAdPrices = (KeyValueStore<String, Integer>) context.getTaskContext().getStore("ad-store");

        //Load the static data from resources into the store
        initialize("NYCstoreAds.json");
    }

    public void initialize(String adPriceFile) {
        try {
            // Using the helper method logic from your reference
            List<String> rawStrings = AdPriceConfig.readFile(adPriceFile);

            System.out.println("Reading ad price file from local resources: " + adPriceFile);

            for (String rawString : rawStrings) {
                try {
                    Map<String, Object> mapResult = mapper.readValue(rawString, HashMap.class);

                    String storeId = (String) mapResult.get("storeId");
                    Integer price = (Integer) mapResult.get("adPrice");

                    if (storeId != null && price != null) {
                        storeAdPrices.put(storeId, price);
                    }
                } catch (Exception e) {
                    System.out.println("Failed to parse ad price line: " + rawString);
                }
            }
            System.out.println("Static ad price data initialization complete.");

        } catch (Exception e) {
            System.err.println("Failed to initialize static ad price data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        /*
        All the messsages are partitioned by userId, which means the messages
        sharing the same userId will arrive at the same task, similar to the
        approach that MapReduce sends all the key value pairs with the same key
        into the same reducer.
        */
        String incomingStream = envelope.getSystemStreamPartition().getStream();

        if (incomingStream.equals(AdPriceConfig.AD_CLICK_STREAM.getStream())) {
            // Handle Ad-click messages
            Map<String, Object> message = (Map<String, Object>) envelope.getMessage();

            String storeId = (String) message.get("storeId");
            Integer userId = (Integer) message.get("userId");
            boolean isClicked = Boolean.parseBoolean(String.valueOf(message.get("clicked")));

            Integer totalPrice = storeAdPrices.get(storeId);

            if (totalPrice != null) {
                int adRevenue;
                int cabRevenue;

                if (isClicked) {
                    adRevenue = (int) (totalPrice * 0.8);
                    cabRevenue = (int) (totalPrice * 0.2);
                } else {
                    adRevenue = (int) (totalPrice * 0.5);
                    cabRevenue = (int) (totalPrice * 0.5);
                }

                // Output
                Map<String, Object> output = new HashMap<>();
                output.put("userId", userId);
                output.put("storeId", storeId);
                output.put("ad", adRevenue);
                output.put("cab", cabRevenue);

                collector.send(new OutgoingMessageEnvelope(AdPriceConfig.AD_PRICE_STREAM, output));
            }
        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }
}
