package com.roncia.elasticsearch.application;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.roncia.elasticsearch.tools.com.roncia.elasticsearch.util.CommandLineUtil;
import com.sun.jmx.snmp.Timestamp;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.cluster.Health;
import io.searchbox.core.Bulk;
import io.searchbox.core.Bulk.Builder;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchScroll;
import io.searchbox.core.search.sort.Sort;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;
import io.searchbox.indices.settings.GetSettings;
import io.searchbox.params.Parameters;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.time.DurationFormatUtils;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;


public class IndexCloner {

    private static final Logger LOGGER = Logger.getLogger(IndexCloner.class.getName());

    private static long minTimestamp = Long.MAX_VALUE;

    /**
     * Index Cloner application main function
     *
     * @param args command line arguments representing source and destination index properties
     * @throws IOException
     * @throws ParseException
     * @throws InterruptedException
     * @throws RuntimeException
     */
    public static void main(String args[]) throws IOException, ParseException, InterruptedException, RuntimeException {
        long time = System.currentTimeMillis();
        CommandLine cmd = getCommandLine(args);
        String srcIndex = cmd.getOptionValue("srcIndex");
        String dstIndex = cmd.getOptionValue("dstIndex");
        String startTimestamp = cmd.getOptionValue("timestamp");
        JestClient src = getClient("srcHost", "srcUser", "srcPwd", cmd);
        JestClient dst = getClient("dstHost", "dstUser", "dstPwd", cmd);
        createDestinationIndexFromSource(srcIndex, dstIndex, src, dst, cmd);
        cloneData(src, dst, srcIndex, dstIndex, startTimestamp);
        logDuration(time);
    }

    private static CommandLine getCommandLine(String[] args) throws ParseException {
        return CommandLineUtil.readCommandLine(args);
    }

    private static void createDestinationIndexFromSource(
            String srcIndex, String dstIndex, JestClient src, JestClient dst, CommandLine cmd)
            throws IOException, InterruptedException {
        boolean keepDstIndex = Boolean.parseBoolean(cmd.getOptionValue("keepDstIndex"));
        if (!keepDstIndex) {
            copySettings(srcIndex, dstIndex, src, dst, cmd);
        } else {
            logInformation("Skip : Copying settings");
        }
    }

    private static void copySettings(String srcIndex, String dstIndex, JestClient src, JestClient dst, CommandLine cmd) throws IOException, InterruptedException {
        logInformation("Copying settings");
        JsonElement srcLoad = getSourceIndexSettings(src, srcIndex);
        modifyIndexReplicaConfigurations(cmd, srcLoad);
        deleteDestinationIndex(dst, dstIndex);
        createDestinationIndexFromSourceSettings(dst, dstIndex, srcLoad);
        applySourceMappingToDestinationIndex(src, dst, srcIndex, dstIndex);
        waitWhilstDestinationIndexIsInRedState(dst);
    }

    private static String getCurrentSettings(JsonElement srcLoad) {
        return srcLoad.getAsJsonObject().get("settings").getAsJsonObject().get("index").getAsJsonObject().toString();
    }

    private static JestClient getClient(String host, String user, String pwd, CommandLine cmd) {
        return getAuthenticatedClient(
                "http://" + cmd.getOptionValue(host), cmd.getOptionValue(user), cmd.getOptionValue(pwd));
    }

    private static void waitWhilstDestinationIndexIsInRedState(JestClient dst)
            throws IOException, InterruptedException {
        String clusterStatus;
        do {
            JestResult result = dst.execute(new Health.Builder().build());
            clusterStatus = result.getJsonObject().get("status").getAsString();
            Thread.sleep(500);
        } while ("red".equals(clusterStatus));
    }

    private static JestClient getAuthenticatedClient(String host, String user, String pwd) {
        JestClientFactory factory = new JestClientFactory();
        HttpClientConfig.Builder builder = new HttpClientConfig.Builder(host);
        if (user != null && pwd != null) {
            builder = builder.defaultCredentials(user, pwd);
        }
        //substantially high timeout to give the application a chance to response respond with adequate msg if any
        factory.setHttpClientConfig(builder.connTimeout(3 * 60 * 1000).readTimeout(3 * 60 * 1000).build());
        return factory.getObject();
    }

    private static Builder bulkRequestBuilder(String indexDst, JsonArray hits) {
        Builder bulk = new Builder().defaultIndex(indexDst);
        for (JsonElement hit : hits) {
            JsonObject h = hit.getAsJsonObject();
            String id = h.get("_id").getAsString();
            String t = h.get("_type").getAsString();
            long version = h.get("_version").getAsLong();
            String source = h.get("_source").getAsJsonObject().toString();
            long timestamp = h.getAsJsonArray("sort").get(0).getAsLong(); // could alternately add _timestamp to fields on query

            minTimestamp = Math.min(minTimestamp, timestamp);

            Index index = new Index.Builder(source)
                    .index(indexDst)
                    .type(t)
                    .id(id)
                    .setParameter(Parameters.ROUTING, id)
                    .setParameter(Parameters.VERSION, version)
                    .setParameter(Parameters.VERSION_TYPE, "external")
                    .setParameter(Parameters.TIMESTAMP, timestamp)
                    .build();

            bulk.addAction(index);
        }
        return bulk;
    }

    private static JsonElement getSourceIndexSettings(JestClient src, String indexSrc) throws IOException {
        GetSettings getSettings = new GetSettings.Builder().addIndex(indexSrc).prefixQuery(indexSrc).build();
        JestResult result = src.execute(getSettings);
        JsonElement srcLoad = result.getJsonObject().get(indexSrc);
        if (srcLoad == null) {
            throw new RuntimeException("The source index " + indexSrc + " doesn't exist. Impossible to continue!");
        }
        return srcLoad;
    }

    private static void modifyIndexReplicaConfigurations(CommandLine cmd, JsonElement srcLoad) {
        final JsonObject settings = srcLoad.getAsJsonObject().get("settings").getAsJsonObject();
        final JsonObject index = settings.get("index").getAsJsonObject();

        String dstIndexReplicas = cmd.getOptionValue("dstIndexReplicas");
        if (dstIndexReplicas != null && dstIndexReplicas.trim().length() > 0) {
            try {
                final int numberDstIndexReplicas = Integer.valueOf(dstIndexReplicas);
                index.addProperty("number_of_replicas", String.valueOf(numberDstIndexReplicas));
            } catch (NumberFormatException e) {
                throw new RuntimeException("Invalid Number for Replicas argument! " + e.getMessage());
            }
        }

        String dstIndexShards = cmd.getOptionValue("dstIndexShards");
        if (dstIndexShards != null && !dstIndexShards.isEmpty()) {
            try {
                final int numberDstIndexShards = Integer.valueOf(dstIndexShards);
                index.addProperty("number_of_shards", String.valueOf(numberDstIndexShards));
            } catch (NumberFormatException e) {
                throw new RuntimeException("Invalid Number for Replicas argument! " + e.getMessage());
            }
        }
    }

    private static void deleteDestinationIndex(JestClient dst, String indexDst) throws IOException {
        DeleteIndex indicesExists = new DeleteIndex.Builder(indexDst).build();
        JestResult delete = dst.execute(indicesExists);
        logInformation("delete: " + delete.getJsonString());
    }

    private static void createDestinationIndexFromSourceSettings(
            JestClient dst, String indexDst, JsonElement currentSettings)
            throws IOException {
        JestResult create = dst.execute(new CreateIndex.Builder(indexDst).settings(getCurrentSettings(currentSettings)).build());
        logInformation("create: " + create.getJsonString());
        confirmResponse(create, "Index not created properly!");
    }

    private static void applySourceMappingToDestinationIndex(
            JestClient src, JestClient dst, String indexSrc, String indexDst)
            throws RuntimeException, IOException {
        GetMapping getMapping = new GetMapping.Builder().addIndex(indexSrc).build();
        JsonElement oldMapping = src.execute(getMapping).getJsonObject().get(indexSrc).getAsJsonObject().get("mappings");
        if (oldMapping instanceof JsonObject) {
            JsonObject m = (JsonObject) oldMapping;
            for (Entry<String, JsonElement> e : m.entrySet()) {
                String type = e.getKey();
                String typeMapping = oldMapping.getAsJsonObject().get(type).getAsJsonObject().toString();
                PutMapping putMapping = new PutMapping.Builder(indexDst, type, typeMapping).build();

                JestResult dstResult = dst.execute(putMapping);
                confirmResponse(dstResult, "Mapping not loaded properly!");
            }
        }
        logInformation("oldMapping: " + oldMapping.toString());
    }

    private static void confirmResponse(JestResult result, String message) {
        if (!result.getJsonObject().get("acknowledged").getAsBoolean()) {
            throw new RuntimeException(message);
        }
    }

    private static void cloneData(JestClient src, JestClient dst, String indexSrc, String indexDst, String startTimestamp) throws IOException {
        logInformation("cloning data phase started");

        long startTime = System.currentTimeMillis();
        long totalAvail = 0;
        int sizePage = 250;
        int nHits = 0;
        int totHits = 0;
        int totalConflicts = 0;
        int totalRejections = 0;
        int totalUnknown = 0;

        boolean lastBatchWasRejected = false;

        String scrollId = null;
        JestResult ret = null;
        while (true) {

            if (ret == null || !lastBatchWasRejected) { // allows us to retry the last fetch if any items were rejected
                // Only on first page: Query
                if (scrollId == null) {
                    String query = "{\"query\":{\"match_all\":{}}}";

                    if (startTimestamp != null)
                        query = "{\"query\":{\"range\":{\"_timestamp\":{\"lte\":" + startTimestamp + "}}}}";

                    Search search = new Search.Builder(query).addIndex(indexSrc).setParameter(Parameters.SIZE, sizePage)
                            .setParameter(Parameters.SCROLL, "5m")
                            .setParameter(Parameters.VERSION, true)
                            .setParameter(Parameters.TIMESTAMP, true)
                            .addSort(new Sort("_timestamp", Sort.Sorting.DESC))
                            .build();
                    ret = src.execute(search);
                    scrollId = ret.getJsonObject().get("_scroll_id").getAsString();
                }
                // Since second page: Scroll
                else {
                    SearchScroll scroll = new SearchScroll.Builder(scrollId, "5m")
                            .setParameter(Parameters.SIZE, sizePage).build();

                    ret = src.execute(scroll);
                }
            }

            lastBatchWasRejected = false;

            JsonElement result = ret.getJsonObject().get("hits");

            if (totalAvail == 0)
                totalAvail = result.getAsJsonObject().get("total").getAsLong();

            JsonArray hits = result.getAsJsonObject().get("hits").getAsJsonArray();
            nHits = hits.size();
            if (nHits == 0) {
                break;
            }

            Builder bulk = bulkRequestBuilder(indexDst, hits);

            JestResult response = null;
            try {
                Bulk build = bulk.build();
//                logInformation(new Timestamp(System.currentTimeMillis()).toString());
                response = dst.execute(build);
            } catch (SocketTimeoutException e) {
                logInformation(" ------ socket exp");
                logInformation(new Timestamp(System.currentTimeMillis()).toString());
                e.printStackTrace();
                continue; // loop back around again
            }

            JsonObject results = response.getJsonObject();

            boolean hasErrors = results.has("errors") && results.getAsJsonPrimitive("errors").getAsBoolean();

            if (hasErrors) {
                JsonArray items = results.getAsJsonArray("items");

                for (JsonElement item : items) {
                    JsonObject index = item.getAsJsonObject().getAsJsonObject("index");

                    if (index.getAsJsonPrimitive("error") == null)
                        continue;

                    String error = index.getAsJsonPrimitive("error").getAsString();

                    // VersionConflictEngineException[[cascading_4_old][0] [flow][FCC1A713CF01954D6C29501A777FDF5C]: version conflict, current [-1], provided [1455630167879]]
                    if (error != null && error.contains("VersionConflictEngineException")) {
                        logInformation("version conflict: " + error);
                        totalConflicts++;
                    } else if (error != null && error.contains("EsRejectedExecutionException")) { // EsRejectedExecutionException
                        logInformation("rejected bulk put, will sleep 10 seconds: " + error);
                        totalRejections++;
                        lastBatchWasRejected = true;
                        try {
                            Thread.sleep(10 * 1000);
                        } catch (InterruptedException e) {
                            // do nothing
                        }
                        break;
                    } else {
                        logInformation("unknown error: " + error);
                        totalUnknown++;
                    }
                }
            }

            if (!lastBatchWasRejected) // don't double count, will try again
                totHits += nHits;

            long currentDuration = System.currentTimeMillis() - startTime;
            long remainingDuration = (currentDuration / totHits) * (totalAvail - totHits);

            System.out.println("available: " + totalAvail +
                    " batch size: " + nHits +
                    " remaining: " + (totalAvail - totHits) +
                    " min ts: " + minTimestamp +
                    " complete: " + Math.round(((float) totHits / (float) totalAvail) * 100.0) + "%" +
                    " rejections: " + totalRejections + // count put puts that rejected
                    " conflicts: " + totalConflicts +
                    " other errors: " + totalUnknown +
                    " remaining time: " + formatDurationHMSms(remainingDuration));


//            logResponse(response);
        }
        logInformation("Copied successfully " + (totHits - totalConflicts - totalUnknown) + " documents");
        logInformation("Conflict errors " + totalConflicts);
        logInformation("Unknown errors " + totalUnknown);
        logInformation("cloning data phase finished");
    }

    private static void logResponse(JestResult response) {
        if (response != null) {
            logInformation(response.getJsonString());
        }
    }

    private static void logDuration(long time) {
        long completedIn = System.currentTimeMillis() - time;
        String duration = DurationFormatUtils.formatDuration(completedIn, "HH:mm:ss:SS");
        logInformation("Duration : " + duration);
    }

    private static void logInformation(String message) {
        LOGGER.log(Level.INFO, message);
    }

    public static String formatDurationHMSms(long duration) {
        long ms = duration % 1000;
        long durationSeconds = duration / 1000;
        long seconds = durationSeconds % 60;
        long minutes = (durationSeconds / 60) % 60;
        long hours = durationSeconds / 60 / 60;

        return String.format("%02d:%02d:%02d.%03d", hours, minutes, seconds, ms);
    }
}
