package cn.edu.thu.manager;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import indexingTopology.api.client.IngestionClient;
import indexingTopology.bolt.InputStreamReceiverBolt;
import indexingTopology.bolt.InputStreamReceiverBoltServer;
import indexingTopology.bolt.QueryCoordinatorBolt;
import indexingTopology.bolt.QueryCoordinatorWithQueryReceiverServerBolt;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.config.TopologyConfig;
import indexingTopology.topology.TopologyGenerator;
import indexingTopology.util.AvailableSocketPool;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WaterWheel implements IDataBase {

    private DataSchema rawSchema;
    private Config config;
    private final String TIME = "timestamp";
    private IngestionClient oneTuplePerTransferIngestionClient = null;
    AvailableSocketPool socketPool = new AvailableSocketPool();
    private int ingestionPort = 0;
    private int queryPort = 0;

    public WaterWheel(Config config) {
        this.config = config;

        // create schema
        rawSchema = new DataSchema();

        // series id
        rawSchema.addVarcharField(config.TAG_NAME, 100);
        rawSchema.setPrimaryIndexField(config.TAG_NAME);
        // time
        rawSchema.addLongField(TIME);
        rawSchema.setTemporalField(TIME);
        // fields
        for (int i = 0; i < config.FIELDS.length; i++) {
            rawSchema.addDoubleField(config.FIELDS[i]);
        }

    }

    @Override
    public long insertBatch(List<Record> records) {

        List<DataTuple> tuples = convertToTuples(records);

        long start = System.currentTimeMillis();
        try {

            IngestionClient oneTuplePerTransferIngestionClient = new IngestionClient("localhost", ingestionPort);
            oneTuplePerTransferIngestionClient.connectWithTimeout(30000);

            // add points
            for (DataTuple tuple : tuples) {
                oneTuplePerTransferIngestionClient.append(tuple);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return System.currentTimeMillis() - start;

    }

    private List<DataTuple> convertToTuples(List<Record> records) {
        List<DataTuple> tuples = new ArrayList<>();
        for (Record record : records) {
            tuples.add(convertToTuple(record));
        }
        return tuples;
    }

    private DataTuple convertToTuple(Record record) {
        Comparable[] fields = new Comparable[record.fields.size() + 2];
        fields[0] = record.tag;
        fields[1] = record.timestamp;
        for (int i = 0; i < record.fields.size(); i++) {
            fields[i + 2] = ((Float) record.fields.get(i)).doubleValue();
        }

        return new DataTuple(fields);
    }

    @Override
    public void createSchema() {
        final String topologyName = "testTopologyIntegerFilter";

        ingestionPort = socketPool.getAvailablePort();
        queryPort = socketPool.getAvailablePort();

        Integer lowerBound = 0;
        Integer upperBound = 5000;

        final boolean enableLoadBalance = false;

        TopologyConfig config = new TopologyConfig();
        config.HDFS_HOST = "hdfs://127.0.0.1:9000/";
        config.HDFSFlag = true;
        config.dataChunkDir = "./waterwheel";
        config.metadataDir = "./waterwheel";
        config.previousTime = Integer.MAX_VALUE;

        InputStreamReceiverBolt dataSource = new InputStreamReceiverBoltServer(rawSchema, ingestionPort, config);
        QueryCoordinatorBolt<Integer> queryCoordinatorBolt = new QueryCoordinatorWithQueryReceiverServerBolt<>(lowerBound,
                upperBound, queryPort, config, rawSchema);

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();

        StormTopology topology = topologyGenerator.generateIndexingTopology(rawSchema, lowerBound, upperBound,
                enableLoadBalance, dataSource, queryCoordinatorBolt, config);

        org.apache.storm.Config conf = new org.apache.storm.Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

        conf.put(org.apache.storm.Config.WORKER_CHILDOPTS, "-Xmx2048m");
        conf.put(org.apache.storm.Config.WORKER_HEAP_MEMORY_MB, 2048);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, topology);
    }

    @Override
    public long count(String tagValue, String field, long startTime, long endTime) {
        return 0;
    }

    @Override
    public long close() {
        try {
            if (oneTuplePerTransferIngestionClient != null) {
                oneTuplePerTransferIngestionClient.close();
            }
            socketPool.returnPort(ingestionPort);
            socketPool.returnPort(queryPort);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
