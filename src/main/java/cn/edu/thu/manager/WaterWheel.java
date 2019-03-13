package cn.edu.thu.manager;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import indexingTopology.api.client.*;
import indexingTopology.bolt.InputStreamReceiverBolt;
import indexingTopology.bolt.InputStreamReceiverBoltServer;
import indexingTopology.bolt.QueryCoordinatorBolt;
import indexingTopology.bolt.QueryCoordinatorWithQueryReceiverServerBolt;
import indexingTopology.common.aggregator.AggregateField;
import indexingTopology.common.aggregator.Aggregator;
import indexingTopology.common.aggregator.Count;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.config.TopologyConfig;
import indexingTopology.topology.TopologyGenerator;
import indexingTopology.util.AvailableSocketPool;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WaterWheel implements IDataBase {

    private static Logger logger = LoggerFactory.getLogger(WaterWheel.class);
    private DataSchema schema;
    private Config myConfig;
    private static final String TIME = "timestamp";
    private IngestionClientBatchMode ingestionClient = null;

    public WaterWheel(Config myConfig, boolean forQuery) {
        this.myConfig = myConfig;
        schema = getSchema(myConfig);
        try {
            if (!forQuery) {
                ingestionClient = new IngestionClientBatchMode(myConfig.WATERWHEEL_IP, myConfig.WATERWHEEL_INGEST_PORT, schema, 1024);
                ingestionClient.connectWithTimeout(10000);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public long insertBatch(List<Record> records) {

        List<DataTuple> tuples = convertToTuples(records);

        long start = System.currentTimeMillis();

        try {
            for (DataTuple tuple : tuples) {
                ingestionClient.appendInBatch(tuple);
            }
            ingestionClient.flush();
            ingestionClient.waitFinish();
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
        DataTuple tuple = new DataTuple();
        long tagV = toLong(record.tag);
        tuple.add(tagV);
        tuple.add(record.timestamp);
        for (Object field : record.fields) {
            double doubleV = ((Float) field).doubleValue();
            tuple.add(doubleV);
        }
        return tuple;
    }

    /**
     * @param tag must be in the format of: 000_111
     * @return
     */
    private long toLong(String tag) {
        StringBuilder builder = new StringBuilder();

        for (String s : tag.split("_")) {
            builder.append(s);
        }

        return Long.parseLong(builder.toString());
    }


    @Override
    public void createSchema() {

    }

    @Override
    public long count(String tagValue, String field, long startTime, long endTime) {

        final QueryClient queryClient = new QueryClient(myConfig.WATERWHEEL_IP, 10001);

        try {
            queryClient.connectWithTimeout(10000);
        } catch (IOException e) {
            e.printStackTrace();
        }

        long tagV = toLong(tagValue);

        Aggregator<Integer> aggregator = new Aggregator<>(schema, myConfig.TAG_NAME, new AggregateField(new Count(), field));

        long start = System.currentTimeMillis();
        try {
            //a key range query
            QueryResponse response = queryClient.query(new QueryRequest<>(tagV, tagV, Long.MIN_VALUE, Long.MAX_VALUE, aggregator));
            logger.info("result: {}", response.getTuples().get(0).get(1));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return System.currentTimeMillis() - start;
    }

    @Override
    public long flush() {
        return 0;
    }

    @Override
    public long close() {
        try {
            if (ingestionClient != null) {
                ingestionClient.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }


    /**
     * deploy WaterWheel
     */
    public static void main(String... args) {
        Config myConfig;
        if (args.length > 0) {
            try {
                FileInputStream fileInputStream = new FileInputStream(args[0]);
                myConfig = new cn.edu.thu.common.Config(fileInputStream);
            } catch (Exception e) {
                e.printStackTrace();
                myConfig = new cn.edu.thu.common.Config();
            }
        } else {
            myConfig = new cn.edu.thu.common.Config();
        }

        TopologyConfig config = new TopologyConfig();

        LocalCluster cluster;

        config.dataChunkDir = "./target/tmp";
        config.metadataDir = "./target/tmp";
        config.CHUNK_SIZE = 512 * 1024;
        config.HDFSFlag = false;
        config.previousTime = Integer.MAX_VALUE;
        System.out.println("dataChunkDir is set to " + config.dataChunkDir);
        cluster = new LocalCluster();

        DataSchema schema = getSchema(myConfig);

        final long minIndex = 0L;
        final long maxIndex = Long.MAX_VALUE;

        AvailableSocketPool socketPool = new AvailableSocketPool();
        int ingestionPort = socketPool.getAvailablePort();
        int queryPort = socketPool.getAvailablePort();

        TopologyGenerator<Long> topologyGenerator = new TopologyGenerator<>();

        InputStreamReceiverBolt inputStreamReceiverBolt = new InputStreamReceiverBoltServer(schema, ingestionPort, config);
        QueryCoordinatorBolt<Long> coordinator = new QueryCoordinatorWithQueryReceiverServerBolt<>(minIndex, maxIndex, queryPort,
                config, schema);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, minIndex, maxIndex, false, inputStreamReceiverBolt,
                coordinator, config);

        org.apache.storm.Config conf = new org.apache.storm.Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

        cluster.submitTopology("qiao_topology", conf, topology);

        logger.info("topology submitted, ingestion port: {}, query port: {}", ingestionPort, queryPort);
    }

    static DataSchema getSchema(Config config) {
        DataSchema schema = new DataSchema();

        schema.addLongField(config.TAG_NAME);
        schema.setPrimaryIndexField(config.TAG_NAME);

        schema.addLongField(TIME);
        schema.setTemporalField(TIME);

        for (String field : config.FIELDS) {
            schema.addDoubleField(field);
        }
        return schema;
    }
}
