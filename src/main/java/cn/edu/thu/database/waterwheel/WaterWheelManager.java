package cn.edu.thu.database.waterwheel;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.database.IDataBaseManager;


import indexingTopology.api.client.QueryClient;
import indexingTopology.api.client.QueryRequest;
import indexingTopology.api.client.QueryResponse;
import indexingTopology.common.aggregator.*;
import indexingTopology.common.data.DataSchema;

import indexingTopology.api.client.IngestionClientBatchMode;
import indexingTopology.common.data.DataTuple;

import indexingTopology.bolt.InputStreamReceiverBolt;
import indexingTopology.bolt.InputStreamReceiverBoltServer;
import indexingTopology.bolt.QueryCoordinatorBolt;
import indexingTopology.bolt.QueryCoordinatorWithQueryReceiverServerBolt;
import indexingTopology.config.TopologyConfig;
import indexingTopology.topology.TopologyGenerator;
import indexingTopology.util.AvailableSocketPool;

import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WaterWheelManager implements IDataBaseManager {

  private static Logger logger = LoggerFactory.getLogger(WaterWheelManager.class);
  private DataSchema schema;
  private Config config;
  private IngestionClientBatchMode ingestionClient;

  public WaterWheelManager(Config config) {
    this.config = config;
    schema = getSchema(config);
  }

  @Override
  public void initServer() {

  }

  @Override
  public void initClient() {
    try {
      if (!Config.FOR_QUERY) {
        ingestionClient = new IngestionClientBatchMode(config.WATERWHEEL_IP,
            config.WATERWHEEL_INGEST_PORT, schema, 1024);
        ingestionClient.connectWithTimeout(10000);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public long insertBatch(List<Record> records) {

    List<DataTuple> tuples = convertToTuples(records);

    long start = System.nanoTime();

    try {
      for (DataTuple tuple : tuples) {
        ingestionClient.appendInBatch(tuple);
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

    return System.nanoTime() - start;

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
   */
  private long toLong(String tag) {
    StringBuilder builder = new StringBuilder();

    for (String s : tag.split("_")) {
      builder.append(s);
    }

    return Long.parseLong(builder.toString());
  }


  @Override
  public long count(String tagValue, String field, long startTime, long endTime) {

    final QueryClient queryClient = new QueryClient(config.WATERWHEEL_IP, config.WATERWHEEL_QUERY_PORT);

    try {
      queryClient.connectWithTimeout(10000);
    } catch (IOException e) {
      e.printStackTrace();
    }

    long tagV = toLong(tagValue);

    Aggregator<Integer> aggregator = new Aggregator<>(schema, Config.TAG_NAME,
        new AggregateField(new Count(), field));

    long start = System.nanoTime();
    try {
      //a key range query
      QueryResponse response = queryClient
            .query(new QueryRequest<>(tagV, tagV, startTime, endTime, aggregator));
      logger.info("result: {}", response != null ? response.getTuples().get(0).get(1) : "null");
    } catch (IOException e) {
      e.printStackTrace();
    }

    return System.nanoTime() - start;
  }

  @Override
  public long flush() {
    long start = System.nanoTime();
    try {
      ingestionClient.flush();
      ingestionClient.waitFinish();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return System.nanoTime() - start;
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
   * deploy WaterWheel topology in Storm
   */
  public static void main(String... args) {
    Config config;
    if (args.length > 0) {
      try {
        FileInputStream fileInputStream = new FileInputStream(args[0]);
        config = new cn.edu.thu.common.Config(fileInputStream);
      } catch (Exception e) {
        e.printStackTrace();
        config = new cn.edu.thu.common.Config();
      }
    } else {
      config = new cn.edu.thu.common.Config();
    }

    TopologyConfig topologyConfig = new TopologyConfig();

    topologyConfig.INSERTION_SERVER_PER_NODE = 4;

    if (config.LOCAL) {
      topologyConfig.dataChunkDir = "./target/tmp";
      topologyConfig.metadataDir = "./target/tmp";
      topologyConfig.HDFSFlag = false;
    } else {
      topologyConfig.HDFSFlag = true;
      topologyConfig.HDFS_HOST = config.HDFS_IP;
      topologyConfig.dataChunkDir = config.HDFS_IP + "waterwheel_data";
      topologyConfig.metadataDir = config.HDFS_IP + "waterwheel_meta";
    }

    if(config.LOCAL) {
      // 512K
      topologyConfig.CHUNK_SIZE = 512 * 1024;
    } else {
      // 16M
      topologyConfig.CHUNK_SIZE = 16 * 1024 * 1024;
    }

    topologyConfig.previousTime = Integer.MAX_VALUE;
    logger.info("dataChunkDir is set to : {}", topologyConfig.dataChunkDir);

    DataSchema schema = getSchema(config);

    final long minIndex = 0L;
    final long maxIndex = Long.MAX_VALUE;

    AvailableSocketPool socketPool = new AvailableSocketPool();
    int ingestionPort = socketPool.getAvailablePort();
    int queryPort = socketPool.getAvailablePort();

    TopologyGenerator<Long> topologyGenerator = new TopologyGenerator<>();

    InputStreamReceiverBolt inputStreamReceiverBolt = new InputStreamReceiverBoltServer(schema,
        ingestionPort, topologyConfig);
    QueryCoordinatorBolt<Long> coordinator = new QueryCoordinatorWithQueryReceiverServerBolt<>(
        minIndex, maxIndex, queryPort,
        topologyConfig, schema);

    StormTopology topology = topologyGenerator
        .generateIndexingTopology(schema, minIndex, maxIndex, false, inputStreamReceiverBolt,
            coordinator, topologyConfig);

    org.apache.storm.Config conf = new org.apache.storm.Config();
    conf.setDebug(false);
    conf.setNumWorkers(1);

    if (config.LOCAL) {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("qiao_waterwheel_topology", conf, topology);
    } else {
      try {
        StormSubmitter.submitTopology("qiao_waterwheel_topology", conf, topology);
        logger.info("Topology is successfully submitted to the cluster!");
        logger.info(topologyConfig.getCriticalSettings());
      } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
        e.printStackTrace();
      }
    }

    logger.info("topology submitted, ingestion port: {}, query port: {}", ingestionPort, queryPort);
  }

  static DataSchema getSchema(Config config) {
    DataSchema schema = new DataSchema();

    schema.addLongField(Config.TAG_NAME);
    schema.setPrimaryIndexField(Config.TAG_NAME);

    schema.addLongField(Config.TIME_NAME);
    schema.setTemporalField(Config.TIME_NAME);

    for (String field : config.FIELDS) {
      schema.addDoubleField(field);
    }
    return schema;
  }
}
