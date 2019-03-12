package cn.edu.thu.manager.geolife;

import indexingTopology.api.client.IngestionClientBatchMode;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.util.FrequencyRestrictor;
import indexingTopology.util.taxi.Car;
import indexingTopology.util.taxi.TrajectoryGenerator;
import indexingTopology.util.taxi.TrajectoryMovingGenerator;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.metric.internal.RateTracker;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Random;


public class GeoLifeTopologyIngest {

    /**
     * general configuration
     */
    @Option(name = "--help", aliases = {"-h"}, usage = "help")
    private boolean Help = false;

    @Option(name = "--mode", aliases = {"-m"}, usage = "submit|ingest|query")
    private String Mode = "Not Given";

    /**
     * topology configuration
     */
    @Option(name = "--topology-name", aliases = "-t", usage = "topology name")
    private String TopologyName = "T0";

    @Option(name = "--config-file", aliases = {"-f"}, usage = "conf.yaml to override default configs")
    private String confFile = "conf/conf.yaml";

    @Option(name = "--node", aliases = {"-n"}, usage = "number of nodes used in the topology")
    private int NumberOfNodes = 1;

    @Option(name = "--local", usage = "run the topology in local cluster")
    private boolean LocalMode = false;

    /**
     * ingest api configuration
     */
    @Option(name = "--ingest-server-ip", usage = "the ingestion server ip")
    private String IngestServerIp = "localhost";

    @Option(name = "--ingest-rate-limit", aliases = {"-r"}, usage = "max ingestion rate")
    private int MaxIngestRate = Integer.MAX_VALUE;

    /**
     * query api configuration
     */
    @Option(name = "--query-server-ip", usage = "the query server ip")
    private String QueryServerIp = "localhost";

    @Option(name = "--selectivity", usage = "the selectivity on the key domain")
    private double Selectivity = 1;

    @Option(name = "--temporal", usage = "recent time in seconds of interest")
    private int RecentSecondsOfInterest = 5;

    @Option(name = "--queries", usage = "number of queries to perform")
    private int NumberOfQueries = Integer.MAX_VALUE;


    static final double x1 = 40.012928;
    static final double x2 = 40.023983;
    static final double y1 = 116.292677;
    static final double y2 = 116.614865;
    static final int partitions = 128;


    public void executeIngestion() {

        DataSchema rawSchema = getRawDataSchema();
        TrajectoryGenerator generator = new TrajectoryMovingGenerator(x1, x2, y1, y2, 100000, 45.0);
        IngestionClientBatchMode clientBatchMode = new IngestionClientBatchMode(IngestServerIp, 10000,
                rawSchema, 1024);

        RateTracker rateTracker = new RateTracker(1000,2);
        FrequencyRestrictor restrictor = new FrequencyRestrictor(MaxIngestRate, 5);

        Thread ingestionThread = new Thread(()->{
            Random random = new Random();

            try {
                clientBatchMode.connectWithTimeout(10000);
            } catch (IOException e) {
                e.printStackTrace();
            }

            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

            while(true) {
                Car car = generator.generate();
                DataTuple tuple = new DataTuple();
                tuple.add(Integer.toString((int)car.id));
                tuple.add("" + (char)((int)'A'+Math.random()*((int)'Z'-(int)'A'+1))
                        + (char)((int)'A'+Math.random()*((int)'Z'-(int)'A'+1))
                        + Integer.toString(Math.abs(random.nextInt()) + 1000000).substring(0, 5));
                tuple.add(car.x);
                tuple.add(car.y);
                tuple.add((int)(Math.random() * 3));
                tuple.add(Math.random() * 70.0);
                tuple.add(Integer.toString((int)(Math.random() * 15)));
                Calendar cal = Calendar.getInstance();
                tuple.add(dateFormat.format(cal.getTime()));
                try {
                    restrictor.getPermission();
                    clientBatchMode.appendInBatch(tuple);
                    rateTracker.notify(1);
                    if(Thread.interrupted()) {
                        break;
                    }
                } catch (IOException e) {
//                    if (clientBatchMode.isClosed()) {
                    try {
                        System.out.println("try to reconnect....");
                        clientBatchMode.connectWithTimeout(10000);
                        System.out.println("connected.");
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
//                    }
                    e.printStackTrace();
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                        Thread.currentThread().interrupt();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

//                try {
////                    Thread.sleep(1);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }

            }
        });
        ingestionThread.start();

        new Thread(()->{
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
                DateFormat dateFormat = new SimpleDateFormat("MM-dd HH:mm:ss");
                Calendar cal = Calendar.getInstance();
                System.out.println("[" + dateFormat.format(cal.getTime()) + "]: " + rateTracker.reportRate() + " tuples/s");
            }
        }).start();
    }


    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        args = new String[]{"-m", "ingest", "-r", "10000", "--ingest-server-ip", "localhost"};


        for(String arg: args) {
            System.out.println("arg:" + arg);
        }

        GeoLifeTopologyIngest geoLifeTopologyIngest = new GeoLifeTopologyIngest();

        CmdLineParser parser = new CmdLineParser(geoLifeTopologyIngest);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            e.printStackTrace();
            parser.printUsage(System.out);
        }

        if (geoLifeTopologyIngest.Help) {
            parser.printUsage(System.out);
            return;
        }

        switch (geoLifeTopologyIngest.Mode) {
            case "ingest": geoLifeTopologyIngest.executeIngestion(); break;
            default: System.out.println("Invalid command!");
        }
    }


    static private DataSchema getRawDataSchema() {
        DataSchema rawSchema = new DataSchema();
        rawSchema.addVarcharField("id", 32);
        rawSchema.addVarcharField("veh_no", 10);
        rawSchema.addDoubleField("lon");
        rawSchema.addDoubleField("lat");
        rawSchema.addIntField("car_status");
        rawSchema.addDoubleField("speed");
        rawSchema.addVarcharField("position_type", 10);
        rawSchema.addVarcharField("update_time", 32);
        return rawSchema;
    }

    static private DataSchema getDataSchema() {
        DataSchema schema = new DataSchema();
        schema.addVarcharField("id", 32);
        schema.addVarcharField("veh_no", 10);
        schema.addDoubleField("lon");
        schema.addDoubleField("lat");
        schema.addIntField("car_status");
        schema.addDoubleField("speed");
        schema.addVarcharField("position_type", 10);
        schema.addVarcharField("update_time", 32);
        schema.addIntField("zcode");
        schema.addLongField("timestamp");
        schema.setTemporalField("timestamp");
        schema.setPrimaryIndexField("zcode");
        return schema;
    }

}
