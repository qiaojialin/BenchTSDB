package cn.edu.thu.manager.waterwheelsample;

import indexingTopology.api.client.IngestionClientBatchMode;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;

import java.io.IOException;

public class GeoLoad {

    public static void main(String... args) {

        DataSchema schema = new DataSchema();
        schema.addLongField("id");
        schema.addDoubleField("lon");
        schema.addDoubleField("lat");
        schema.addDoubleField("alt");
        schema.addLongField("timestamp");
        schema.setTemporalField("timestamp");
        schema.setPrimaryIndexField("id");


        final IngestionClientBatchMode ingestionClient = new IngestionClientBatchMode("localhost", 10000, schema, 1024);
        try {
            ingestionClient.connectWithTimeout(10000);
            System.out.println("successfully connected to waterwheel server");
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (long i = 0; i < 100; i++) {
            for(long j = 0; j < 100000; j++) {
                DataTuple tuple = new DataTuple();
                tuple.add(i);
                tuple.add(3.14d);
                tuple.add(100d);
                tuple.add(200d);
                tuple.add(System.currentTimeMillis() + j);
                try {
                    ingestionClient.appendInBatch(tuple);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        try {
            System.out.println("start flush");
            ingestionClient.flush();
            System.out.println("end flush");
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("wait to finish");
        // wait for the tuples to be appended.
        ingestionClient.waitFinish();
        System.out.println("finished!");
        try {
            ingestionClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
