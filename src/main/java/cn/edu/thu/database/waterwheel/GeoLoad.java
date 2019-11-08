//package cn.edu.thu.database.waterwheel;
//
//import indexingTopology.api.client.IngestionClientBatchMode;
//import indexingTopology.common.data.DataSchema;
//import indexingTopology.common.data.DataTuple;
//
//import java.io.IOException;
//
//public class GeoLoad {
//
//    public static void main(String... args) {
//
//        DataSchema schema = new DataSchema();
//
//        schema.addLongField("deviceId");
//        schema.setPrimaryIndexField("deviceId");
//
//        schema.addLongField("timestamp");
//        schema.setTemporalField("timestamp");
//
//        schema.addDoubleField("Latitude");
//        schema.addDoubleField("Longitude");
//        schema.addDoubleField("Zero");
//        schema.addDoubleField("Altitude");
//
//
//        IngestionClientBatchMode ingestionClient = new IngestionClientBatchMode("127.0.0.1", 10000, schema, 1024);
//        try {
//            ingestionClient.connectWithTimeout(10000);
//            System.out.println("successfully connected to waterwheel server");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        for (long i = 0; i < 100; i++) {
//            for(long j = 1000; j > 10; j--) {
//                DataTuple tuple = new DataTuple();
//                tuple.add(i);
//                tuple.add(System.nanoTime() + j);
//                tuple.add(3.14d);
//                tuple.add(100d);
//                tuple.add(0d);
//                tuple.add(200d);
//                try {
//                    ingestionClient.appendInBatch(tuple);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//
//
//        try {
//            ingestionClient.flush();
//            ingestionClient.waitFinish();
//            ingestionClient.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//    }
//}
