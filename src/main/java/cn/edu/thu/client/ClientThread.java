package cn.edu.thu.client;

import cn.edu.thu.common.Record;
import cn.edu.thu.common.Config;
import cn.edu.thu.manager.IDataBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ClientThread implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(ClientThread.class);
    private IDataBase database;
    private Config config;
    private int threadId;
    DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    public ClientThread(IDataBase database, Config config, int threadId) {
        this.database = database;
        this.config = config;
        this.threadId = threadId;
    }

    @Override
    public void run() {

        //read file

        long totalTime = 1;
        long totalPoints = 0;

        try {
            File dirFile = new File(config.DATA_DIR);
            if (!dirFile.exists()) {
                logger.error(config.DATA_DIR + " do not exit");
                return;
            }


            String[] files = dirFile.list();
            if (files == null) {
                logger.error("{} has no file", config.DATA_DIR);
                System.exit(1);
            }

            Arrays.sort(files);

            long lineNum = 0;
            int fileNum = 0;

            // datafile name begins from 0

            logger.info("total file num: {}", files.length);

            for (int i = 0; i < files.length; i++) {

                if(i< config.BEGINE_FILE || i > config.END_FILE) {
                    continue;
                }

                String fileName = files[i];

                String filePath = config.DATA_DIR + "/" + fileName;

                // only read the file that can be exacted division by threadid
                if (i % config.THREAD_NUM != threadId) {
                    continue;
                }

                fileNum++;

                BufferedReader reader = new BufferedReader(new FileReader(filePath));

                // skip first line, which is the metadata
                reader.readLine();

                String line;
                List<Record> records = new ArrayList<>();

                while ((line = reader.readLine()) != null) {
                    lineNum++;
                    try {
                        Record record = convertToRecord(line);
                        records.add(record);
                    } catch (ParseException ignored) {
                    }
                }

                reader.close();

                // write all data in this file to database
                long timecost = database.insertBatch(records);

                totalTime += timecost;

                logger.debug("processed the {}-th file in : {} ms", i, timecost);
            }

            totalTime += database.close();

            logger.info("total produce {} files and {} lines", fileNum, lineNum);

            // points per second
            totalPoints = lineNum * config.FIELDS.length;
            long speed = totalPoints / totalTime * 1000;

            logger.info("points:{},time:{},ms,speed:{},points/s", totalPoints, totalTime, speed);

        } catch (Exception e) {
            logger.error(e.toString());
        }

    }

    private Record convertToRecord(String line) throws ParseException {

        List<Object> fields = new ArrayList<>();

        String tag1 = line.substring(0, 6).trim();
        String tag2 = line.substring(7, 12).trim();

        //add 70 years, make sure time > 0
        String yearmoda = line.substring(14, 22).trim();
        Date date = dateFormat.parse(yearmoda);

//        Date date2 = dateFormat.parse("20400102");
//        long t = date2.getTime();
////        Calendar rightNow = Calendar.getInstance();
////        rightNow.setTime(date);
////        rightNow.add(Calendar.YEAR, 70);
////        date = rightNow.getTime();
        long time = date.getTime() + 2209046400000L;

        fields.add(Float.parseFloat(line.substring(24, 30).trim()));
        fields.add(Float.parseFloat(line.substring(35, 41).trim()));
        fields.add(Float.parseFloat(line.substring(46, 52).trim()));
        fields.add(Float.parseFloat(line.substring(57, 63).trim()));
        fields.add(Float.parseFloat(line.substring(68, 73).trim()));
        fields.add(Float.parseFloat(line.substring(78, 83).trim()));
        fields.add(Float.parseFloat(line.substring(88, 93).trim()));
        fields.add(Float.parseFloat(line.substring(95, 100).trim()));
        fields.add(Float.parseFloat(line.substring(102, 108).trim()));
        fields.add(Float.parseFloat(line.substring(110, 116).trim()));
        fields.add(Float.parseFloat(line.substring(118, 123).trim()));
        fields.add(Float.parseFloat(line.substring(125, 130).trim()));
        fields.add(Float.parseFloat(line.substring(132, 138).trim()));

        return new Record(time, tag1, tag2, fields);

    }

}
