package cn.edu.thu.client;

import cn.edu.thu.Record;
import cn.edu.thu.conf.Config;
import cn.edu.thu.manager.IDBManager;
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
    private IDBManager database;
    private Config config;
    private int threadId;
    DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    public ClientThread(IDBManager database, Config config, int threadId) {
        this.database = database;
        this.config = config;
        this.threadId = threadId;
    }

    @Override
    public void run() {

        //read file

        try {
            File dirFile = new File(config.DATA_DIR);
            if (!dirFile.exists()) {
                logger.error(config.DATA_DIR + " do not exit");
                return;
            }
            long lineNum = 0;

            String[] files = dirFile.list();
            if (files == null) {
                logger.error("{} has no file", config.DATA_DIR);
                System.exit(1);
            }

            Arrays.sort(files);

            int fileNum = 0;

            // datafile name begins from 1
            for (String fileName : files) {

                List<Record> records = new ArrayList<>();

                fileNum++;

                String filePath = config.DATA_DIR + "/" + fileName;

                // only read the file that can be exacted division by threadid
                int intFileName = Integer.parseInt(fileName);
                if (intFileName % config.THREAD_NUM != threadId) {
                    continue;
                }

                BufferedReader reader = new BufferedReader(new FileReader(filePath));

                String line;

                reader.readLine();

                while ((line = reader.readLine()) != null) {

                    try {
                        Record record = convertToRecord(line);
                        records.add(record);
                    } catch (ParseException ignored) {
                    }
                }

                reader.close();

                // write all data in this file to database
                database.process(records);

                logger.info("{} is fully read", fileName);
            }

            logger.info("total produce {} files and {} lines", fileNum, lineNum);

        } catch (Exception e) {
            logger.error(e.toString());
        }

    }

    private Record convertToRecord(String line) throws ParseException {

        List<Object> fields = new ArrayList<>();

        String deviceID = line.substring(0, 6).trim();

        fields.add(Integer.parseInt(line.substring(7, 12).trim()));

        //add 70 years, make sure time > 0
        String yearmoda = line.substring(14, 22).trim();
        Date date = dateFormat.parse(yearmoda);
        Calendar rightNow = Calendar.getInstance();
        rightNow.setTime(date);
        rightNow.add(Calendar.YEAR, 70);
        date = rightNow.getTime();
        long time = date.getTime();

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
        fields.add(Integer.parseInt(line.substring(132, 138).trim()));

        return new Record(time, deviceID, fields);

    }

}
