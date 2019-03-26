package cn.edu.thu.datasource.parser;

import cn.edu.thu.common.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class GeoWriter implements IParser {

    private static Logger logger = LoggerFactory.getLogger(GeolifeParser.class);
    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh:mm:ss");
    private String tag = "";
    private static String fileName = "";

    private FileWriter writer;

    public GeoWriter() {
        try {
            writer = new FileWriter("/Users/qiaojialin/Desktop/geolife.altitude", false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Record> parse(String fileName) {
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {

            // skip 6 lines, which is useless
            for (int i = 0; i < 6; i++) {
                reader.readLine();
            }

            String line;

            while ((line = reader.readLine()) != null) {
                String newLine = convertToRecord(line);
                writer.write(newLine);
                writer.write("\n");
            }

        } catch (Exception e) {
            logger.warn("parse {} failed, because {}", fileName, e.getMessage());
            e.printStackTrace();
        }

        return new ArrayList<>();
    }

    @Override
    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String convertToRecord(String line) {
        try {
            String[] items = line.split(",");
            Date date = dateFormat.parse(items[5] + "-" + items[6]);
            long time = date.getTime();
            return time + " " +  Float.parseFloat(items[3]);
        } catch (Exception ignore) {
            logger.warn("can not parse: {}, error message: {}, File name: {}", line, ignore.getMessage(),
                    fileName);
        }
        return null;
    }

}
