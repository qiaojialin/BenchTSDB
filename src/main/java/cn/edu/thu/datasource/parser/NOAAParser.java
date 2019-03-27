package cn.edu.thu.datasource.parser;

import cn.edu.thu.common.Record;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class NOAAParser implements IParser {

    private DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    @Override
    public List<Record> parse(String fileName) {

        List<Record> records = new ArrayList<>();

        try(BufferedReader reader = new BufferedReader(new FileReader(fileName))) {

            // skip first line, which is the metadata
            reader.readLine();

            String line;

            while ((line = reader.readLine()) != null) {
                try {
                    Record record = convertToRecord(line);
                    records.add(record);
                } catch (ParseException ignored) {
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return records;
    }

    @Override
    public void close() {

    }

    private Record convertToRecord(String line) throws ParseException {

        List<Object> fields = new ArrayList<>();

        String tag = line.substring(0, 6).trim() + "_" + line.substring(7, 12).trim();
        //add 70 years, make sure time > 0
        String yearmoda = line.substring(14, 22).trim();
        Date date = dateFormat.parse(yearmoda);
        long time = date.getTime() + 2209046400000L;

//        System.out.println(time);
//        long time = System.currentTimeMillis();

        fields.add(Double.parseDouble(line.substring(24, 30).trim()));
        fields.add(Double.parseDouble(line.substring(35, 41).trim()));
        fields.add(Double.parseDouble(line.substring(46, 52).trim()));
        fields.add(Double.parseDouble(line.substring(57, 63).trim()));
        fields.add(Double.parseDouble(line.substring(68, 73).trim()));
        fields.add(Double.parseDouble(line.substring(78, 83).trim()));
        fields.add(Double.parseDouble(line.substring(88, 93).trim()));
        fields.add(Double.parseDouble(line.substring(95, 100).trim()));
        fields.add(Double.parseDouble(line.substring(102, 108).trim()));
        fields.add(Double.parseDouble(line.substring(110, 116).trim()));
        fields.add(Double.parseDouble(line.substring(118, 123).trim()));
        fields.add(Double.parseDouble(line.substring(125, 130).trim()));
        fields.add(Double.parseDouble(line.substring(132, 138).trim()));

        return new Record(time, tag, fields);

    }
}
