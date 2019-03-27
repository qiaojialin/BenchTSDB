package cn.edu.thu.datasource.parser;

import cn.edu.thu.common.Record;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class NOAAWriter implements IParser {

    private FileWriter writer;
    private DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");


    public NOAAWriter() {
        try {
            writer = new FileWriter("/Users/qiaojialin/Desktop/noaa_gsod.VISIB", false);
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


    private String convertToRecord(String line) throws ParseException {


        String tag = line.substring(0, 6).trim() + "_" + line.substring(7, 12).trim();
        //add 70 years, make sure time > 0
        String yearmoda = line.substring(14, 22).trim();
        Date date = dateFormat.parse(yearmoda);
        long time = date.getTime();

//        System.out.println(time);
//        long time = System.currentTimeMillis();

//        fields.add(Double.parseDouble(line.substring(24, 30).trim()));
//        fields.add(Double.parseDouble(line.substring(35, 41).trim()));
//        fields.add(Double.parseDouble(line.substring(46, 52).trim()));
//        fields.add(Double.parseDouble(line.substring(57, 63).trim()));
//        fields.add(Double.parseDouble(line.substring(68, 73).trim()));
//        fields.add(Double.parseDouble(line.substring(78, 83).trim()));
//        fields.add(Double.parseDouble(line.substring(88, 93).trim()));
//        fields.add(Double.parseDouble(line.substring(95, 100).trim()));
//        fields.add(Double.parseDouble(line.substring(102, 108).trim()));
//        fields.add(Double.parseDouble(line.substring(110, 116).trim()));
//        fields.add(Double.parseDouble(line.substring(118, 123).trim()));
//        fields.add(Double.parseDouble(line.substring(125, 130).trim()));
//        fields.add(Double.parseDouble(line.substring(132, 138).trim()));

        return time + " " + Double.parseDouble(line.substring(68, 73).trim());

    }

}
