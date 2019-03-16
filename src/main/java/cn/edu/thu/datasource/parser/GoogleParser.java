package cn.edu.thu.datasource.parser;

import cn.edu.thu.common.Record;

import java.util.Base64;
import java.util.List;

/**
 * @author qiaojialin
 */
@Deprecated public class GoogleParser implements IParser {

    @Override
    public List<Record> parse(String fileName) {
        return null;
    }

    public static void main(String... args) {

        byte[] a = Base64.getDecoder().decode("GKAYWlOFlntxaxFt+CCHj/Og1BgToNx62SMW9WHlf8g=");

    }

}
