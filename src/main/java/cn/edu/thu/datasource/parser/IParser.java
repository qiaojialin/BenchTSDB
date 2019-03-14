package cn.edu.thu.datasource.parser;

import cn.edu.thu.common.Record;

import java.util.List;

public interface IParser {

    /**
     * read file
     */
    List<Record> parse(String fileName);

}
