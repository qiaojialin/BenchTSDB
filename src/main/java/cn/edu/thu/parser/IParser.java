package cn.edu.thu.parser;

import cn.edu.thu.common.Record;

import java.util.List;

public interface IParser {

    List<Record> parse(String fileName);

}
