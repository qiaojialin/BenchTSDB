package cn.edu.thu.common;

public class Statistics {

    public long fileNum = 0;
    public long lineNum = 0;
    public long pointNum = 0;
    public long timeCost = 0;

    public Statistics(){

    }

    /**
     * @return points / s
     */
    public float speed() {
        return (float) pointNum / (float) timeCost * 1000;
    }
}
