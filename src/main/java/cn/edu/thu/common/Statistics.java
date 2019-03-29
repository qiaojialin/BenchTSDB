package cn.edu.thu.common;

import java.util.concurrent.atomic.AtomicLong;

public class Statistics {

    public AtomicLong fileNum = new AtomicLong(0);
    public AtomicLong recordNum = new AtomicLong(0);
    public AtomicLong pointNum = new AtomicLong(0);
    public AtomicLong timeCost = new AtomicLong(0);

    public Statistics(){

    }

    /**
     * @return points / s
     */
    public float speed() {
        return ((float) pointNum.get()) / ((float) timeCost.get()) * 1000;
    }
}
