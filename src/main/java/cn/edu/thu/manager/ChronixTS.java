package cn.edu.thu.manager;

import de.qaware.chronix.timeseries.MetricTimeSeries;
import java.time.Instant;

public class ChronixTS {

    public static void main(String... args) {

        MetricTimeSeries.Builder builder = new MetricTimeSeries.Builder("deviceId", "metric");
        builder.attribute("deviceId", "12211");
        builder.point(Instant.now().toEpochMilli(), 517);
        builder.point(Instant.now().toEpochMilli()+1, 518);
        builder.point(Instant.now().toEpochMilli()+2, 519);
        MetricTimeSeries series = builder.build();
        System.out.println(series.toString());

        series.points().forEach((a) -> System.out.println(a.getTimestamp() + " " + a.getValue()));

    }
}
