package cn.edu.thu.database.druid;

import com.google.common.collect.ImmutableMap;
import com.metamx.common.logger.Logger;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.config.PropertiesBasedConfig;
import com.metamx.tranquility.config.TranquilityConfig;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.twitter.util.FutureEventListener;
import org.joda.time.DateTime;

import java.io.InputStream;
import java.util.Map;
import scala.runtime.BoxedUnit;

public class InsertExample {

  private static final Logger log = new Logger(InsertExample.class);

  public static void main(String[] args) {
    // Read config from "example.json" on the classpath.
    final InputStream configStream = InsertExample.class.getClassLoader()
        .getResourceAsStream("example.json");
    final TranquilityConfig<PropertiesBasedConfig> config = TranquilityConfig.read(configStream);
    final DataSourceConfig<PropertiesBasedConfig> wikipediaConfig = config
        .getDataSource("wikipedia");
    final Tranquilizer<Map<String, Object>> sender = DruidBeams.fromConfig(wikipediaConfig)
        .buildTranquilizer(wikipediaConfig.tranquilizerBuilder());

    sender.start();

    try {
      // Send 100 objects

      for (int i = 0; i < 100; i++) {
        // Build a sample event to send; make sure we use a current date
        final Map<String, Object> obj = ImmutableMap.<String, Object>of(
            "timestamp", new DateTime().toString(),
            "page", "foo",
            "added", i
        );

//        System.out.println(obj);

        // Asynchronously send event to Druid:
        sender.send(obj).addEventListener(
            new FutureEventListener<BoxedUnit>()
            {
              @Override
              public void onSuccess(BoxedUnit value)
              {
                log.info("Sent message: %s", obj);
              }

              @Override
              public void onFailure(Throwable e)
              {
                if (e instanceof MessageDroppedException) {
                  log.warn(e, "Dropped message: %s", obj);
                } else {
                  log.error(e, "Failed to send message: %s", obj);
                }
              }
            }
        );
      }
    } finally {
      sender.flush();
      sender.stop();
    }
  }
}