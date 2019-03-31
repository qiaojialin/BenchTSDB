package backup;

import cn.edu.thu.common.Utils;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtendTdrive {

  private static Logger logger = LoggerFactory.getLogger(ExtendTdrive.class);

  public static void main(String[] args) {

    String src = "data/tdrive/1.txt";
    int threadNum = 1;

    if (args.length != 0) {
      src = args[0];
      threadNum = Integer.parseInt(args[1]);
    }

    List<String> files = new ArrayList<>();
    Utils.getAllFiles(src, files);
    Collections.sort(files);

    List<List<String>> thread_files = new ArrayList<>();
    for (int i = 0; i < threadNum; i++) {
      thread_files.add(new ArrayList<>());
    }

    for (int i = 0; i < files.size(); i++) {
      String filePath = files.get(i);
      int thread = i % threadNum;
      thread_files.get(thread).add(filePath);
    }

    ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
    for (int threadId = 0; threadId < threadNum; threadId++) {
      executorService.submit(new Worker(thread_files.get(threadId)));
    }

    executorService.shutdown();

    // wait for all threads done
    boolean allDown = false;
    while (!allDown) {
      if (executorService.isTerminated()) {
        allDown = true;
      }
    }

  }

  static class Worker implements Runnable {

    private Logger logger = LoggerFactory.getLogger(Worker.class);
    private List<String> files;
    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public Worker(List<String> files) {
      this.files = files;
    }

    @Override
    public void run() {

      try {

        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));

        // extend each file
        for (String file : files) {

          logger.info("start to extend file: {}", file);

          BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

          String carId = "";
          List<String> values = new ArrayList<>();
          List<Long> times = new ArrayList<>();

          long startTime = 0;
          long endTime = 0;

          String str;

          boolean first = true;

          while ((str = bufferedReader.readLine()) != null) {
            String[] items = str.split(",");
              Date date = dateFormat.parse(items[1]);
              String a = dateFormat.format(date);

              if(first) {
                startTime = date.getTime();
                carId = items[0];
                first = false;
              }
              endTime = date.getTime();
              times.add(date.getTime());
              values.add(items[2] + "," + items[3]);
          }

          bufferedReader.close();

          FileWriter writer = new FileWriter(file, true);

          long period = endTime - startTime;

          for (int i = 0; i < 2; i++) {

            for(int j = 0; j < times.size(); j++) {
              long time = times.get(j) + period * (i+1);
              Date date = new Date(time);
              String dataStr = dateFormat.format(date);
              System.out.println(dataStr);
//              writer.write("\n"  + carId + "," + dataStr + "," + values.get(j));
            }

          }
          writer.close();
        }
        logger.info("I'm done.");
      } catch (Exception e) {
        e.printStackTrace();
      }

    }
  }
}



