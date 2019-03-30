package backup;

import cn.edu.thu.common.Utils;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtendRedd {

  private static Logger logger = LoggerFactory.getLogger(ExtendRedd.class);

  public static void main(String[] args) {

    String src = "/Users/qiaojialin/Desktop/testfile";
    int threadNum = 10;

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

}

class Worker implements Runnable {

  private static Logger logger = LoggerFactory.getLogger(Worker.class);
  private List<String> files;

  public Worker(List<String> files) {
    this.files = files;
  }

  @Override
  public void run() {

    try {

      // extend each file
      for (String file : files) {

        logger.info("start to extend file: {}", file);

        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

        List<String> values = new ArrayList<>();

        long time = 0;

        String str;

        while ((str = bufferedReader.readLine()) != null) {
          String[] items = str.split(" ");
          time = Long.parseLong(items[0]);
          values.add(items[1]);
        }
        bufferedReader.close();

        FileWriter writer = new FileWriter(file, true);

        for (int i = 0; i < 1000; i++) {
          for (String value : values) {
            writer.write("\n" + (time++) + " " + value);
          }
        }
        writer.close();
      }
      logger.info("I'm done.");
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
