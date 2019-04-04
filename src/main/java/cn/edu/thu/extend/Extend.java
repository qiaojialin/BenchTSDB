package cn.edu.thu.extend;

import cn.edu.thu.common.Utils;
import cn.edu.thu.extend.horizon.HExtendRedd;
import cn.edu.thu.extend.horizon.HExtendTdrive;
import cn.edu.thu.extend.vertical.VExtendTdrive;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Extend {

  static String dataset = "";
  static String inputDir = "data/tdrive/1.txt";
  static String outputDir = "";
  static int copyNum = 2;
  static String choice = "";
  static int threadNum = 5;

  /**
   * 在服务器上扩展时，需要的参数为： 1、扩展数据集名称 2、数据集目录 3、扩展数据集输出目录 4、扩展的倍数 5、横向扩展还是纵向扩展
   *
   * @param args dataSetName, inputDir, outputDir, copyNum, choice
   */
  public static void main(String[] args) throws Exception {
    if (!parseInput(args)) {
      return;
    }

    List<String> files = new ArrayList<>();
    Utils.getAllFiles(inputDir, files);
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
      executorService.submit(constructWork(thread_files, threadId));
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

  static boolean parseInput(String[] args) {
    if (args.length != 5) {
      System.out.println("need to specify dataSetName, inputDir, outputDir, copyNum, extendChoice");
      System.out.println("extendChoice should be h or v");
      return false;
    }
    dataset = args[0].toLowerCase();
    inputDir = args[1];
    outputDir = args[2];
    copyNum = Integer.parseInt(args[3]);
    choice = args[4].toLowerCase();
    if (!outputDir.endsWith("/")) {
      outputDir += "/";
    }
    if (choice.contains("h")) {
      choice = "h";
    } else if (choice.contains("v")) {
      choice = "v";
    } else {
      return false;
    }

    if (dataset.equals("tdrive") || dataset.equals("redd")) {
      File file = new File(outputDir);
      file.mkdirs();
      return true;
    } else {
      System.out.println("only support extend tdrive, redd.");
      return false;
    }
  }

  static Runnable constructWork(List<List<String>> thread_files, int threadId) throws Exception {
    int offset = 0;
    for (int i = 0; i < threadId; i++) {
      offset += thread_files.get(i).size();
    }
    if (choice.equals("h")) {
      if (dataset.equals("tdrive")) {
        return new HExtendTdrive(thread_files.get(threadId), copyNum, outputDir, offset);
      } else if (dataset.equals("redd")) {
        return new HExtendRedd(thread_files.get(threadId), copyNum, outputDir, offset);
      }
    } else if (choice.equals("v")) {
      if (dataset.equals("tdrive")) {
        return new VExtendTdrive(thread_files.get(threadId), copyNum, outputDir, offset);
      } else if (dataset.equals("redd")) {
        throw new Exception("not support vertical extend for redd!");
      }
    }
    return null;
  }

}
