package backup;

import java.io.*;
import java.util.*;

public class AzureReadWrite {

    public static void main(String[] args) throws IOException {
        String vmFile = "/Users/qiaojialin/Desktop/azure_data/vmtable.csv";
        String filePath = "/Users/qiaojialin/Desktop/azure_data/vm_cpu_readings-file-1-of-125.csv";
        String outPath = "/Users/qiaojialin/Desktop/azure_data/haha";

        List<String> files = new ArrayList<>();
        getAllFiles(filePath, files);
        Collections.sort(files);

        Set<String> vmIds = new HashSet<>();

        // get all vmIds
        try (BufferedReader reader = new BufferedReader(new FileReader(vmFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] items = line.split(",");
                vmIds.add(items[0]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<String> ids = new ArrayList<>(vmIds);

        for (int i = 0; i < ids.size(); i++) {

            String id = ids.get(i);
            System.out.println(i + ": " + id);

            String tmpOutFile = outPath + "/" + i + ".txt";
            FileWriter writer = new FileWriter(tmpOutFile, false);

            for (int j = 0; j < files.size(); j++) {
                String file = files.get(j);
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (!line.contains(id))
                            continue;
                        String[] items = line.split(",");

                        // extract avg cpu
                        writer.write(items[0] + " " + items[4]);
                        writer.write("\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            writer.close();

            File file = new File(tmpOutFile);
            if(file.length() < 1) {
                file.delete();
                System.out.println("delete empty file: " + tmpOutFile);
            }
        }

    }

    private static void getAllFiles(String strPath, List<String> files) {
        File f = new File(strPath);
        if (f.isDirectory()) {
            File[] fs = f.listFiles();
            for (File f1 : fs) {
                String fsPath = f1.getAbsolutePath();
                getAllFiles(fsPath, files);
            }
        } else if (f.isFile()) {
            files.add(f.getAbsolutePath());
        }
    }
}
