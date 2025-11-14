package edu.supmti.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        // Check if a file path is provided
        if (args.length < 1) {
            System.err.println("Usage: ReadHDFS <hdfs-file-path>");
            System.exit(1);
        }

        String filePath = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);

        // Check if the file exists
        if (!fs.exists(path)) {
            System.out.println("❌ The file does not exist: " + filePath);
            fs.close();
            return;
        }

        // Open the file for reading
        FSDataInputStream inputStream = fs.open(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        System.out.println("📖 Reading file content from HDFS: " + filePath);
        System.out.println("--------------------------------------------------");

        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        // Close all resources
        reader.close();
        inputStream.close();
        fs.close();

        System.out.println("--------------------------------------------------");
        System.out.println("✅ Done reading file from HDFS.");
    }
}
