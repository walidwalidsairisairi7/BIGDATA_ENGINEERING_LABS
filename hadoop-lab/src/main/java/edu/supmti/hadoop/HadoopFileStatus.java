package edu.supmti.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.IOException;

public class HadoopFileStatus {
    public static void main(String[] args) throws IOException {
        // Check that a file path argument is provided
        if (args.length < 1) {
            System.err.println("Usage: HadoopFileStatus <hdfs-file-or-directory-path>");
            System.exit(1);
        }

        // Read the HDFS path argument
        String filePath = args[0];

        // Initialize Hadoop configuration
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);

        // Check if the path exists
        if (!fs.exists(path)) {
            System.out.println("❌ The file or directory does not exist: " + filePath);
            fs.close();
            return;
        }

        // Get file status
        FileStatus status = fs.getFileStatus(path);

        System.out.println("✅ File Status Information:");
        System.out.println("-----------------------------");
        System.out.println("Path: " + status.getPath());
        System.out.println("Is directory: " + status.isDirectory());
        System.out.println("Length: " + status.getLen());
        System.out.println("Owner: " + status.getOwner());
        System.out.println("Group: " + status.getGroup());
        System.out.println("Permissions: " + status.getPermission());
        System.out.println("Modification Time: " + status.getModificationTime());

        // If the path is a directory, list its contents
        if (status.isDirectory()) {
            System.out.println("\n📂 Directory contents:");
            FileStatus[] files = fs.listStatus(path);
            for (FileStatus file : files) {
                System.out.println(" - " + file.getPath().getName() +
                        " (size: " + file.getLen() + " bytes, owner: " + file.getOwner() + ")");
            }
        }

        // Close FileSystem
        fs.close();
    }
}
