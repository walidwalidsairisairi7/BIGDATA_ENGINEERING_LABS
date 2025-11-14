package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HDFSWrite {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: HDFSWrite <output-path> <message>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(args[0]);

        if (!fs.exists(path)) {
            FSDataOutputStream out = fs.create(path);
            out.writeUTF(args[1]);
            out.close();
            System.out.println("File written to HDFS: " + path);
        } else {
            System.out.println("File already exists: " + path);
        }

        fs.close();
    }
}
