import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class Launcher {

    public static void main(String[] args) throws IOException {

        HdfsAppendExample example = new HdfsAppendExample();
        String coreSite = "/opt/hadoop_backup/etc/hadoop/core-site.xml";
        String hdfsSite = "/opt/hadoop_backup/etc/hadoop/hdfs-site.xml";
        FileSystem fileSystem = example.configureFileSystem(coreSite, hdfsSite);

        String hdfsFilePath = "hdfs://localhost:54310/UniqueEntry.csv";
        String res = example.appendToFile(fileSystem, "It's never too late" +
                " to start something good.", hdfsFilePath);

        if (res.equalsIgnoreCase( "success")) {
            System.out.println("Successfully appended to file");
            String content = example.readFromHdfs(fileSystem, hdfsFilePath);
            System.out.println(">>>>Content read from file<<<<\n" + content);
        }
        else
            System.out.println("couldn't append to file");

        example.closeFileSystem(fileSystem);
    }
}
