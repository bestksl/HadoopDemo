package HdfsDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

public class HdfsClientDemo {
    FileSystem fs = null;

    public static void main(String[] args) throws Exception {

        new HdfsClientDemo().init();


    }

    //初始化连接
    @Before
    public void init() throws Exception {
        // new configuration() 会从core-default.xml hdfs-default.xml core-site.xml hdfs-site.xml
        Configuration conf = new Configuration();
        //指定该客户端hdfs的备份数量
        conf.set("des.replication", "2");
        //指定该客户端在hdfs的块大小
        conf.set("dfs.blocksize", "64m");
        fs = FileSystem.get(new URI("hdfs://HDP-01:9000/"), conf, "root");
    }

    //下载
    @Test
    public void testGet() throws Exception {
        //fs.copyFromLocalFile(new Path("E:/不要让他们失望/22-软件包/akka_2.11-2.4.16.zip"), new Path("/dir1/"));
        fs.copyToLocalFile(new Path("/dir1/akka_2.11-2.4.16.zip"), new Path("f:/"));
    }

    //移动文件
    @Test
    public void testRename() throws Exception {
        fs.rename(new Path("/111.zip"), new Path("/dir1/akka_2.11-2.4.16.zip"));
    }

    //创建目录
    @Test
    public void testMkdir() throws Exception {
        fs.mkdirs(new Path("/111/1222/撒大苏打"));
    }

    //删除
    @Test
    public void testRemove() throws Exception {
        fs.delete(new Path("/a/b"), true);
    }


    @Test
    //查看
    public void testLs() throws IOException {
        //RemoteIterator iter = fs.listFiles(new Path("/"), true);
        var iter = fs.listFiles(new Path("/"), true);
        while (iter.hasNext()) {
            //LocatedFileStatus status=iter.next();
            var status = iter.next();
            System.out.println("blkSize: " + status.getBlockSize());
            System.out.println(status.isDirectory() ? "dir" : "file");
            System.out.println("fileSize" + status.getLen());
            System.out.println("repNum: " + status.getReplication());
            System.out.println("location: " + Arrays.toString(status.getBlockLocations()));
        }
    }

    @Test
    public void testReadData() throws IOException {
        FSDataInputStream s = fs.open(new Path("/test666.txt"));
        byte[] buf = new byte[10];
        while (s.read(buf) != -1) {
            System.out.println(new String(buf));
        }
    }

    @Test
    public void testWriteData() throws IOException {
        FSDataOutputStream fsout = fs.create(new Path("/new.jpg"), true);
        FileInputStream fsin = new FileInputStream("d:/鬼刀风铃日落4K高清壁纸_彼岸图网.jpg");

        byte[] buffer = new byte[1024];
        var len = 0;
        while ((len = fsin.read(buffer)) != -1) {
            fsout.write(buffer, 0, len);
        }
        fsout.flush();
        fsin.close();
        fsout.close();
    }

    @After
    public void finalMathod() throws Exception {
        fs.close();
    }
}