package com.bestksl.hdfs.dataCollect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

public class Utils {

    private static Properties prop = new Properties();
    private static Properties prop1 = null;

    static {
        try {
            prop.load(Utils.class.getClassLoader().getResourceAsStream("collect.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //获取连接
    public static FileSystem getConnection() throws URISyntaxException, IOException, InterruptedException {
        // new configuration() 会从core-default.xml hdfs-default.xml core-site.xml hdfs-site.xml
        Configuration conf = new Configuration();
        prop1 = Utils.getPropsLazy();
        FileSystem fs = FileSystem.get(new URI(prop1.getProperty(Constants.HDFS_URI)), conf, "root");
        return fs;
    }


    //获取配置文件参数(恶汉式单例)
    public static Properties getPropsHangury() {
        return prop;
    }


    //懒汉模式 同步锁 写法
    public static Properties getPropsLazy() throws IOException {
        if (prop1 == null) {
            synchronized (Utils.class) {
                if (prop1 == null) {
                    prop1 = new Properties();
                    prop1.load(Utils.class.getClassLoader().getResourceAsStream("collect.properties"));
                }
            }
        }
        return prop1;
    }
}
