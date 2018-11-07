package com.bestksl.hdfs.WordCount;

import com.bestksl.hdfs.dataCollect.Utils;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class HdfsWordCount {


    private static FileSystem fs;
    private static RemoteIterator<LocatedFileStatus> iter;
    private static LocatedFileStatus ls;
    private static BufferedReader bfr;
    private static Properties props;
    private static Class<?> mapper_class;
    private static Context context;
    private static HashMap<Object, Object> map;
    private static Set<Map.Entry<Object, Object>> entrySet;
    private static FSDataOutputStream outputStream;

    public static void main(String[] args) throws Exception {
        props = new Properties();
        props.load(HdfsWordCount.class.getClassLoader().getResourceAsStream("job.properties"));

        mapper_class = Class.forName(props.getProperty("MAPPER_CLASS"));
        Mapper mapper = (Mapper) mapper_class.newInstance();
        context = new Context();
        fs = Utils.getConnection();
        iter = fs.listFiles(new Path("/wordcount/input/"), false);
        while (iter.hasNext()) {
            ls = iter.next();
            FSDataInputStream in = fs.open(ls.getPath());
            bfr = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while ((line = bfr.readLine()) != null) {
                mapper.map(line, context);
            }
            in.close();
        }
        //输出数据

        map = context.getMap();
        Path outPath = new Path("/wordcount/output/");
        if (!fs.exists(outPath)) {
            fs.mkdirs(outPath);
        }
        if (fs.exists(new Path("/wordcount/output/res.dat"))) {
            throw new RuntimeException("吃饭喽");
        }

        outputStream = fs.create(new Path("/wordcount/output/res.dat"));

        entrySet = map.entrySet();
        for (Map.Entry<Object, Object> entry : entrySet) {
            outputStream.write((entry.getKey().toString() + "\t" + entry.getValue() + "\n").getBytes());
        }
        outputStream.close();
        fs.close();
        System.out.println("数据统计完成");
    }
}
