package com.bestksl.hdfs.dataCollect;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class CollectTask extends TimerTask {
    @Override
    public void run() {
        //获取properties
        Properties prop = null;
        try {
            prop = Utils.getPropsLazy();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //获取采集时间
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
        String day = sdf.format(new Date());
        //创建log对象
        Logger logger = Logger.getLogger("logRollingFile");

        //将原文件列出
        File fileSrc = new File(prop.getProperty(Constants.LOG_SOURCE_DIR));
        String temp = prop.getProperty(Constants.LOG_LEGAL_PREFIX);
        File[] files = fileSrc.listFiles((dir, name) -> {
            return name.startsWith(temp);
        });

        //打日志(检测到文件)
        logger.info("探测到如下文件需要转移: " + Arrays.toString(files));

        //将原日志文件转移到待上传目录
        File toUpLoadDir = new File(prop.getProperty(Constants.LOG_TOUPLOAD_DIR));
        for (File file : files) {
            try {
                FileUtils.moveFileToDirectory(file, toUpLoadDir, true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        File[] files1 = toUpLoadDir.listFiles();
        //打日志(转移成功)
        logger.info("已转移文件到" + toUpLoadDir.getAbsolutePath() + " 等待上传: " + Arrays.toString(files1));

        //构造一个HDFS客户端对象

        try {
            FileSystem fs = Utils.getConnection();
            File[] toUpLoadFiles = toUpLoadDir.listFiles();
            //检查HDFS日期目录 和 backup目录 是否存在  else 创建

            if (!fs.exists(new Path(prop.getProperty(Constants.HDFS_DEST_BASE_DIR) + day))) {
                fs.mkdirs(new Path(prop.getProperty(Constants.HDFS_DEST_BASE_DIR) + day));
            }

            File backUpDir = new File(prop.getProperty(Constants.LOG_BACK_BASE_DIR) + day + "/");
            if (!backUpDir.exists()) {
                backUpDir.mkdirs();
            }

            for (File file : toUpLoadFiles) {
                Path des = new Path(prop.getProperty(Constants.HDFS_DEST_BASE_DIR) + day + "/" + prop.getProperty(Constants.HDFS_FILE_PREFIX) + UUID.randomUUID() + ".log");
                // 传输文件到HDFS + 改名
                fs.copyFromLocalFile(new Path(file.getAbsolutePath()), des);
                //打日志(上传)
                logger.info(file.getName() + "从 " + file.getAbsolutePath() + " 已上传到目录: " + des);
                FileUtils.moveFileToDirectory(file, backUpDir, false);
                //打日志(备份)
                logger.info(file.getName() + "从 " + file.getAbsolutePath() + " 已备份到目录: " + backUpDir);

            }


        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
