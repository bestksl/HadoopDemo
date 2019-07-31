package com.bestksl.newHdfsDemo.dataCollect;

import com.bestksl.hdfs.dataCollect.Constants;
import com.bestksl.hdfs.dataCollect.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-07-30 16:58
 */
public class TimerTaskImpl extends TimerTask {
    @Override
    public void run() {
        /*
        读取配置文件信息
         */
        Properties properties;
        try {
            properties = Utils.getPropsLazy();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        //获取采集时间
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-DD-HH");
        String day = sdf.format(new Date());

        //准备日志
        Logger logger = Logger.getLogger("logRollingFile");

        //将原文件列出
        File logFoder = new File(properties.getProperty(Constants.LOG_SOURCE_DIR));
        String logPrefix = properties.getProperty(Constants.LOG_LEGAL_PREFIX);
        File[] files = logFoder.listFiles(((dir, name) -> name.startsWith(logPrefix)));

        //打日志(检测到文件)
        logger.info("检测到如下文件需要转移: " + Arrays.toString(files));

        //将原日志文件转移到待上传目录
        File toUploadDir = new File(properties.getProperty(Constants.LOG_TOUPLOAD_DIR));
        if (files == null || files.length == 0) {
            return;
        }
        for (File file : files) {
            logger.info("正在转移文件: " + file.getName());
            try {
                FileUtils.moveFileToDirectory(file, toUploadDir, true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("所有文件转移成功!");

        //构造一个HDFS客户端对象
        FileSystem fs;
        try {
            fs = Utils.getConnection();

            //检查 hdfs备份目录 和 backup目录 是否存在 or 创建
            Path hdfsBaseDir = new Path(properties.getProperty(Constants.HDFS_DEST_BASE_DIR) + day + "/");
            if (!fs.exists(hdfsBaseDir)) {
                fs.mkdirs(hdfsBaseDir);
            }
            File backupDir = new File(properties.getProperty(Constants.LOG_BACK_BASE_DIR));
            if (!backupDir.exists()) {
                backupDir.mkdirs();
            }

            //开始转移
            File[] toUploadFiles = toUploadDir.listFiles();
            for (File file : toUploadFiles != null ? toUploadFiles : new File[0]) {
                //传输文件到HDFS+改名
                Path des = new Path(hdfsBaseDir + properties.getProperty(Constants.HDFS_FILE_PREFIX) + UUID.randomUUID() + properties.getProperty(Constants.HDFS_FILE_SUFFIX));
                fs.moveFromLocalFile(new Path(file.getAbsolutePath()), des);

                FileUtils.moveFileToDirectory(file, backupDir, false);
                //打日志(上传到hdfs)
                logger.info(file.getName() + "从 " + file.getAbsolutePath() + "已上传的目录: " + des);
                //打日志
                logger.info(file.getName() + "从 " + file.getAbsolutePath() + "已备份到目录: " + backupDir);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
