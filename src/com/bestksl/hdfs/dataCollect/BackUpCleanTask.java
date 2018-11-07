package com.bestksl.hdfs.dataCollect;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;

public class BackUpCleanTask extends TimerTask {

    @Override
    public void run() {
        //探测备份目录
        File baseLogDir = new File("d:/logs/backUp/");
        File[] dirs = baseLogDir.listFiles();
        //判断是否过期
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
        Long now = new Date().getTime();
        Long dateTemp;
        try {
            for (File dir :
                    dirs) {
                dateTemp = sdf.parse(dir.getName()).getTime();
                if (now - dateTemp > 60 * 60 * 1000L) {
                    FileUtils.deleteDirectory(dir);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
