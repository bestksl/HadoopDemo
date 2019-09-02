package com.bestksl.newHdfsDemo.dataCollect;

import com.bestksl.hdfs.dataCollect.CollectTask;

import java.util.Timer;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-07-30 16:54
 */
public class DataCollectMain {

    public static void main(String[] args) {
        Timer timer = new Timer();

        try {
            timer.schedule(new TimerTaskImpl(), 0, 60 * 60 * 1000L);
            //timer.schedule(new BackUpCleanTask(), 0, 60 * 60 * 1000L);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
