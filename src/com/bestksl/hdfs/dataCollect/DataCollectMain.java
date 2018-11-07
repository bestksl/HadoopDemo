package com.bestksl.hdfs.dataCollect;

import java.util.Timer;

public class DataCollectMain {
    public static void main(String[] args) {
        Timer timer = new Timer();
        try {
            timer.schedule(new CollectTask(), 0, 60 * 60 * 1000L);

        } catch (Exception e) {
            e.printStackTrace();
        }
        timer.schedule(new BackUpCleanTask(), 0, 60 * 60 * 1000L);
    }


}
