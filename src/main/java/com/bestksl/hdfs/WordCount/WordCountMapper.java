package com.bestksl.hdfs.WordCount;

public class WordCountMapper implements Mapper {
    @Override
    public void map(String line, Context context) {
        String[] strs = line.split(" ");
        for (String str : strs) {
            Object value = context.get(str);
            if (value == null) {
                context.write(str, 1);
            } else {
                context.write(str, (int) value + 1);
            }
        }
    }
}
