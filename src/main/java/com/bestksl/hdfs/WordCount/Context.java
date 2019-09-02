package com.bestksl.hdfs.WordCount;

import java.util.HashMap;

public class Context {
    private HashMap<Object, Object> contextMap = new HashMap<>();

    public void write(Object key, Object Value) {
        contextMap.put(key, Value);
    }

    public Object get(Object o) {
        return contextMap.get(o);
    }

    public HashMap<Object, Object> getMap() {
        return contextMap;
    }
}
