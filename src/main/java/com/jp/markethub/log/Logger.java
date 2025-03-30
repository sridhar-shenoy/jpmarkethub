package com.jp.markethub.log;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Logger {
    private static Logger instance;
    private final DateTimeFormatter formatter;
    
    private Logger() {
        formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    }
    
    public static synchronized Logger getInstance() {
        if (instance == null) {
            instance = new Logger();
        }
        return instance;
    }
    
    public void info(Class<?> clazz, String message) {
        log("INFO", clazz, message);
    }
    
    public void debug(Class<?> clazz, String message) {
        log("DEBUG", clazz, message);
    }
    
    public void error(Class<?> clazz, String message) {
        log("ERROR", clazz, message);
    }
    
    private void log(String level, Class<?> clazz, String message) {
        String timestamp = LocalDateTime.now().format(formatter);
        String logLine = String.format("[%s]---[%s] [%s] [%s] - %s",Thread.currentThread().getName(),
            timestamp, clazz.getSimpleName(), level, message);
        System.out.println(logLine);
        System.out.flush();
    }

    public boolean isDebugEnabled() {
        return false;
    }
}