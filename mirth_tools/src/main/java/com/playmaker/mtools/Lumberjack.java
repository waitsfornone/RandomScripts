package com.playmaker.mtools;


public class Lumberjack {

    public static void main(String[] args) {
        ;
    }

    public static String logBuilder(String message, String class_name) {
        return class_name + " -- " + message; 
    }

    public static String logAggregator(String log_str, String new_msg) {
        if (log_str.length() == 0) {
            return new_msg;
        } else {
            return log_str + "\n" + new_msg;
        }
    }
}

//this.getClass().getCanonicalName()