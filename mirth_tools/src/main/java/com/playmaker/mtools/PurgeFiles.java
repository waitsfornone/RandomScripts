package com.playmaker.mtools;

import java.util.Date;
import java.io.File;
import java.io.FileFilter;
import com.playmaker.mtools.Lumberjack;
import com.playmaker.mtools.FileCabinet;


public class PurgeFiles {

    public static void main(String[] args) {
        ;
    }

    public static Long getEpoch() {
        Date timestamp = new Date();
        return Long.valueOf(timestamp.getTime() / 1000L);
    }

    public static String purgeFiles(File file_path) {
        Long file_time = 0L;
        Long now = getEpoch();
        File[] stuck_files = file_path.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return !file.isHidden();
            }
        });
        if (stuck_files.length != 0) {
            for (File child : stuck_files) {
                if (!".DS_Store".equals(child.getName())) {
                    file_time = child.lastModified();
                }
                if (file_time != 0 && (now - file_time) >= 7776000L) {
                    child.delete();
                }
            }
        } else {
            return Lumberjack.logBuilder("No files to be purged.", PurgeFiles.class.getName());
        }
        return Lumberjack.logBuilder("Files purged to create free space", PurgeFiles.class.getName());
    }

    public static String thePurge(File file_path, Boolean purgefiles) {

        if (FileCabinet.freespaceCheck(file_path) < 10) {
            if (purgefiles) {
                return purgeFiles(file_path);
            }
            if (FileCabinet.freespaceCheck(file_path) < 10) {
                return Lumberjack.logBuilder("Not enough freespace to generate new files.", PurgeFiles.class.getName());
            }
        } else {
            return null;
        }
    return Lumberjack.logBuilder("this should never appear", PurgeFiles.class.getName());
    }

}