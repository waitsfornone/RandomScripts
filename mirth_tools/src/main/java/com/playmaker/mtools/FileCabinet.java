package com.playmaker.mtools;

import java.io.File;

public class FileCabinet {

    public static void main(String[] args) {
        ;
    }

    public static Long freespaceCheck(File file_path) {
        return file_path.getUsableSpace()/1073741824;
    }
}