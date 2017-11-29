package com.playmaker.tests;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.Rule;
import com.playmaker.mtools.JDBCtoCSV;
import com.playmaker.mtools.Lumberjack;
import com.playmaker.mtools.Upload;
import com.playmaker.mtools.PurgeFiles;
import com.playmaker.mtools.FileCabinet;
import java.io.File;
import org.junit.rules.TemporaryFolder;
import java.util.Hashtable;
import java.util.Date;
import static org.mockito.Mockito.*;


public class UnitTests {

    @Test
    public void testGLumberjackLogBuilder() {
        assertEquals("com.playmaker.mtools.JDBCtoCSV -- test passed!", Lumberjack.logBuilder("test passed!", JDBCtoCSV.class.getName()));
    }

    @Test
    public void testGetDataLogAggregatorEmptyLog() {
        String log = "";
        String message = "test log aggregation";
        assertEquals(message, Lumberjack.logAggregator(log, message));
    }

    @Test
    public void testGetDataLogAggregatorWithLog() {
        String log = "log line 1";
        String message = "test log aggregation";
        assertEquals(log + "\n" + message, Lumberjack.logAggregator(log, message));
    }

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void testFileCabinetFreespaceCheck() {
        File root = new File("/");
        Long test_space = root.getUsableSpace()/1073741824;
        assertEquals(test_space, FileCabinet.freespaceCheck(root));
    }

    @Test
    public void testPurgeFilesThePurgeAll() {
        FileCabinet mockFileCab = mock(FileCabinet.class);
        when(mockFileCab.freespaceCheck(any(File.class))).thenReturn(0L);
        File fold1 = null;
        File file1 = null;
        File file2 = null;
        Date timestamp = new Date();
        Long cur_time = Long.valueOf(timestamp.getTime() / 1000L);
        Long new_time = (cur_time - 7862400L);
        try {
            fold1 = testFolder.newFolder("tempfold");
            file1 = File.createTempFile("file1", "txt", fold1);
            file2 = File.createTempFile("file2", "txt", fold1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        file2.setLastModified(new_time);
        file1.setLastModified(new_time);
        System.out.println(fold1.getAbsolutePath());
        String results = PurgeFiles.thePurge(fold1, true);
        assertEquals(0, fold1.listFiles().length);
    }

    // Need to mock out the freespacecheck as it is tied to Purge Files.
    // But that should prevent needing more testing on purges
    @Test
    public void testPurgeFilesthePurgeKeep() {
        File fold1 = null;
        File file1 = null;
        File file2 = null;
        Date timestamp = new Date();
        Long cur_time = Long.valueOf(timestamp.getTime() / 1000L);
        Long new_time = (cur_time - 7862400L);
        try {
            fold1 = testFolder.newFolder("tempfold");
            file1 = File.createTempFile("file1", "txt", fold1);
            file2 = File.createTempFile("file2", "txt", fold1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        file1.setLastModified(new_time);
        String results = PurgeFiles.thePurge(fold1, true);
        assertEquals(1, fold1.listFiles().length);
    }

    @Test
    public void testGetDataHashQuery() {
        assertEquals("7CC9AB9BA4C429D20883772A1D13BEA1", JDBCtoCSV.hashQuery("test my hash"));
    }

    @Test
    public void testUploadLogBuilder() {
        String text = "com.playmaker.mtools.Upload.UploadFile -- {success: 'OK'} -- 200 -- http://mysite.internet.the";
        assertEquals(text, Upload.logBuilder("http://mysite.internet.the", "200", "{success: 'OK'}"));
    }

    @Test
    public void testUploadResultsExtenderHashString() {
        Hashtable<String, String> results = new Hashtable<String, String>(2);
        Hashtable<String, String> test_hash = new Hashtable<String, String>(2);
        results.put("success", "yes\nyes");
        results.put("error", "no");
        test_hash.put("success", "yes");
        Upload.resultsExtender(test_hash, "success", "yes");
        Upload.resultsExtender(test_hash, "error", "no");
        assertEquals(results.get("success"), test_hash.get("success"));
        assertEquals(results.get("error"), test_hash.get("error"));
    }

    @Test
    public void testUploadResultsExtenderHashHash() {
        Hashtable<String, String> results = new Hashtable<String, String>(2);
        Hashtable<String, String> test_start = new Hashtable<String, String>(2);
        Hashtable<String, String> test_add = new Hashtable<String, String>(2);        
        results.put("success", "yes\nyes");
        results.put("error", "no");
        test_start.put("success", "yes");
        test_add.put("success", "yes");
        test_add.put("error", "no");
        Upload.resultsExtender(test_start, test_add);
        assertEquals(results.get("success"), test_start.get("success"));
        assertEquals(results.get("error"), test_start.get("error"));
    }

    @Test
    public void testPurgeFilesGetEpoch() {
        Date timestamp = new Date();
        Long unixtime = Long.valueOf(timestamp.getTime() / 1000L);
        assertEquals(unixtime, PurgeFiles.getEpoch());
    }
}