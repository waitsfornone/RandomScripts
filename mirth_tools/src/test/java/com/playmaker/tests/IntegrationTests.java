package com.playmaker.tests;

import org.junit.Test;
import org.junit.Rule;
import static org.junit.Assert.*;
import org.junit.rules.TemporaryFolder;
import java.io.File;
import com.playmaker.mtools.JDBCtoCSV;
import com.playmaker.mtools.Upload;
import org.apache.commons.io.FilenameUtils;
import static org.mockito.Mockito.*;


public class IntegrationTests {

    /*@RunWith(PowerMockRunner.class)
@PrepareForTest(MyClassThatWillBeTested.class)
public class MyUnitTest{
    private File mockedFile = mock(File.class);

    @Before
    public void setUp() throws Exception {
        PowerMockito.whenNew(File.class).withAnyArguments().thenReturn(mockedFile);
    }
}
*/

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();
    public String db_driver = "org.sqlite.JDBC";
    public String user = "";
    public String pass = "";
    
    @Test
    public void ITGetDataParamDirectoryFailure() {
        File fold1 = null;
        File file1 = null;
        try {
            fold1 = testFolder.newFolder("tempfold");
            file1 = File.createTempFile("file1", "txt", fold1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        fold1.setWritable(false, false);
        file1.setWritable(true, false);
        String results = JDBCtoCSV.GetData(null, null, null, null, fold1.getAbsolutePath(), null, null, null, null);
        assertEquals("com.playmaker.mtools.JDBCtoCSV -- Provided directory either doesn't exist, or is not writeable.", results);
        results = JDBCtoCSV.GetData(null, null, null, null, file1.getAbsolutePath(), null, null, null, null);
        assertEquals("com.playmaker.mtools.JDBCtoCSV -- Provided directory either doesn't exist, or is not writeable.", results);
    }

    @Test
    public void ITGetDataOutDirectoryFailure() {
        File fold1 = null;
        File fold2 = null;
        try {
            fold1 = testFolder.newFolder("tempfold");
            String tmp_dir = FilenameUtils.concat(fold1.getAbsolutePath(), "out");
            fold2 = new File(tmp_dir);
            fold2.mkdir();
        } catch (Exception e) {
            e.printStackTrace();
        }
        fold1.setWritable(true, false);
        fold2.setWritable(false, false);
        String results = JDBCtoCSV.GetData(null, null, null, null, fold1.getAbsolutePath(), null, null, null, null);
        assertEquals("com.playmaker.mtools.JDBCtoCSV -- Can't write to the output directory", results);
    }

    /*GetData(String dbdriver,
    String db_conn_str,
    String username,
    String password,
    String out_dir,
    String tenant_id,
    String int_uuid,
    String sql,
    Boolean purgefiles)*/

    /*@Test
    public void ITGetDataNoFreespaceNoPurgeFailure() {
        File fold1 = null;
        JDBCtoCSV mocked_class = mock(JDBCtoCSV.class);
        try {
            fold1 = testFolder.newFolder("tempfold");
        } catch (Exception e) {
            e.printStackTrace();
        }
        // doReturn(0L).when(mocked_class).freespaceCheck(any(File.class));
        doCallRealMethod().when(mocked_class).getEpoch();
        doCallRealMethod().when(mocked_class).purgeFiles(any(File.class));
        // when(mocked_class.GetData(anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean())).thenCallRealMethod();
        // when(mocked_class.logAggregator(anyString(), anyString())).thenCallRealMethod();
        // when(mocked_class.logBuilder(anyString())).thenCallRealMethod();
        //thenCallRealMethod();
        String results = mocked_class.GetData(null, null, null, null, fold1.getAbsolutePath(), null, null, null, false);
        assertEquals("com.playmaker.mtools.JDBCtoCSV.GetData -- Not enough freespace to generate new files.", results);
    }

    public void ITGetDataNoFreespaceYesPurgeFailure() {
        File fold1 = null;
        File freespace_file = mock(File.class);
        JDBCtoCSV mocked_class = mock(JDBCtoCSV.class);
        try {
            fold1 = testFolder.newFolder("tempfold");
        } catch (Exception e) {
            e.printStackTrace();
        }
        when(mocked_class.freespaceCheck(any(File.class))).thenReturn(0L);
        when(mocked_class.GetData(anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean())).thenCallRealMethod();
        //need to mock failure in purging fucntion, see above for getUsableSpace() mocking
        ;
    }
*/
    public void ITGetDataTempFileFailure() {
        //no temp file found after query
        ;
    }

    public void ITGetDataDBFailure() {
        //bad db connection
        ;
    }

    public void ITGetDataQueryFailure() {
        ;
    }

    public void ITGetDataRenameFailure() {
        //permissions mock to generate failure?
        ;
    }

    public void ITGetDataSuccess() {
        //what is the assert criteria here? filename?
        ;
    }
}