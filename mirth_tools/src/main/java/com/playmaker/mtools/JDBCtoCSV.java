package com.playmaker.mtools;

import java.sql.*;
import java.io.File;
import java.io.FileFilter;
import java.lang.management.ManagementFactory;
import com.opencsv.*;
import java.security.MessageDigest;
import org.apache.commons.io.FilenameUtils;
import com.playmaker.mtools.PurgeFiles;
import static com.playmaker.mtools.Lumberjack.logBuilder;
import static com.playmaker.mtools.Lumberjack.logAggregator;

public class JDBCtoCSV {

    public static void main(String[] args) {
        String db_driver = "org.sqlite.JDBC";
        String conn_str = "jdbc:sqlite:/Users/tenders/Downloads/database.sqlite";
        String user = "";
        String pass = "";
        String out_dir = "/Users/tenders/Documents/testing";
        String int_uuid = "6fea45fe-e08d-47ea-98ff-255ef58c6545";
        //bad int_uuid
        //String int_uuid = "19939115-4750-438b-9960-f92006b51dd0";
        String query = "select * from country;";
        String tenant = "859e4142-67f8-4807-a0e4-937f02b01319";
        //bad tenant
        //String tenant = "19939115-4750-438b-9960-f92006b51dd0";

        GetData(db_driver, conn_str, user, pass, out_dir, tenant, int_uuid, query, true);
        
    }

    public static String hashQuery(String query) {
        String results = "";
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(query.getBytes("US-ASCII"));
            byte[] digest = md.digest();
            results = javax.xml.bind.DatatypeConverter.printHexBinary(digest);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }

    public static String GetData(String dbdriver,
                                String db_conn_str,
                                String username,
                                String password,
                                String out_dir,
                                String tenant_id,
                                String int_uuid,
                                String sql,
                                Boolean purgefiles) {
        //Creating variables in largest scope for use later
        File upload_file = null;
        String tmp_file = null;
        String tmp_dir = null;
        String results = "";

        //Getting Unix epoch for filename and purging
        Long epoch = PurgeFiles.getEpoch();

        //Check output directory for write access (and that it is not a file)
        File tmp_chk = new File(out_dir);
        if (!tmp_chk.isDirectory() || !tmp_chk.canWrite()) {
            return logAggregator(results, logBuilder("Provided directory either doesn't exist, or is not writeable.", JDBCtoCSV.class.getName()));
        }

        //Create and check script output directory
        tmp_dir = FilenameUtils.concat(out_dir, "out");
        File tmp = new File(tmp_dir);
        if (!tmp.exists()) {
            tmp.mkdir();
        }
        if (!tmp.canWrite()) {
            return logAggregator(results, logBuilder("Can't write to the output directory", JDBCtoCSV.class.getName()));
        }

        //Doing freespace checks and purging of files
        PurgeFiles.thePurge(tmp, purgefiles);

        //Hashing the Query for filename
        String query_hash = hashQuery(sql);

        //Creating output filename for Upload script
        String filename = tenant_id + "_" + int_uuid + "_" + epoch + "_" + query_hash + ".csv";



        //Using PID as temporary file name
        String tmp_filename = ManagementFactory.getRuntimeMXBean().getName() + ".csv";
        tmp_file = FilenameUtils.concat(tmp_dir, tmp_filename);
        
        //Generate temporary file from SQL query
        try {
            Class.forName(dbdriver);
            Connection conn = java.sql.DriverManager.getConnection(db_conn_str, username, password);
            java.sql.Statement stmt = conn.createStatement();
            java.sql.ResultSet rs = stmt.executeQuery(sql);
            CSVWriter csv_write = new CSVWriter(new java.io.FileWriter(tmp_file), ',');
            csv_write.writeAll(rs, true);
            csv_write.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        //Now we rename the file for the Upload portion
        File f = new File(tmp_file); 
        upload_file = new File(FilenameUtils.concat(tmp_dir, filename));
        if (!f.exists()) {
            return logAggregator(results, logBuilder("No output file was created. Please try again.", JDBCtoCSV.class.getName()));
        }
        if (f.renameTo(upload_file)) {
            return logAggregator(results, upload_file.getPath());
        } else {
            return logAggregator(results, logBuilder("rename failure", JDBCtoCSV.class.getName()));
        }
    }
}