package com.playmaker.mtools;

import java.sql.*;
import java.io.File;
import java.io.FileWriter;
import java.io.FileFilter;
import java.lang.management.ManagementFactory;
import com.opencsv.*;
import java.util.Date;
import java.security.MessageDigest;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.io.FilenameUtils;

public class JDBCtoCSV {

    public static void main(String[] args) {
        String db_driver = "org.postgresql.Driver";
        String conn_str = "jdbc:postgresql://192.168.103.102:5432/playmaker";
        String user = "integration";
        String pass = "(qaswedfr{};')";
        String out_dir = "/Users/tenders/Documents/testing";
        String int_uuid = "6fea45fe-e08d-47ea-98ff-255ef58c6545";
        String query = "select * from pm20029.referrals;";
        String tenant = "859e4142-67f8-4807-a0e4-937f02b01319";

        GetData(db_driver, conn_str, user, pass, out_dir, tenant, int_uuid, query, true);
        
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
        String results = "";
        File upload_file = null;
        String tmp_file = null;
        String tmp_dir = null;
        Long file_time = 0L;

        //Getting Unix epoch for filename
        java.util.Date timestamp = new java.util.Date();
        Long epoch = Long.valueOf(timestamp.getTime() / 1000L);

        //Getting PID for temporary file
        String running_pid = ManagementFactory.getRuntimeMXBean().getName();

        //Check output directory for write access (and that it is not a file)
        File tmp_chk = new File(out_dir);
        if (!tmp_chk.isDirectory() || !tmp_chk.canWrite()) {
            System.out.println("Provided directory either doesn't exist, or is not writeable.");            
            return "Provided directory either doesn't exist, or is not writeable.";
        }
        tmp_dir = FilenameUtils.concat(out_dir, "out");
        File tmp = new File(tmp_dir);
        if (!tmp.exists()) {
            tmp.mkdir();
        }
        if (!tmp.canWrite()) {
            System.out.println(tmp_dir);
            System.out.println("Can't write to the output directory");            
            return "Can't write to the output directory";
        }
        //This isn't in the correct dir...
        String tmp_filename = running_pid + ".csv";
        tmp_file = FilenameUtils.concat(tmp_dir, tmp_filename);
        
        //Doing freespace checks
        Long freespace = tmp_chk.getUsableSpace();
        Long free_gigs = freespace/1073741824;
        System.out.println(free_gigs);
        if (free_gigs < 10) {
            if (purgefiles) {
                File[] stuck_files = tmp.listFiles(new FileFilter() {
                    @Override
                    public boolean accept(File file) {
                        return !file.isHidden();
                    }
                });
                if (stuck_files.length != 0) {
                    for (File child : stuck_files) {
                        System.out.println(child.getName());
                        if (!".DS_Store".equals(child.getName())) {
                            file_time = child.lastModified();
                        }
                        if (file_time != 0 && (epoch - file_time) >= 7776000L) {
                            child.delete();
                        }
                    }
                } else {
                    System.out.println("No files to be purged.");
                }
                System.out.println("Files purged to create free space");
            }
            freespace = tmp_chk.getUsableSpace();
            free_gigs = freespace/1073741824;
            if (free_gigs < 10) {
                System.out.println("Not enough freespace to generate new files.");
                return "Not enough freespace to generate new files.";
            }
        }

        //Hashing the Query for filename
        String query_hash = null;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(sql.getBytes("US-ASCII"));
            byte[] digest = md.digest();
            query_hash = javax.xml.bind.DatatypeConverter.printHexBinary(digest);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //Creating output filename for Upload script
        String filename = tenant_id + "_" + int_uuid + "_" + epoch + "_" + query_hash + ".csv";
        
        //Generate temporary file from SQL query
        try {
            Class.forName(dbdriver);
            Connection conn = java.sql.DriverManager.getConnection(db_conn_str, username, password);
            java.sql.Statement stmt = conn.createStatement(1005, 1007);
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
            return "No output file was created. Please try again.";
        }
        if (f.renameTo(upload_file)) {
            return upload_file.getPath();
        } else {
            System.out.println("rename failure");
            return "rename failure";
        }
    }
}