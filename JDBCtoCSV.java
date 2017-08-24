package jdbctocsv;

import java.sql.*;
import java.io.FileWriter;
import com.opencsv.*;
import java.util.Date;
import java.security.MessageDigest;
import javax.xml.bind.DatatypeConverter;
public class JDBCtoCSV {

    public static void GetData(String dbdriver, String db_conn_str, String username, String password, String out_path, String int_uuid, String sql) {
        String query_hash = null;
        Date file_time = new Date();
        Long epoch = (file_time.getTime()/1000);
        try {
            String hash_in = sql;
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(sql.getBytes("US-ASCII"));
            byte[] digest = md.digest();
            query_hash = DatatypeConverter.printHexBinary(digest);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String filename = out_path + int_uuid + "_" + epoch + "_" + query_hash + ".csv";
        try {
            CSVWriter csv_write = new CSVWriter(new FileWriter(filename), ',');                    
            Class.forName(dbdriver);
            Connection conn = DriverManager.getConnection(db_conn_str, username, password);
            Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = stmt.executeQuery(sql);
            csv_write.writeAll(rs, true);
            conn.close();            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}