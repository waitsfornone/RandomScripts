package com.playmaker.mtools;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Hashtable;

/**
 * com.playmaker.mtools.Upload data to playmaker
 * <p>
 * $server/api_import_file/$tenant_id/$integration_id/$file_size/$format/$filename
 * <p>
 * format is always CSV (for now)
 * filename is the filename only, not the entire path
 * the HTTP method is PUT
 */
public class Upload {

    /**
     * Default upload retries
     */
    private static int DEFAULT_RETRIES = 3;

    /**
     * Default back-off time difference in seconds
     */
    private static int DEFAULT_BACKOFF = 10;

    /**
     * File size of streaming chunks (in bytes)
     */
    private static int CHUNK_SIZE = 10485760;

    /**
     * Tester Method
     *
     * @param args Program arguments (unused)
     */
    public static void main(String[] args) {

        // Tim's testing values
        // Change these to test

        String file_path = "/Users/tenders/Documents/testing/out";
        String url_endpoint = "https://mobile.vividnoodle.com/api_import_file";
        // Carl's testing values
        // Change these to test
        //"/Users/cpoole_playmaker/Desktop/testmirth"

        Hashtable<String, String> output = upload(file_path, url_endpoint);
        if (output.containsKey("failure")) {
            System.out.println(output.get("failure"));
        }
        if (output.containsKey("success")) {
            System.out.println(output.get("success"));
        }
    }

    /**
     * Attempts to upload all CSV files in the provided directory with a default try count.
     *
     * @param dir_path       The directory to upload files from.
     */
    public static Hashtable<String,String> upload(String dir_path, String url_endpoint) {
        return upload(dir_path, url_endpoint, DEFAULT_RETRIES);
    }

    /**
     * Attempts to upload all CSV files in the provided directory with a default try count.
     *
     * @param dir_path       The directory to upload files from.
     * @param retryAttempts  The number of times to retry uploading a file if there is a failure.
     */
    public static Hashtable<String, String> upload(String dir_path, String url_endpoint, int retryAttempts) {

        String tenant_id = "";
        String integration_id = "";
        File dir = new File(dir_path);
        Hashtable<String, String> results = new Hashtable<String, String>(2);

        if(!dir.canRead()){
            String err = String.format("DIRECTORY %s DOES NOT HAVE READ PERMISSIONS. ABORTING...", dir_path);
            String hash_val = logBuilder(dir_path, "", err);
            resultsExtender(results, "failure", hash_val);
            return results;
        }
        if(!dir.canWrite()){
            String err = String.format("DIRECTORY %s DOES NOT HAVE WRITE PERMISSIONS. ABORTING...", dir_path);
            String hash_val = logBuilder(dir_path, "", err);
            resultsExtender(results, "failure", hash_val);
            return results;
        }
        File[] directoryListing = dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                //inline return
                return !file.isHidden();
            }
        });
        Arrays.sort(directoryListing, new Comparator<File>(){
            public int compare(File f1, File f2)
            {
                //inline return
                return String.valueOf(f1.getName()).compareTo(f2.getName());
            } });
        if (directoryListing != null) {
            for (File child : directoryListing) {
                //get tenant and integration ids here
                String[] filename_split = FilenameUtils.getBaseName(child.getName()).split("_");
                tenant_id = filename_split[0];
                integration_id = filename_split[1];

                Hashtable<String, String> up_results = uploadFile(tenant_id, integration_id, child.getAbsolutePath(), url_endpoint, retryAttempts);

                if (up_results.containsKey("success") || up_results.containsKey("iderr")) {
                    //Log id issues as failure, but still delete
                    if (up_results.containsKey("iderr")) {
                        Hashtable<String, String> id_res = new Hashtable<String, String>(1);
                        id_res.put("failure", up_results.get("iderr"));
                        resultsExtender(results, id_res);
                    }
                    // Delete file after successful upload
                    if (!child.delete()) {
                        String err = String.format("FILE %s COULD NOT BE DELETED AFTER UPLOAD", child.getName());
                        //remove this after above
                        if (!child.renameTo(new File(child.getName() + ".txt"))) {
                            err = String.format("FILE %s COULD NOT BE RENAMED AFTER FAILED DELETE. PLEASE CHECK PERMISSIONS\n", child.getName());
                        }
                        String hash_val = logBuilder(child.getPath(), "", err);
                        resultsExtender(results, "failure", hash_val);
                    }
                    if (!up_results.containsKey("iderr")) {
                        resultsExtender(results, up_results);
                    }
                } else {
                    resultsExtender(results, up_results);
                }
            }
        } else {
            String err = "FILE DIRECTORY PROVIDED IS EMPTY OR NOT VALID";
            String hash_val = logBuilder(dir_path, "", err);
            resultsExtender(results, "failure", hash_val);
        }
        return results;
    }

    /**
     * Calls the file upload method with a default retry count.
     *
     * @param tenant_id      ID of the tenant user.
     * @param integration_id ID of the integration.
     * @param file_path      full path of the file to upload to the server.
     */
    public static Hashtable<String, String> uploadFile(String tenant_id, String integration_id, String url_endpoint, String file_path) {
        return Upload.uploadFile(tenant_id, integration_id, file_path, url_endpoint, DEFAULT_RETRIES);
    }

    /**
     * Uploads a file to vnext integration storage.
     *
     * @param tenant_id      ID of the tenant user.
     * @param integration_id ID of the integration.
     * @param file_path      full path of the file to upload to the server.
     * @param retryAttempts  The number of times to retry uploading a file if there is a failure.
     */
    public static Hashtable<String, String> uploadFile(String tenant_id, String integration_id, String file_path, String url_endpoint, int retryAttempts) {

        File file = new File(file_path);
        Hashtable<String, String> results = new Hashtable<String, String>(1);

        // Skip if file is not CSV
        if(!"csv".equals(FilenameUtils.getExtension(file_path))) {
            String err = String.format("SAW FILE %s BUT NOT CSV. IGNORING...\n", file.getName());
            String hash_val = logBuilder(file.getName(), "", err);
            return resultsExtender(results, "failure", hash_val);
        }

        // If we can't read the file then problem. Quit.
        if(!file.canRead()){
            String err = String.format("BAD READ PERMISSION ON FILE %s. SKIPPING...\n", file.getName());
            String hash_val = logBuilder(file.getName(), "", err);
            return resultsExtender(results, "failure", hash_val);
        }

        long fileSize = file.length();

        // Get the file name from the file path
        String filename = FilenameUtils.getBaseName(file_path) + "." + FilenameUtils.getExtension(file_path);

        // Resolve the full server URL
        String FULL_URL = String.format("%s/%s/%s/%d/%s/%s", url_endpoint, tenant_id, integration_id, fileSize, FilenameUtils.getExtension(file_path), filename);

        HttpURLConnection connection;

        int tryCount = 0;

        while (tryCount < retryAttempts) {

            tryCount++;

            try {

                // Setup new web connection to the server
                connection = ((HttpURLConnection) new URL(FULL_URL).openConnection());
                connection.setRequestMethod("PUT");
                connection.setUseCaches(false);
                connection.setDoOutput(true);
                connection.setChunkedStreamingMode(CHUNK_SIZE);
                connection.connect();

                // Get an output stream we can write over the connection to
                OutputStream outputStream = connection.getOutputStream();

                // Make a file input stream and buffered stream to read the file from the disk in chunks
                FileInputStream streamFileInputStream = new FileInputStream(file);
                BufferedInputStream streamFileBufferedInputStream = new BufferedInputStream(streamFileInputStream);

                // Make a byte buffer to hold some data from the file
                byte[] streamFileBytes = new byte[4096];
                int bytesRead;
                //int totalBytesRead = 0;

                // Run through the file from disk and pass it to the connection via the buffer
                while ((bytesRead = streamFileBufferedInputStream.read(streamFileBytes)) > 0) {
                    outputStream.write(streamFileBytes, 0, bytesRead);
                    outputStream.flush();

                    /*totalBytesRead += bytesRead;
                    if ((totalBytesRead % 2097152) == 0) {
                        System.out.printf(".. Free memory after %d bytes read: %d\n", totalBytesRead, Runtime.getRuntime().freeMemory());
                    }*/
                }

                //streamFileBufferedInputStream.close();
                outputStream.close();

                // Print out connection response
                /*System.out.printf(".. Server code %s: %s \n",
                        String.valueOf(connection.getResponseCode()),
                        String.valueOf(connection.getResponseMessage()));*/

                String serverResponse = getResponseBody(connection.getInputStream());


                // --- Handle server responses -------------------------------------------------

                // Success
                if (connection.getResponseCode() == 200) {
                    
                    if (serverResponse.trim().length() == 0) {
                        String err = "Server side exception. File was not uploaded.";
                        String hash_val = logBuilder(connection.getURL().toString(), Integer.toString(connection.getResponseCode()), err);
                        return resultsExtender(results, "failure", hash_val);
                    }
                    
                    try {
                        JSONObject jsonResponse = new JSONObject(serverResponse);
                        //empty serverResponse assumes server exception

                        if (jsonResponse.has("success") && !jsonResponse.has("error")) {
                            // 200 response is good and we do not need to try again. Stop.
                            String hash_val = logBuilder(connection.getURL().toString(), Integer.toString(connection.getResponseCode()), serverResponse);
                            return resultsExtender(results, "success", hash_val);
                        } else if (jsonResponse.has("error")) {
                            if (jsonResponse.getString("error").contains("can't find tenant for tenant uuid")) {
                                String hash_val = logBuilder(connection.getURL().toString(), Integer.toString(connection.getResponseCode()), serverResponse);
                                return resultsExtender(results, "iderr", hash_val);
                            } else if (jsonResponse.getString("error").contains("can't find integration with id of")) {
                                String hash_val = logBuilder(connection.getURL().toString(), Integer.toString(connection.getResponseCode()), serverResponse);
                                return resultsExtender(results, "iderr", hash_val);
                            } else {
                                String hash_val = logBuilder(connection.getURL().toString(), Integer.toString(connection.getResponseCode()), serverResponse);
                                return resultsExtender(results, "failure", hash_val);
                            }
                        } else {
                            String hash_val = logBuilder(connection.getURL().toString(), Integer.toString(connection.getResponseCode()), serverResponse);
                            return resultsExtender(results, "failure", hash_val);
                        }
                    } catch (Exception e) {
                        // Problem with the JSON response from server. Did not contain "Success" parameter.
                        String hash_val = org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace(e);
                        e.printStackTrace();
                        return resultsExtender(results, "failure", hash_val);
                    }

                }

                // Not Found
                if (connection.getResponseCode() == 404) {
                    String hash_val = logBuilder(connection.getURL().toString(), Integer.toString(connection.getResponseCode()), serverResponse);
                    return resultsExtender(results, "failure", hash_val);
                }

            } catch (Exception e) {

                // Handle when server can't be reached (Network failure)
                if (e instanceof UnknownHostException) {
                    String err = "SERVER CANNOT BE REACHED --> CHECK NETWORK";
                    return resultsExtender(results, "failure", err);
                } else {
                    String hash_val = org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace(e);
                    e.printStackTrace();
                    return resultsExtender(results, "failure", hash_val);
                }
            }

            // If we got this far there was a problem. Need to retry...
            // Need to NOT retry when it is a data issue. Next step.
            if (tryCount < retryAttempts) {
                int retryTime = tryCount * DEFAULT_BACKOFF;
                //System.out.printf(".. Retrying in %ds. Run %d out of %d times... \n", retryTime, tryCount, retryAttempts);
                try {
                    Thread.sleep(retryTime * 1000);
                } catch (Exception e) {
                    //Need something here?
                    System.out.println("RETRY INTERRUPTED");
                }

            } else {
                //Need something here?
                System.out.println("FILE UPLOAD FAILED");
            }
        }
        String err = "Loop failure, number of retries is <= 0";
        String hash_val = logBuilder(file.getName(), "", err);
        return resultsExtender(results, "failure", hash_val);
    }

    /**
     * Helper method to convert the server response stream to a String for easy viewing.
     *
     * @param responseStream The response body from the server.
     * @return String of the response body.
     * @throws IOException Exception if there is an issue with stream reading.
     */
    private static String getResponseBody(InputStream responseStream) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        BufferedReader br = new BufferedReader(new InputStreamReader(responseStream));
        while ((line = br.readLine()) != null) {
            stringBuilder.append(line).append("\n");
        }
        return stringBuilder.toString();
    }

    private static String logBuilder(String url, String status, String responsePayload) {
        return "com.playmaker.mtools.Upload.UploadFile -- " + responsePayload + " -- " + status + " -- " + url; 
    }

    private static Hashtable<String, String> resultsExtender(Hashtable<String, String> hash_to, String key, String val) {
        if (hash_to.containsKey(key)) {
            val = hash_to.get(key) + "\n" + val; 
        }
        hash_to.put(key, val);
        return hash_to;
    }

    private static Hashtable<String, String> resultsExtender(Hashtable<String, String> hash_to, Hashtable<String, String> hash_from) {
        for(String key: hash_from.keySet()){
            resultsExtender(hash_to, key, hash_from.get(key));
        }
        return hash_to;
    }
}