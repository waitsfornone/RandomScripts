package com.playmaker.mtools;

import org.apache.commons.io.FilenameUtils;
import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Comparator;

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

        // Carl's testing values
        // Change these to test
        //"/Users/cpoole_playmaker/Desktop/testmirth"

        upload(file_path);
    }

    /**
     * Attempts to upload all CSV files in the provided directory with a default try count.
     *
     * @param dir_path       The directory to upload files from.
     */
    public static void upload(String dir_path) {
        upload(dir_path, DEFAULT_RETRIES);
    }

    /**
     * Attempts to upload all CSV files in the provided directory with a default try count.
     *
     * @param dir_path       The directory to upload files from.
     * @param retryAttempts  The number of times to retry uploading a file if there is a failure.
     */
    public static String upload(String dir_path, int retryAttempts) {

        String tenant_id = "";
        String integration_id = "";
        File dir = new File(dir_path);

        if(!dir.canRead()){
            System.out.println("!! DIRECTORY DOES NOT HAVE READ PERMISSIONS. ABORTING...");
            return "!! DIRECTORY DOES NOT HAVE READ PERMISSIONS. ABORTING...";
        }

        File[] directoryListing = dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return !file.isHidden();
            }
        });
        Arrays.sort(directoryListing, new Comparator<File>(){
            public int compare(File f1, File f2)
            {
                return Long.valueOf(f1.lastModified()).compareTo(f2.lastModified());
            } });
        if (directoryListing != null) {
            for (File child : directoryListing) {
                //get tenant and integration ids here
                String[] filename_split = FilenameUtils.getBaseName(child.getName()).split("_");
                tenant_id = filename_split[0];
                integration_id = filename_split[1];

                boolean success = uploadFile(tenant_id, integration_id, child.getAbsolutePath(), retryAttempts);

                if (success) {
                    // Delete file after successful upload
                    if (!child.delete()) {
                        System.out.printf("!! FILE %s COULD NOT BE DELETED AFTER UPLOAD\n", child.getName());
                        if (!child.renameTo(new File(child.getName() + ".txt"))) {
                            System.out.printf("!! FILE %s COULD NOT BE RENAMED AFTER FAILED DELETE. PLEASE CHECK PERMISSIONS\n", child.getName());
                        }
                    }
                }
            }
            return "FILES UPLOADED SUCCESSFULLY";
        } else {
            System.out.println("!! FILE DIRECTORY PROVIDED IS EMPTY OR NOT VALID");
            return "!! FILE DIRECTORY PROVIDED IS EMPTY OR NOT VALID";
        }
    }

    /**
     * Calls the file upload method with a default retry count.
     *
     * @param tenant_id      ID of the tenant user.
     * @param integration_id ID of the integration.
     * @param file_path      full path of the file to upload to the server.
     */
    public static boolean uploadFile(String tenant_id, String integration_id, String file_path) {
        return Upload.uploadFile(tenant_id, integration_id, file_path, DEFAULT_RETRIES);
    }

    /**
     * Uploads a file to vnext integration storage.
     *
     * @param tenant_id      ID of the tenant user.
     * @param integration_id ID of the integration.
     * @param file_path      full path of the file to upload to the server.
     * @param retryAttempts  The number of times to retry uploading a file if there is a failure.
     */
    public static boolean uploadFile(String tenant_id, String integration_id, String file_path, int retryAttempts) {

        String BASE_URL = "https://mobile.vividnoodle.com/api_import_file";
        File file = new File(file_path);

        // Skip if file is not CSV
        if(!"csv".equals(FilenameUtils.getExtension(file_path))) {
            System.out.printf("!! SAW FILE %s BUT NOT CSV. IGNORING...\n", file.getName());
            return false;
        }

        // If we can't read the file then problem. Quit.
        if(!file.canRead()){
            System.out.printf("!! BAD READ PERMISSION ON FILE %s. SKIPPING...\n", file.getName());
            return false;
        }

        long fileSize = file.length();

        // Get the file name from the file path
        String filename = FilenameUtils.getBaseName(file_path) + FilenameUtils.getExtension(file_path);

        // Resolve the full server URL
        String FULL_URL = String.format("%s/%s/%s/%d/csv/%s", BASE_URL, tenant_id, integration_id, fileSize, filename);

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
                int totalBytesRead = 0;

                // Run through the file from disk and pass it to the connection via the buffer
                while ((bytesRead = streamFileBufferedInputStream.read(streamFileBytes)) > 0) {
                    outputStream.write(streamFileBytes, 0, bytesRead);
                    outputStream.flush();

                    totalBytesRead += bytesRead;
                    /*if ((totalBytesRead % 2097152) == 0) {
                        System.out.printf(".. Free memory after %d bytes read: %d\n", totalBytesRead, Runtime.getRuntime().freeMemory());
                    }*/
                }

                outputStream.close();

                // Print out connection response
                System.out.printf(".. Server code %s: %s \n",
                        String.valueOf(connection.getResponseCode()),
                        String.valueOf(connection.getResponseMessage()));

                // --- Handle server responses -------------------------------------------------

                // Success
                if (connection.getResponseCode() == 200) {

                    String serverResponse = getResponseBody(connection.getInputStream());

                    try {

                        JSONObject jsonResponse = new JSONObject(serverResponse);

                        if (jsonResponse.getInt("success") == 1) {
                            // 200 response is good and we do not need to try again. Stop.
                            System.out.printf(":) SERVER RESPONSE --> FILE %s UPLOADED SUCCESSFULLY\n", file.getName());
                            return true;
                        } else {
                            System.out.println("!! SERVER RESPONSE --> UPLOAD FAILED");
                        }
                    } catch (Exception e) {
                        //THIS NEEDS UPDATED WHEN THAD CHANGES THE RESPONSE BEHAVIOR!!!!!!!!
                        // Problem with the JSON response from server. Did not contain "Success" parameter.
                        System.out.println("!! SERVER RESPONSE --> UNKNOWN FAILURE");
                        System.out.println(".. BODY:\n" + serverResponse);
                        return true;
                    }

                }

                // Not Found
                if (connection.getResponseCode() == 404) {
                    System.out.println("!! SERVER RESPONSE --> 404 ENDPOINT NOT FOUND");
                    return false;
                }

            } catch (Exception e) {

                // Handle when server can't be reached (Network failure)
                if (e instanceof UnknownHostException) {
                    System.out.println("!! SERVER CANNOT BE REACHED --> CHECK NETWORK");
                } else {
                    e.printStackTrace();
                }

            }

            // If we got this far there was a problem. Need to retry...
            if (tryCount < retryAttempts) {
                int retryTime = tryCount * DEFAULT_BACKOFF;
                System.out.printf(".. Retrying in %ds. Run %d out of %d times... \n", retryTime, tryCount, retryAttempts);
                try {
                    Thread.sleep(retryTime * 1000);
                } catch (Exception e) {
                    System.out.println("!! RETRY INTERRUPTED");
                }

            } else {
                System.out.println("!! FILE UPLOAD FAILED");
            }
        }
        return false;
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

    /**
     * Checks to see if the file has a CSV extension.
     *
     * @param file The file reference.
     * @return True if the file is a CSV file, false if not.
     */
    private static boolean isFileCSV(File file) {
        String fileName = file.getName();
        int dotIndex = fileName.lastIndexOf('.');
        return dotIndex != -1 && fileName.substring(dotIndex + 1).equalsIgnoreCase("CSV");
    }
}