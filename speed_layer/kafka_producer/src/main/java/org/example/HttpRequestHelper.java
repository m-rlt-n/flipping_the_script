package org.example;

//import com.fasterxml.jackson.databind.JsonNode;
//import org.json.JSONArray;
//import org.json.JSONObject;
//
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.io.OutputStreamWriter;
//import java.io.Writer;
//import java.net.HttpURLConnection;
//import java.net.URL;
//import java.util.Scanner;
//
//public class HttpRequestHelper {
//
//    public static JSONObject fetchData(String url, int batch_size, int offset, String[] headers) {
//        try {
//            URL apiUrl = new URL(url + "?$limit=" + batch_size + "&$offset=" + offset);
//            HttpURLConnection connection = (HttpURLConnection) apiUrl.openConnection();
//
//            for (String header : headers) {
//                String[] parts = header.split(": ");
//                connection.setRequestProperty(parts[0], parts[1]);
//            }
//
//            connection.setRequestMethod("GET");
//
//            int responseCode = connection.getResponseCode();
//            if (responseCode == HttpURLConnection.HTTP_OK) {
//                Scanner scanner = new Scanner(connection.getInputStream());
//                StringBuilder response = new StringBuilder();
//
//                while (scanner.hasNextLine()) {
//                    response.append(scanner.nextLine());
//                }
//
//                scanner.close();
//                if (response.charAt(0) == '[') {
//                    return new JSONObject(response.toString());
//                } else {
//                    // Handle non-array response if needed
//                    System.out.println("Response is not a JSON array");
//                }
//            } else {
//                System.out.println("Failed to fetch data. Response Code: " + responseCode);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        return null;
//    }
//
//    public static int countRecords(String response) {
//        // Implement logic to count records in the JSON response
//        // For simplicity, assuming each record is a separate JSON object
//        return response.split("\\{").length - 1;
//    }
//}

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

public class HttpRequestHelper {

    public static JSONObject fetchData(String url, int batch_size, int offset, String[] headers) {
        try {
            URL apiUrl = new URL(url + "?$limit=" + batch_size + "&$offset=" + offset);
            HttpURLConnection connection = (HttpURLConnection) apiUrl.openConnection();

            for (String header : headers) {
                String[] parts = header.split(": ");
                connection.setRequestProperty(parts[0], parts[1]);
            }

            connection.setRequestMethod("GET");

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                Scanner scanner = new Scanner(connection.getInputStream());
                StringBuilder response = new StringBuilder();

                while (scanner.hasNextLine()) {
                    response.append(scanner.nextLine());
                }

                scanner.close();

                // Check if the response is a JSON array
                if (response.charAt(0) == '[') {
                    JSONArray jsonArray = new JSONArray(response.toString());

                    // Check if the array is not empty
                    return jsonArray.getJSONObject(0);

                } else {
                    // Handle non-array response if needed
                    System.out.println("Response is not a JSON array");
                }
            } else {
                System.out.println("Failed to fetch data. Response Code: " + responseCode);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static int countRecords(String response) {
        // Implement logic to count records in the JSON response
        // For simplicity, assuming each record is a separate JSON object
        return response.split("\\{").length - 1;
    }
}
