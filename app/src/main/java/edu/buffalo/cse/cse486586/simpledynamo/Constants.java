package edu.buffalo.cse.cse486586.simpledynamo;

import android.net.Uri;

public class Constants {

    public static String RECOVER_REQUEST = "recovered_request#";

    public static String RECOVER_RESPONSE = "recovered_response";

    public static String IS_RECOVERED = "is_recovered";

    public static String INSERT_REQUEST = "insert_request#";

    public static String DELETE_REQUEST = "delete_request#";

    public static String QUERY_REQUEST = "query#request#";

    public static String QUERY_STAR = "query#*";


    public static String TESTING_RECOVERY = "testing_recovery";

    public static String QUERY = "query";


    public static String QUERY_STAR_SENDING = "query#*#sending#";

    public static String QUERY_STAR_RETURN = "query#*#return";

    public static String SENDING = "sending";

    public static String Dynamo_Test = "Dynamo_Test";

    public static Uri getUri() {
        return Constants.buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    }

    public static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}
