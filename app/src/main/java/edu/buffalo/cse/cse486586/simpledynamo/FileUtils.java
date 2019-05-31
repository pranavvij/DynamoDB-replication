package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.util.Log;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FileUtils {

    public static void writeToFile(String key, String value, Context context) {
        try {
            FileOutputStream fileOutputStream = context.openFileOutput(key, Context.MODE_PRIVATE);
            String input = value + "!" + System.currentTimeMillis();
            fileOutputStream.write(input.getBytes());
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeToFileWithOutTimeStamp(String key, String value, Context context) {
        try {
            FileOutputStream fileOutputStream = context.openFileOutput(key, Context.MODE_PRIVATE);
            String input = value;
            fileOutputStream.write(input.getBytes());
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void deleteAllFiles(Context context) {
        String[] files = context.fileList();
        for (String file : files) {
            deleteFile(context, file);
        }
    }

    public static String[] getAllFile(Context context) {
        return context.fileList();
    }

    public static void deleteFile(Context context, String file) {
        context.deleteFile(file);
    }

    public static Message getKeyValue(Context context, String key) {
        Message message  = new Message();
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = context.openFileInput(key);
            int count;
            StringBuilder val = new StringBuilder();
            while ((count = fileInputStream.read()) != -1) {
                val.append((char) count);
            }
            String value = val.toString();
            message.value = value;
            message.key = key;
            fileInputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return message;
        } catch (IOException e) {
            e.printStackTrace();
            return message;
        }
        return message;
    }

}
