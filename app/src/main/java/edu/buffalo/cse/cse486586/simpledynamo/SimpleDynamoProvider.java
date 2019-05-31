package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

// References for help
//arrayBlockingQueue ====  https://www.geeksforgeeks.org/arrayblockingqueue-class-in-java/


public class SimpleDynamoProvider extends ContentProvider {

    static final int NUMBER_OF_AVDS = 5;
    static final int SERVER_PORT = 10000;
    static final String[] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
    String myPort;
    Node myNode;
    long recoveryTime = 0;

    List<Node> ring = new LinkedList<Node>();
    BlockingQueue<String> blockingRequestQueue = new ArrayBlockingQueue<String>(4);// for handling query response in star
    BlockingQueue<String> blockingQueue = new ArrayBlockingQueue<String>(3); // for handling query request in @ and single query until server response
    int counter = 0;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.d(Constants.Dynamo_Test + "  Deletion", "" + selection);

        Node node = getNodeForHashId(Utils.genHash(selection));
        if (node.nodeId.compareTo(myNode.nodeId) == 0 || node.successorOne.nodeId.compareTo(myNode.nodeId) == 0 || node.successorTwo.nodeId.compareTo(myNode.nodeId) == 0) {
            if (node.nodeId.compareTo(myNode.nodeId) == 0) {
                FileUtils.deleteFile(getContext(), selection);
                String msg = Constants.DELETE_REQUEST + selection;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.successorOne.nodeId);
                sleep(200);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.successorTwo.nodeId);
            } else if (node.successorOne.nodeId.compareTo(myNode.nodeId) == 0) {
                FileUtils.deleteFile(getContext(), selection);
                String msg = Constants.DELETE_REQUEST + selection;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.nodeId);
                sleep(200);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.successorTwo.nodeId);
            } else if (node.successorTwo.nodeId.compareTo(myNode.nodeId) == 0) {
                FileUtils.deleteFile(getContext(), selection);
                String msg = Constants.DELETE_REQUEST + selection;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.nodeId);
                sleep(200);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.successorOne.nodeId);
            }
        } else {// does nt below to current node delete in separate node from the replicas as well
            String msg = Constants.DELETE_REQUEST + selection;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.nodeId);
            sleep(200);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.successorOne.nodeId);
            sleep(200);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.successorTwo.nodeId);
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        synchronized (this) {
            Log.d(Constants.Dynamo_Test + "  Insert", "" + values);

            String key = values.getAsString("key");
            String value = values.getAsString("value");
            Node node = getNodeForHashId(Utils.genHash(key));
            if (node.nodeId.compareTo(myNode.nodeId) == 0 || node.successorOne.nodeId.compareTo(myNode.nodeId) == 0 || node.successorTwo.nodeId.compareTo(myNode.nodeId) == 0) {
                if (node.nodeId.compareTo(myNode.nodeId) == 0) {
                    FileUtils.writeToFile(key, value, getContext());
                    String msg = Constants.INSERT_REQUEST + key + "#" + value;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.successorOne.nodeId);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.successorTwo.nodeId);
                } else if (node.successorOne.nodeId.compareTo(myNode.nodeId) == 0) {
                    FileUtils.writeToFile(key, value, getContext());
                    String msg = Constants.INSERT_REQUEST + key + "#" + value;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.nodeId);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.successorTwo.nodeId);
                } else if (node.successorTwo.nodeId.compareTo(myNode.nodeId) == 0) {
                    FileUtils.writeToFile(key, value, getContext());
                    String msg = Constants.INSERT_REQUEST + key + "#" + value;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.nodeId);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.successorOne.nodeId);
                }
            } else { // does nt below to current node insert in separate node from the replicas as well
                String msg = Constants.INSERT_REQUEST + key + "#" + value;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.nodeId);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.successorOne.nodeId);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.successorTwo.nodeId);
            }
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        String portStr = Utils.getPort(getContext());
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.d(Constants.Dynamo_Test + "  onCreate Port", portStr);
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            serverSocket.setReuseAddress(true);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
        }
        createMockRing();
        Boolean is_recovered = Prefs.with(getContext()).getBoolean(Constants.IS_RECOVERED, false);

        if (is_recovered) {
            recoveryTime = System.currentTimeMillis();
            String msg = Constants.RECOVER_REQUEST + myNode.nodeId;
            for (int i = 0; i < REMOTE_PORT.length; i++) {
                if (REMOTE_PORT[i].compareTo(myPort) == 0) {
                    continue;
                }
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, String.valueOf((Integer.parseInt(REMOTE_PORT[i]) / 2)));
            }
        } else {
            Prefs.with(getContext()).save(Constants.IS_RECOVERED, true);
        }
        return false;
    }

    private void createMockRing() {
        for (String port : REMOTE_PORT) {
            int portStr = Integer.parseInt(port) / 2;
            Node node = new Node("" + portStr, Utils.genHash("" + portStr));
            ring.add(node);
            if (port.equals(myPort)) {
                myNode = node;
            }
        }
        Collections.sort(ring);
        for (int i = 0; i < NUMBER_OF_AVDS; i++) {
            Node node = ring.get(i);
            node.successorOne = ring.get((i + 1) % ring.size());
            node.successorTwo = ring.get((i + 2) % ring.size());
            node.predeccessorTwo = ring.get((i + 3) % ring.size());
            node.predeccessorOne = ring.get((i + 4) % ring.size());
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        Log.d(Constants.Dynamo_Test + "  Query", selection);

        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
        synchronized (this) {
            String[] splitQuery = selection.split("#");
            if (splitQuery.length == 1) {

                if (splitQuery[0].compareTo("*") == 0) {
                    String[] list = FileUtils.getAllFile(getContext());
                    for (int i = 0; i < list.length; i++) {
                        Message message = FileUtils.getKeyValue(getContext(), list[i]);
                        String[] values = message.value.split("\\!");
                        matrixCursor.addRow(new Object[]{list[i], values[0]});
                    }
                    for (int i = 0; i < NUMBER_OF_AVDS; i++) {
                        if (REMOTE_PORT[i].compareTo(myPort) == 0) {
                            continue;
                        }
                        String msg = Constants.QUERY_STAR_SENDING + myNode.nodeId;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, "" + (Integer.parseInt(REMOTE_PORT[i]) / 2));
                        sleep(200);
                    }
                    for (int i = 0; i < counter; i++) {
                        String whole = null;
                        try {
                            whole = blockingRequestQueue.take();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        String[] parts = whole.split("#");
                        if (parts.length > 3) {
                            for (int j = 3; j < parts.length; j = j + 2) {
                                String[] temp = parts[j + 1].split("\\!");
                                matrixCursor.addRow(new Object[]{parts[j], temp[0]});
                            }
                        }
                    }
                    counter = 0;
                } else if (splitQuery[0].compareTo("@") == 0) { // @ query
                    String[] fileList = FileUtils.getAllFile(getContext());

                    for (int i = 0; i < fileList.length; i++) {
                        String hashKey = Utils.genHash(fileList[i]);
                        Node node = getNodeForHashId(hashKey);

                        if (node.nodeId.compareTo(myNode.nodeId) == 0) {
                            Message message = FileUtils.getKeyValue(getContext(), fileList[i]);
                            String msg = Constants.QUERY_REQUEST + fileList[i];
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myNode.successorOne.nodeId);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myNode.successorTwo.nodeId);
                            String clientResponse_1 = null;
                            try {
                                clientResponse_1 = blockingQueue.take();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            String clientResponse_2 = null;
                            try {
                                clientResponse_2 = blockingQueue.take();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            String correctString = getStringMatched(message.value, clientResponse_1, clientResponse_2);
                            Log.d(Constants.Dynamo_Test, fileList[i] + "   @@@@@@@@@  " + correctString);
                            matrixCursor.addRow(new Object[]{fileList[i], correctString});
                        } else if (node.nodeId.compareTo(myNode.predeccessorOne.nodeId) == 0) {

                            Message message = FileUtils.getKeyValue(getContext(), fileList[i]);
                            String msg = Constants.QUERY_REQUEST + fileList[i];
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myNode.successorOne.nodeId);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myNode.predeccessorOne.nodeId);
                            String clientResponse_1 = null;
                            try {
                                clientResponse_1 = blockingQueue.take();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            String clientResponse_2 = null;
                            try {
                                clientResponse_2 = blockingQueue.take();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            String correctString = getStringMatched(message.value, clientResponse_1, clientResponse_2);
                            Log.d(Constants.Dynamo_Test, fileList[i] + "   @@@@@@@@@  " + correctString);
                            matrixCursor.addRow(new Object[]{fileList[i], correctString});
                        } else if (node.nodeId.compareTo(myNode.predeccessorTwo.nodeId) == 0) {

                            Message message = FileUtils.getKeyValue(getContext(), fileList[i]);
                            String msg = Constants.QUERY_REQUEST + fileList[i];
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myNode.predeccessorOne.nodeId);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myNode.predeccessorTwo.nodeId);
                            String clientResponse_1 = null;
                            try {
                                clientResponse_1 = blockingQueue.take();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            String clientResponse_2 = null;
                            try {
                                clientResponse_2 = blockingQueue.take();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            String correctString = getStringMatched(message.value, clientResponse_1, clientResponse_2);
                            Log.d(Constants.Dynamo_Test, fileList[i] + "   @@@@@@@@@  " + correctString);
                            matrixCursor.addRow(new Object[]{fileList[i], correctString});
                        }
                    }
                } else {
                    Node node = getNodeForHashId(Utils.genHash(splitQuery[0]));

                    if (node.nodeId.compareTo(myNode.nodeId) == 0 || node.successorOne.nodeId.compareTo(myNode.nodeId) == 0 || node.successorTwo.nodeId.compareTo(myNode.nodeId) == 0) {
                        if (node.nodeId.compareTo(myNode.nodeId) == 0) {
                            Message message = FileUtils.getKeyValue(getContext(), splitQuery[0]);
                            String msg = Constants.QUERY_REQUEST + splitQuery[0];
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.successorOne.nodeId);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.successorTwo.nodeId);
                            String clientResponse_1 = null;
                            try {
                                clientResponse_1 = blockingQueue.take();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            String clientResponse_2 = null;
                            try {
                                clientResponse_2 = blockingQueue.take();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            String correctString = getStringMatched(message.value, clientResponse_1, clientResponse_2);
                            Log.d(Constants.Dynamo_Test, splitQuery[0] + "   @@@@@SINGLE@@@@  " + correctString);
                            matrixCursor.addRow(new Object[]{splitQuery[0], correctString});
                        } else if (node.successorOne.nodeId.compareTo(myNode.nodeId) == 0) {

                            Message message = FileUtils.getKeyValue(getContext(), splitQuery[0]);
                            String msg = Constants.QUERY_REQUEST + splitQuery[0];
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.nodeId);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.successorTwo.nodeId);
                            String clientResponse_1 = null;
                            try {
                                clientResponse_1 = blockingQueue.take();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            String clientResponse_2 = null;
                            try {
                                clientResponse_2 = blockingQueue.take();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            String correctString = getStringMatched(message.value, clientResponse_1, clientResponse_2);
                            Log.d(Constants.Dynamo_Test, splitQuery[0] + "   @@@@SINGLE@@@@@  " + correctString);
                            matrixCursor.addRow(new Object[]{splitQuery[0], correctString});
                        } else if (node.successorTwo.nodeId.compareTo(String.valueOf(Integer.parseInt(myPort) / 2)) == 0) {
                            Message message = FileUtils.getKeyValue(getContext(), splitQuery[0]);
                            String msg = Constants.QUERY_REQUEST + splitQuery[0];
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.successorOne.nodeId);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.nodeId);
                            String clientResponse_1 = null;
                            try {
                                clientResponse_1 = blockingQueue.take();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            String clientResponse_2 = null;
                            try {
                                clientResponse_2 = blockingQueue.take();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            String correctString = getStringMatched(message.value, clientResponse_1, clientResponse_2);
                            Log.d(Constants.Dynamo_Test, splitQuery[0] + "   @@@@SINGLE@@@@@  " + correctString);
                            matrixCursor.addRow(new Object[]{splitQuery[0], correctString});
                        }
                    } else {
                        String msg = Constants.QUERY_REQUEST + splitQuery[0];
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.nodeId);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.successorOne.nodeId);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node.successorTwo.nodeId);
                        String clientResponse_1 = null;
                        String clientResponse_2 = null;
                        String clientResponse_3 = null;

                        try {
                            clientResponse_1 = blockingQueue.take();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        try {
                            clientResponse_2 = blockingQueue.take();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        try {
                            clientResponse_3 = blockingQueue.take();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        String correctString = getStringMatched(clientResponse_1, clientResponse_2, clientResponse_3);
                        Log.d(Constants.Dynamo_Test, splitQuery[0] + "   @@@@@@@@@  " + correctString);
                        matrixCursor.addRow(new Object[]{splitQuery[0], correctString});
                    }
                }
            } else {
                if (splitQuery[0].compareTo(Constants.QUERY) == 0) {
                    if (splitQuery[1].compareTo("*") == 0) {
                        String[] list = FileUtils.getAllFile(getContext());
                        String msg = Constants.QUERY_STAR_RETURN;
                        for (int i = 0; i < list.length; i++) {
                            Message message = FileUtils.getKeyValue(getContext(), list[i]);
                            msg += "#" + list[i] + "#" + message.value.split("\\!")[0];
                        }
                        Log.d(Constants.Dynamo_Test, splitQuery[0] + "   @@@@starReturn@@@@@  " + msg);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, splitQuery[3]);
                    }
                }
            }
        }
        return matrixCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while (true) {
                Socket ss;
                try {
                    ss = serverSocket.accept();
                    ObjectInputStream is = new ObjectInputStream(ss.getInputStream());
                    ObjectOutputStream os = new ObjectOutputStream(ss.getOutputStream());
                    String read = (String) is.readObject();
                    Log.d(Constants.Dynamo_Test, "ServerTask" + read);
                    String response = "ACK";

                    if (read.startsWith(Constants.QUERY_REQUEST)) {
                        String[] splitMessage = read.split("#");
                        Message message = FileUtils.getKeyValue(getContext(), splitMessage[2]);
                        response = message.value;
                    } else if (read.contains(Constants.RECOVER_REQUEST)) {
                        String data = Constants.RECOVER_RESPONSE;
                        String[] request = read.split("#");
                        String nodeIdFrom = request[1];
                        Node node = getNode(nodeIdFrom);
                        String[] fileList = getContext().fileList();

                        if (node.successorOne.nodeId.compareTo(myNode.nodeId) == 0 || node.successorTwo.nodeId.compareTo(myNode.nodeId) == 0) {
                            for (int i = 0; i < fileList.length; i++) {
                                String hashKey = Utils.genHash(fileList[i]);
                                if ((node.nodeId.compareTo(ring.get(0).nodeId) == 0 && hashKey.compareTo(ring.get(0).hashId) <= 0)
                                        || (node.nodeId.compareTo(ring.get(0).nodeId) == 0 && hashKey.compareTo(ring.get(4).hashId) > 0)
                                        || (hashKey.compareTo(node.predeccessorOne.hashId) > 0 && hashKey.compareTo(node.hashId) <= 0)) {
                                    Message message = FileUtils.getKeyValue(getContext(), fileList[i]);
                                    data += "#" + message.key + "#" + message.value;
                                }
                            }
                        } else if (node.predeccessorOne.nodeId.compareTo(myNode.nodeId) == 0 || node.predeccessorTwo.nodeId.compareTo(myNode.nodeId) == 0) {
                            for (int i = 0; i < fileList.length; i++) {
                                String hashKey = Utils.genHash(fileList[i]);
                                if ((myNode.nodeId.compareTo(ring.get(0).nodeId) == 0 && hashKey.compareTo(ring.get(0).hashId) <= 0)
                                        || (myNode.nodeId.compareTo(ring.get(0).nodeId) == 0 && hashKey.compareTo(ring.get(4).hashId) > 0)
                                        || (hashKey.compareTo(myNode.predeccessorOne.hashId) > 0 && hashKey.compareTo(myNode.hashId) <= 0)) {
                                    Message message = FileUtils.getKeyValue(getContext(), fileList[i]);
                                    data += "#" + message.key + "#" + message.value;
                                }
                            }
                        }
                        response = data;
                    }

                    os.writeObject(response);
                    is.close();
                    os.flush();
                    os.close();
                    // closing this so that the client task does remain open and keep waiting for the server to respond
                    // no waiting

                    if (read.startsWith(Constants.INSERT_REQUEST)) {
                        String[] splitMessage = read.split("#");
                        String key = splitMessage[1];
                        String value = splitMessage[2];
                        FileUtils.writeToFile(key, value, getContext());
                    } else if (read.startsWith(Constants.DELETE_REQUEST)) {
                        String[] splitMessage = read.split("#");
                        FileUtils.deleteFile(getContext(), splitMessage[1]);
                    } else if (read.startsWith(Constants.QUERY_STAR)) {
                        String[] splitMessage = read.split("#");
                        if (splitMessage[2].compareTo(Constants.SENDING) == 0) {
                            query(Constants.getUri(), null, read, null, null);
                        } else {
                            blockingRequestQueue.put(read);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            int sendingPort = Integer.parseInt(String.valueOf(Integer.parseInt(msgs[1]) * 2));
            Log.d("sending port", String.valueOf(sendingPort));
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), sendingPort);
                socket.setSoTimeout(750);
                ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream is = new ObjectInputStream(socket.getInputStream());
                String msgToSend = msgs[0];
                os.writeObject(msgToSend);
                String read = (String) is.readObject();
                Log.d(Constants.Dynamo_Test, read);
                if (msgs[0].contains(Constants.QUERY_STAR)) {
                    String[] splitMessage = msgs[0].split("#");
                    if (splitMessage[2].compareTo(Constants.SENDING) == 0) {
                        counter++;
                    }
                } else if (msgs[0].contains(Constants.QUERY_REQUEST)) {
                    if (read != null) {
                        blockingQueue.put(read);
                    } else {
                        blockingQueue.put("" + System.currentTimeMillis());
                    }
                } else if (msgs[0].startsWith(Constants.RECOVER_REQUEST)) {
                    String[] response = read.split("#");
                    if (response.length > 1) {
                        for (int j = 1; j < response.length; j = j + 2) {
                            String key = response[j];
                            String value = response[j + 1];
                            String[] values = value.split("\\!");

                            Message message = FileUtils.getKeyValue(getContext(), key);
                            String[] messageValues = message.value.split("\\!");

                            if (values.length == 2 && messageValues.length == 2) {
                                Log.d(Constants.TESTING_RECOVERY, key + " found  inside  " + value);
                                Log.d(Constants.TESTING_RECOVERY, message.key + " found  inside  " + message.value);

                                if (Long.parseLong(messageValues[1]) > Long.parseLong(values[1])) {
                                    FileUtils.writeToFileWithOutTimeStamp(message.key, message.value, getContext());
                                } else {
                                    FileUtils.writeToFileWithOutTimeStamp(key, value, getContext());
                                }
                            } else if (values.length == 2) {
                                Log.d(Constants.TESTING_RECOVERY, message.key + " bad results  " + message.value);
                                FileUtils.writeToFileWithOutTimeStamp(key, value, getContext());
                            } else {
                                Log.d(Constants.TESTING_RECOVERY, message.key + " bad results  " + message.value);
                                FileUtils.writeToFileWithOutTimeStamp(message.key, message.value, getContext());
                            }
                        }
                    }
                }
                os.flush();
                os.close();
                is.close();
                socket.close();
            } catch (Exception e) {
                if (msgs[0].contains(Constants.QUERY_REQUEST)) {
                    try {
                        blockingQueue.put("" + System.currentTimeMillis());
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
                e.printStackTrace();
            }
            return null;
        }

    }


    public Node getNodeForHashId(String hashId) {
        for (Node node : this.ring) {
            if (hashId.compareTo(node.hashId) <= 0 && hashId.compareTo(node.predeccessorOne.hashId) > 0) {
                return node;
            } else if ((hashId.compareTo(node.predeccessorOne.hashId) > 0 && hashId.compareTo(node.hashId) > 0 && ring.indexOf(node) == 0)
                    || (hashId.compareTo(node.predeccessorOne.hashId) < 0 && hashId.compareTo(node.hashId) < 0 && ring.indexOf(node) == 0)) {
                {
                    return node;
                }
            }
        }
        return null;
    }

    public void sleep(long milli) {
        try {
            Thread.sleep(milli);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String getStringMatched(String message_1, String message_2, String message_3) { // for handling failure the correct message, value comparator
        String key_to_query = null;
        String[] message_11_list = message_1.split("\\!");
        String[] message_22_list = message_2.split("\\!");
        String[] message_33_list = message_3.split("\\!");
        message_1 = message_11_list[0];
        message_2 = message_22_list[0];
        message_3 = message_33_list[0];

        if (message_1.compareTo(message_2) == 0) {
            key_to_query = message_1;
        } else if (message_1.compareTo(message_3) == 0) {
            key_to_query = message_1;
        } else if (message_2.compareTo(message_3) == 0) {
            key_to_query = message_2;
        }
        return key_to_query;
    }

    public Node getNode(String port) {
        for (Node node : this.ring) {
            if (node.nodeId.equals(port)) {
                return node;
            }
        }
        return null;
    }
}