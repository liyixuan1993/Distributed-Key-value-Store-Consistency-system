package com.company;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
/*
client read config and store the info of all the replicas in List<Node> nodeList, Node has replica's
id,ip,port. we need to indicate which replica we want to connect, so we also need to store serverId
and client id itself,so that we can get the response from the server. Also we need a socket to 
store the socket between client and server.

then we read the cmd operation, we first connect the server,here a single replica(serverID) might 
crash(we will just kill the replica's process),and your system should continue working correctly. 
Any clients connected to the crashed replica should then connect to the replica with the next 
higher ID. so we should first find the index of serverID in nodeList,and then try connect the server
if fails,we increment the index,and connect the next server. Also,after connected,we send our id
to the replica,so the replica can send response to us. Finally,we send the cmd to the replica,
and wait for the response.

Once connected, it should accept on
stdin the following commands:
• put var_name value
• get var_name
• delay milliseconds
• dump
put tells the replica to write the value to the variable. get request the variable's value from
the replica, and should print the result to stdout of that replica. delay should pause the client's 
execution for the specified duration.dump should instruct the replica to dump its current state
for all variables (not a consistent read operation, just its own data) to the replica's stdout

we use arraylist nodelist here because we need to guarantee the fault tolarence(if replica crash
go to next replica),so we need order and do not choose hashmap. 
*/
public class Client
{
    private static class Node
    {
        public final String id;
        public final String ip;
        public final int port;

        private Node(String id, String ip, int port)
        {
            this.id = id;
            this.ip = ip;
            this.port = port;
        }
    }

    private ObjectInputStream objectInputStream;
    private ObjectOutputStream objectOutputStream;
    private Socket socket;

    private List<Node> nodeList = new ArrayList<>();
    private String id;
    private String serverId;

    // params: <config-path> <client-id> <server-id>
    public static void main(String[] args)
    {
        if(args.length != 3) {
            System.out.println("usage: client <config-path> <client-id> <server-id>");
        }
        try {
            new Client(args[0], args[1], args[2]).mainLoop();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Client(String configPath, String id, String serverId)
    {
        try {
            if (loadConfiguration(configPath)) {
                this.id = id;
                this.serverId = serverId;
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean loadConfiguration(String filename)
            throws IOException
    {
        File file = new File(filename);
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] stringList = line.split(" ");
                nodeList.add(new Node(stringList[0], stringList[1], Integer.parseInt(stringList[2])));
            }
        }
        return true;
    }

    public void mainLoop()
            throws IOException
    {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String line;
        while ((line = br.readLine()) != null) {
            //System.out.println("hehe "+ line);
            executeCommand(line.trim());
        }
    }

    private void executeCommand(String cmd)
    {
        String[] results = cmd.split("\\s+", 2);
        if (results.length < 1) {
            return;
        }

        if (results[0].equals("dump")) {
            connectAndRequest("dump"); //just let the replica print out info,client do nothing
        }
        else if (results[0].equals("get")) {
            String varName = results[1];
            String response = connectAndRequest("get:" + varName);
            if(response != null) {
                System.out.println(varName + " = " + response);
            } else {
                System.out.println("failed!");
            }
        }
        else if (results[0].equals("delay")) {
            long ms = Long.parseLong(results[1]);
            try {
                Thread.sleep(ms);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        else if (results[0].equals("put")) {
            results = cmd.split("\\s+", 3);
            if (results.length >= 2) {
                String varName = results[1];
                String value = results[2];
                String response = connectAndRequest("put:" + varName + ":" + value);
                if(response != null) {
                    System.out.println("success!");
                } else {
                    System.out.println("failed!");
                }
            }
        }
    }

    private void connectServer(int index) {
        try {
            socket = new Socket(nodeList.get(index).ip, nodeList.get(index).port);
            objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            objectInputStream = new ObjectInputStream(socket.getInputStream());
            objectOutputStream.writeObject(id);
        }
        catch (IOException e) {
            socket = null;
            //e.printStackTrace();
        }
    }

    private void tryConnect() {
        if (socket == null) {
            int retry = 0;
            int len = nodeList.size();
            int index = 0;
            while (!nodeList.get(index).id.equals(serverId)) {
                index++;
            }
            while (retry < len && socket == null) {
                connectServer(index);
                retry++;
                index = (index + 1) % len;
            }
        }
    }

    private String connectAndRequest(String cmd)
    {
        tryConnect();
        if (socket != null) {
            try {
                objectOutputStream.writeObject(cmd);
            }
            catch (IOException e) {
                socket = null;
                tryConnect();
                if(socket != null) {
                    try {
                        objectOutputStream.writeObject(cmd);
                    }
                    catch (IOException e1) {
                        e1.printStackTrace();
                    }
                }
            }
            try {
                String response = (String) objectInputStream.readObject();
                return response;
            }
            catch (IOException e) {
                e.printStackTrace();
                return null;
            }
            catch (ClassNotFoundException e) {
                e.printStackTrace();
                return null;
            }
        }
        return null;
    }
}
