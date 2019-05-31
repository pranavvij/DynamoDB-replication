package edu.buffalo.cse.cse486586.simpledynamo;


class Node implements Comparable<Node> {
    String nodeId;
    String hashId;

    Node successorOne;
    Node successorTwo;
    Node predeccessorOne;
    Node predeccessorTwo;

    Node(String nodeId, String hashId) {
        this.nodeId = nodeId;
        this.hashId = hashId;
        successorOne = this;
        successorTwo = this;
        predeccessorOne = this;
        predeccessorTwo = this;
    }

    @Override
    public int compareTo(Node another) {

        return this.hashId.compareTo(another.hashId);
    }
}
