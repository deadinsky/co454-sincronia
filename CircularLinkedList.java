import java.util.Objects;

/* Original Author: Matthew Belisle, 2020 */
public class CircularLinkedList {
    //Represents the node of list.
    private int size = 0;
    private Node curr;


    public class Node {
        ClientContainer data;
        Node next;
        public Node(ClientContainer data) {
            this.data = data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node node = (Node) o;
            return Objects.equals(data, node.data);
        }

        @Override
        public int hashCode() {
            return Objects.hash(data);
        }
    }

    //Declaring head and tail pointer as null.
    public Node head = null;
    public Node tail = null;

    //This function will add the new node at the end of the list.
    public synchronized void add(ClientContainer data) {
        //Create new node
        Node n = new Node(data);
        //Checks if the list is empty.
        if (head == null) {
            head = n;
            tail = n;
            n.next = head;
            curr = head;
        } else {
            //tail will point to new node.
            tail.next = n;
            //New node will become new tail.
            tail = n;
            //Since, it is circular linked list tail will point to head.
            tail.next = head;
        }
        size++;
    }
    public boolean isEmpty(){
        return size == 0;
    }

    public int size(){
        return size;
    }

    //this will delete all clients like the clientContainer from the list
    //like meaning same host/port combo
    public synchronized void delete(String host, int port){
        Boolean onceThrough = false;
        if(isEmpty()){
            curr = null;
            return;
        }
        Node first;
        Node prev = tail;
        do {
            first = prev.next;
            if(first.equals(tail)) onceThrough = true;
            ClientContainer headData = first.data;
            if(headData.port == port && headData.host.equals(host)){

                //remove it
                prev.next = first.next;
                if(first.equals(head)) head = head.next;
                if(first.equals(tail)) tail = prev;
                // we also need to free the threads that are waiting on them as they won't succeed to expedite the process
                if(first.data.countDownLatch != null) {
                    while(first.data.countDownLatch.getCount() > 0) {
                        first.data.countDownLatch.countDown();
                    }
                    while(first.data.lock.hasQueuedThreads()){
                        //this will effectively give each thing trying to acquire the client the go ahead to take it
                        //although not the most efficient as each one will error this will in the end work
                        first.data.lock.release();
                    }
                }

                // move curr up
                if(curr.equals(first)){
                    curr = first.next;
                }
                size--;

            }
            if(isEmpty()){
                tail = null;
                head = null;
                //will break the loop
                curr = null;
            }
            //cannot just set to first as it may have been deleted
            prev = prev.next;
        }while(!onceThrough);
    }
    public synchronized void delete(ClientContainer c){
       delete(c.host, c.port);
    }

    public synchronized ClientContainer next(){
        if(curr == null || isEmpty()){
            curr = null;
            return null;
        } else {
            ClientContainer ret = curr.data;
            curr = curr.next;
            return ret;
        }
    }
}
