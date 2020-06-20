import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RoundRobinLoadBalancer {
    static int MAX_ATTEMPTS = 5;
    private CircularLinkedList clients;
    int index = 0;
    public RoundRobinLoadBalancer(CircularLinkedList clients){
        this.clients = clients;
    }

    public synchronized ArrayList<ClientContainer> getBalancedLoad(int schedulesSize) throws InterruptedException {
        //basically we just want to go through the list
        ArrayList<ClientContainer> ret = new ArrayList<>();
        if (clients.isEmpty()) return ret;
        int scheduleCost = 0;
        //divide the work into the number of clients and return
        if (schedulesSize > clients.size()){
            //we dont have enough threads to spread this across, also use fenode as a thread
            int perClient = (int) Math.ceil((double) schedulesSize / (clients.size() + 1));
            //in rare cases (e.g. n clients and n+1 tasks), rounding up the per client could assign useless clients
            //so extra check in for loop is in place for this reason
            for (int i = 0; i < clients.size() && (perClient * i) < schedulesSize; i++){
                addClient(ret);
            }
        } else {
            for (int i = 0; i < schedulesSize; i++) {
                addClient(ret);
            }
        }
        if(clients.size() == 0){
            ret.clear();
        }
        return ret;
    }

    private void addClient(ArrayList<ClientContainer> ret) throws InterruptedException {
        boolean found = false;
        int currTries = 0;
        while(!found &&  currTries < MAX_ATTEMPTS &&clients.size() > 0) {
            ClientContainer client = clients.next();

            if (client.lock.tryAcquire(100, TimeUnit.MILLISECONDS)) {
                ret.add(client);
                found = true;
            } else {
                currTries++;
            }
        }
    }

}
