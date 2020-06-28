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

    public synchronized ArrayList<ClientContainer> getBalancedLoad(int egressesSize) throws InterruptedException {
        ArrayList<ClientContainer> ret = new ArrayList<>();
        if (clients.isEmpty()) return ret;
        for (int i = 0; i < Math.min(egressesSize, clients.size()); i++) {
            addClient(ret);
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
