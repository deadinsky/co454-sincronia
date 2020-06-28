import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class FENodeServiceHandler implements SincroniaService.Iface {
    private CircularLinkedList backEnds = new CircularLinkedList();
    private ArrayList<SyncClient> pingClients = new ArrayList<>();
    private RoundRobinLoadBalancer loadBalancer = new RoundRobinLoadBalancer(backEnds);
    private Timer timer = new Timer();
    private int numClients = 0;
    private List<List<Job>> schedules = new ArrayList<>();
    private LinkedHashSet<Integer> ids = new LinkedHashSet<Integer>();
    private LinkedHashSet<String> egresses = new LinkedHashSet<String>();

    public FENodeServiceHandler(){
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                Iterator<SyncClient> itt = pingClients.iterator();
                while (itt.hasNext()) {
                    SyncClient pingClient = itt.next();
                    try {
                        pingClient.client.ping();
                    } catch (TException t){
                        log.info("Backend went down on: " + pingClient.host + ":" + pingClient.port);
                        backEnds.delete(pingClient.host, pingClient.port);
                        itt.remove();
                    }
                }
            }
        };
        //just ping them all 5 times a second, note it will only ping a backend once not each client
        timer.scheduleAtFixedRate(timerTask, 200, 200);
    }
    static Logger log = Logger.getLogger(FENodeServiceHandler.class.getName());

    public ArrayList<List<Job>> calculateSchedules(List<List<Job>> localSchedules, Integer[] localIds,
                                                         String[] localEngresses) {
        ArrayList<List<Job>> beNodeSchedules = new ArrayList<>();
        for (int i = 0; i < localEngresses.length; i++) {
            beNodeSchedules.add(new ArrayList<>());
        }
        int i = 0;
        for (List<Job> schedule : localSchedules) {
            for (Job job : schedule) {
                beNodeSchedules.get(i).add(job);
                i = (i + 1) % localEngresses.length;
            }
        }
        return beNodeSchedules;
    }

    public int sendJobs(List<Job> schedule) throws org.apache.thrift.TException {
        List<List<Job>> localSchedules;
        boolean areJobsLoaded = false;
        String localEgresses[];
        Integer localIds[];
        for (Job job : schedule) {
            ids.add(job.id);
            egresses.add(job.egress);
        }
        synchronized (this) {
            schedules.add(schedule);
            areJobsLoaded = (schedules.size() == numClients);
            if (!areJobsLoaded) {
                return 0;
            }
            localSchedules = new ArrayList<>();
            localSchedules.addAll(schedules);
            localIds = ids.toArray(new Integer[0]);
            localEgresses = egresses.toArray(new String[0]);
            schedules.clear();
            ids.clear();
            egresses.clear();
        }
        if (areJobsLoaded) {
            try {
                ArrayList<ClientContainer> clients = loadBalancer.getBalancedLoad(localEgresses.length);
                log.info("Calling " + clients.size() + " backend node thread(s)");
                if (localEgresses.length != clients.size()) {
                    log.info("The number of BENodes expected is more than what's available.");
                }

                ArrayList<List<Job>> beNodeSchedules = calculateSchedules(localSchedules,
                        localIds, localEgresses);

                for (int i = 0; i < clients.size(); i++) {
                    ClientContainer clientContainer = clients.get(i);
                    log.debug("Locked client " + clientContainer.id + " for processing");
                    //rearm the joining barrier
                    clientContainer.countDownLatch = new CountDownLatch(1);
                    if (!clientContainer.client.hasError()) {
                        clientContainer.client.sendJobs(beNodeSchedules.get(i),
                                new SendJobsCallback(clientContainer, backEnds));
                    } else {
                        clientContainer.countDownLatch.countDown();
                    }
                }

                for (ClientContainer client : clients) {
                    client.countDownLatch.await();
                    log.debug("Unlocked client " + client.id + " from processing");
                    client.lock.release();
                }
                return 0;
            } catch (Exception e) {
                throw new IllegalArgument(e.getMessage());
            }
        }
        return 0;
    }

    public void setClients(int numClient) throws org.apache.thrift.TException {
        synchronized(this) {
            numClients = numClient;
        }
    }

    //goal of this is to communicate to the frontend that a backend node exists and set the backend node up
    @Override
    public void initialize(String hostname, int port, int numThreads) throws TException {

        TNonblockingTransport trans;
        TAsyncClientManager ca;

        TProtocolFactory protocol = new TBinaryProtocol.Factory();
        SincroniaService.AsyncClient client;
        for (int i = 0; i < numThreads; i++) {
            try {
                ca = new TAsyncClientManager();
                trans = new TNonblockingSocket(hostname, port);
            } catch (IOException e) {
                e.printStackTrace();
                throw new TException(e);
            }
            client = new SincroniaService.AsyncClient(protocol, ca, trans);
            backEnds.add(new ClientContainer(client,hostname,port));
        }
        //this is thrift initialization for a client
        TSocket sock = new TSocket(hostname,port);
        TTransport transport = new TFramedTransport(sock);
        TProtocol syncProt = new TBinaryProtocol(transport);
        SincroniaService.Client syncClient = new SincroniaService.Client(syncProt);
        transport.open();
        pingClients.add(new SyncClient(syncClient, transport, hostname, port));
        log.info("Added client " + hostname + ":" + port + " with " + numThreads + " thread(s).");
    }

    @Override
    public void ping() throws TException {/*noop*/}
}
