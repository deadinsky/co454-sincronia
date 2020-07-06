import com.google.common.primitives.Booleans;
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
    private ArrayList<ArrayList<Job>> schedules = new ArrayList<>();
    private ArrayList<Integer> ingresses = new ArrayList<>();
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

    public ArrayList<ArrayList<Job>> calculateSchedules(ArrayList<ArrayList<Job>> localSchedules,
                                                        Integer[] localIngresses, Integer[] localIds,
                                                        String[] localEgresses) {
        Map<Integer, Integer> idDict = new HashMap<>();
        Map<String, Integer> egressDict = new HashMap<>();
        EpsilonFraction weights[] = new EpsilonFraction[localIds.length];
        int jobDurations[] = new int[localIds.length];
        int jobDurationsEps[] = new int[localIds.length];
        int jobOrdering[] = new int[localIds.length];
        boolean isScheduled[] = new boolean[localIds.length];
        int ingressTimeUnits[] = new int[localIngresses.length];
        int ingressEpsilons[] = new int[localIngresses.length];

        for (int i = 0; i < localIds.length; i++) {
            idDict.put(localIds[i], i);
            weights[i] = EpsilonFraction.one;
            isScheduled[i] = false;
        }
        for (int i = 0; i < localEgresses.length; i++) {
            egressDict.put(localEgresses[i], i);
        }
        for (int n = localIds.length; n > 0; n--) {
            for (int i = 0; i < localIngresses.length; i++) {
                ingressTimeUnits[i] = 0;
                ingressEpsilons[i] = 0;
            }
            //determine what jobs remaining are at each port
            int index = 0;
            for (ArrayList<Job> schedule : localSchedules) {
                for (Job job : schedule) {
                    if (!isScheduled[idDict.get(job.id)]) {
                        ingressTimeUnits[index] += job.timeUnits;
                        ingressEpsilons[index] += job.epsilon;
                    }
                }
                index++;
            }
            //determine which port is most bottlenecked
            int maxValue = 0;
            int maxValueEps = 0;
            int maxIngressIndex = -1; //b
            for (int i = 0; i < localIngresses.length; i++) {
                if (maxIngressIndex == -1 || ingressTimeUnits[i] > maxValue || (ingressTimeUnits[i] == maxValue && (
                        ingressEpsilons[i] > maxValueEps || (ingressEpsilons[i] == maxValueEps &&
                        localIngresses[i] >= localIngresses[maxIngressIndex])))) {
                    maxValue = ingressTimeUnits[i];
                    maxValueEps = ingressEpsilons[i];
                    maxIngressIndex = i;
                }
            }
            //now figure out the durations of each job at specified port
            for (int i = 0; i < localIds.length; i++) {
                jobDurations[i] = 0;
                jobDurationsEps[i] = 0;
            }
            for (Job job : localSchedules.get(maxIngressIndex)) {
                if (!isScheduled[idDict.get(job.id)]) {
                    jobDurations[idDict.get(job.id)] += job.timeUnits;
                    jobDurationsEps[idDict.get(job.id)] += job.epsilon;
                }
            }
            //find weighted largest job
            EpsilonFraction minValue = null;
            int minJobIndex = -1;
            for (int i = 0; i < localIds.length; i++) {
                if (jobDurations[i] > 0 || (jobDurations[i] == 0 && jobDurationsEps[i] > 0)) {
                    EpsilonFraction curValue = EpsilonFraction.divideFractions(weights[i],
                            new EpsilonFraction(jobDurations[i], jobDurationsEps[i]));
                    if (!isScheduled[i] && (minValue == null || minValue.isGreater(curValue))) {
                        minValue = curValue;
                        minJobIndex = i;
                    }
                }
            }
            //schedule said job last
            isScheduled[minJobIndex] = true;
            jobOrdering[n - 1] = minJobIndex;
            //update weights
            weights[minJobIndex] = EpsilonFraction.zero;
            for (int i = 0; i < localIds.length; i++) {
                if (!isScheduled[i] && jobDurations[i] > 0) {
                    weights[i] = EpsilonFraction.subtractFractions(weights[i],
                            EpsilonFraction.multiplyFractions(minValue, jobDurations[i]));
                }
            }
        }
        String tmp = "Order of jobs: ";
        for (int i = 0; i < localIds.length; i++) {
            tmp += localIds[jobOrdering[i]] + "; ";
        }
        log.info(tmp);

        int jobOrderingInverse[] = new int[localIds.length];
        EpsilonFraction ingressTimes[] = new EpsilonFraction[localIngresses.length];
        EpsilonFraction egressTimes[] = new EpsilonFraction[localEgresses.length];
        ArrayList<ArrayList<Job>> beNodeSchedules = new ArrayList<>();
        ArrayList<Integer> nextIngress = new ArrayList<>();

        Comparator<Job> jobComparator = new Comparator<Job>() {
            @Override
            public int compare(Job x, Job y) {
                return (jobOrderingInverse[idDict.get(x.id)] == jobOrderingInverse[idDict.get(y.id)] ?
                        y.egress.compareTo(x.egress) : //if tied, favour later egress
                        jobOrderingInverse[idDict.get(x.id)] - jobOrderingInverse[idDict.get(y.id)]);
            }
        };

        Comparator<Integer> ingressComparator = new Comparator<Integer>() {
            @Override
            public int compare(Integer x, Integer y) {
                return (jobOrderingInverse[idDict.get(localSchedules.get(x).get(0).id)] ==
                        jobOrderingInverse[idDict.get(localSchedules.get(y).get(0).id)] ?
                        localIngresses[y] - localIngresses[x] : //if tied, favour later ingress
                        jobOrderingInverse[idDict.get(localSchedules.get(x).get(0).id)] -
                        jobOrderingInverse[idDict.get(localSchedules.get(y).get(0).id)]);
            }
        };
        
        for (int i = 0; i < localIds.length; i++) {
            jobOrderingInverse[jobOrdering[i]] = i;
        }
        for (int i = 0; i < localIngresses.length; i++) {
            ingressTimes[i] = EpsilonFraction.zero;
            if (!localSchedules.get(i).isEmpty()) {
                localSchedules.get(i).sort(jobComparator);
                nextIngress.add(i);
            }
        }
        nextIngress.sort(ingressComparator);
        for (int i = 0; i < localEgresses.length; i++) {
            egressTimes[i] = EpsilonFraction.zero;
            beNodeSchedules.add(new ArrayList<Job>());
        }
        //while there are still ingresses busy
        while (!nextIngress.isEmpty()) {
            int ingressIndex = nextIngress.get(0);
            ArrayList<Job> ingressSchedule = localSchedules.get(ingressIndex);
            Integer nextEgressIndex = egressDict.get(ingressSchedule.get(0).egress);
            boolean jobFound = false;
            //find a job from the next available ingress that has an open egress
            for (int i = 0; !jobFound && i < ingressSchedule.size(); i++) {
                if (!egressTimes[egressDict.get(ingressSchedule.get(i).egress)].isGreater(ingressTimes[ingressIndex])) {
                    //ingress and egress will now be free at the same time
                    ingressTimes[ingressIndex] = EpsilonFraction.addFractions(ingressTimes[ingressIndex],
                            new EpsilonFraction(ingressSchedule.get(i).timeUnits, ingressSchedule.get(i).epsilon));
                    egressTimes[egressDict.get(ingressSchedule.get(i).egress)] = ingressTimes[ingressIndex];
                    beNodeSchedules.get(egressDict.get(ingressSchedule.get(i).egress)).add(ingressSchedule.get(i));
                    ingressSchedule.remove(i);
                    jobFound = true;
                //record the next available egress, just in case none are currently available
                } else if (egressTimes[nextEgressIndex].isGreater(
                        egressTimes[egressDict.get(ingressSchedule.get(i).egress)])) {
                    nextEgressIndex = egressDict.get(ingressSchedule.get(i).egress);
                }
            }
            //if no applicable egress is available, let the job wait until there is one
            if (!jobFound) {
                ingressTimes[ingressIndex] = egressTimes[nextEgressIndex];
            }
            //if no jobs are left for the ingress, remove the index, otherwise add it back in line
            if (localSchedules.get(ingressIndex).isEmpty()) {
                nextIngress.remove(0);
            } else {
                nextIngress.remove(0);
                int swapIndex = 0;
                while (swapIndex != -1) {
                    if (swapIndex == nextIngress.size() ||
                            ingressTimes[nextIngress.get(swapIndex)].isGreater(ingressTimes[ingressIndex]) || (
                            ingressTimes[nextIngress.get(swapIndex)].isEqual(ingressTimes[ingressIndex]) &&
                            jobOrderingInverse[idDict.get(localSchedules.get(ingressIndex).get(0).id)] <=
                            jobOrderingInverse[idDict.get(localSchedules.get(nextIngress.get(swapIndex)).get(0).id)])) {
                        nextIngress.add(swapIndex, ingressIndex);
                        swapIndex = -1;
                    } else {
                        swapIndex++;
                    }
                }
            }
        }
        return beNodeSchedules;
    }

    public int sendJobs(List<Job> schedule, int ingress) throws org.apache.thrift.TException {
        ArrayList<ArrayList<Job>> localSchedules;
        boolean areJobsLoaded = false;
        Integer localIngresses[];
        Integer localIds[];
        String localEgresses[];
        ArrayList<Job> fixedSchedule = new ArrayList<Job>();
        for (Job job : schedule) {
            fixedSchedule.add(job);
        }
        synchronized (this) {
            for (Job job : schedule) {
                ids.add(job.id);
                egresses.add(job.egress);
            }
            schedules.add(fixedSchedule);
            ingresses.add(ingress);
            areJobsLoaded = (schedules.size() == numClients);
            if (!areJobsLoaded) {
                return 0;
            }
            localSchedules = new ArrayList<>();
            localSchedules.addAll(schedules);
            localIngresses = ingresses.toArray(new Integer[0]);
            localIds = ids.toArray(new Integer[0]);
            Arrays.sort(localIds);
            localEgresses = egresses.toArray(new String[0]);
            Arrays.sort(localEgresses);
            schedules.clear();
            ingresses.clear();
            ids.clear();
            egresses.clear();
        }
        if (areJobsLoaded) {
            try {
                ArrayList<ClientContainer> clients = loadBalancer.getBalancedLoad(localEgresses.length);
                log.info("Calling " + clients.size() + " backend node thread(s)");
                if (localEgresses.length != clients.size()) {
                    log.info("The number of BENodes expected is more than what's available." + localEgresses.length);
                }

                ArrayList<ArrayList<Job>> beNodeSchedules = calculateSchedules(localSchedules,
                        localIngresses, localIds, localEgresses);

                for (int i = 0; i < clients.size(); i++) {
                    ClientContainer clientContainer = clients.get(i);
                    log.debug("Locked client " + clientContainer.id + " for processing");
                    //rearm the joining barrier
                    clientContainer.countDownLatch = new CountDownLatch(1);
                    if (!clientContainer.client.hasError()) {
                        clientContainer.client.sendJobs((List<Job>)beNodeSchedules.get(i), localIngresses[i],
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
