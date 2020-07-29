import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class CalculateSchedules {
    static Logger log = Logger.getLogger(CalculateSchedules.class.getName());
    public static ArrayList<ArrayList<Job>> parseSchedules(String fileName) {
        ArrayList<ArrayList<Job>> schedules = new ArrayList<>();
        int ingressLength = 0;
        Map<String, Integer> ingressDict = new HashMap<>();
        File scheduleFile = new File(fileName);
        Integer numClients = 0;
        try {
            Scanner scheduleReader = new Scanner(scheduleFile);
            String firstLine[] = scheduleReader.nextLine().split(" ");
            numClients = Integer.valueOf(firstLine[0]);
            for (int i = 0; i < numClients; i++) {
                schedules.add(new ArrayList<Job>());
            }
            //formatting is determined by sincronia schedule generator
            while (scheduleReader.hasNextLine()) {
                String currentLine[] = scheduleReader.nextLine().split(" ");
                int coflowId = Integer.parseInt(currentLine[0]);
                int releaseTime = Integer.parseInt(currentLine[1]);
                for (int i = 5; i < currentLine.length; i += 3) {
                    String ingress = currentLine[i];
                    int ingressIndex = ingressDict.getOrDefault(ingress, -1);
                    if (ingressIndex == -1) {
                        ingressIndex = ingressLength;
                        ingressDict.put(ingress, ingressLength);
                        ingressLength++;
                    }
                    String egress = currentLine[i+1];
                    int timeUnits = 0;
                    int epsilon = 0;
                    if (currentLine[i+2].contains("e")) {
                        String timeSplit[];
                        String epsilonString;
                        boolean isNegative = false;
                        if (currentLine[i+2].contains("-")) {
                            timeSplit = currentLine[i+2].split("-");
                            timeUnits = Integer.valueOf(timeSplit[0]);
                            epsilonString = timeSplit[1];
                            isNegative = true;
                        } else if (currentLine[i+2].contains("+")) {
                            timeSplit = currentLine[i+2].split("\\+");
                            timeUnits = Integer.valueOf(timeSplit[0]);
                            epsilonString = timeSplit[1];
                        } else {
                            epsilonString = currentLine[i+2];
                        }
                        if (epsilonString.length() == 1) {
                            epsilon = (isNegative ? -1 : 1);
                        } else {
                            epsilon = Integer.valueOf(epsilonString.substring(0, epsilonString.length() - 1)) * (isNegative ? -1 : 1);
                        }
                    } else {
                        timeUnits = Integer.valueOf(currentLine[i+2]);
                    }
                    schedules.get(ingressIndex).add(new Job(coflowId, egress, 1, releaseTime, timeUnits, epsilon));
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        if (numClients != schedules.size()) {
            System.err.println("Incorrect input file format, client size discrepancy.");
            System.exit(-1);
        }
        return schedules;
    }

    public static ArrayList<ArrayList<Job>> calculateSchedules(ArrayList<ArrayList<Job>> localSchedules,
                                                        Integer[] localIngresses, Integer[] localIds,
                                                        String[] localEgresses) {
        Map<Integer, Integer> idDict = new HashMap<>();
        Map<String, Integer> egressDict = new HashMap<>();
        EpsilonFraction weights[] = new EpsilonFraction[localIds.length];
        EpsilonFraction releaseTimes[] = new EpsilonFraction[localIds.length];
        int jobDurations[] = new int[localIds.length];
        int jobDurationsEps[] = new int[localIds.length];
        int jobOrdering[] = new int[localIds.length];
        boolean isScheduled[] = new boolean[localIds.length];
        int ingressTimeUnits[] = new int[localIngresses.length];
        int ingressEpsilons[] = new int[localIngresses.length];

        for (int i = 0; i < localIds.length; i++) {
            idDict.put(localIds[i], i);
            isScheduled[i] = false;
        }
        for (int i = 0; i < localEgresses.length; i++) {
            egressDict.put(localEgresses[i], i);
        }
        //get weights and release times
        for (ArrayList<Job> schedule : localSchedules) {
            for (Job job : schedule) {
                int index = idDict.get(job.id);
                if (weights[index] == null) {
                    weights[index] = new EpsilonFraction(job.weight);
                    releaseTimes[index] = new EpsilonFraction(job.releaseTime);
                }
            }
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
            EpsilonFraction nextIngressCheck = ingressTimes[ingressIndex];
            boolean jobFound = false;
            //find a job from the next available ingress that has an open egress with available job
            for (int i = 0; !jobFound && i < ingressSchedule.size(); i++) {
                if (releaseTimes[idDict.get(ingressSchedule.get(i).id)].isGreater(ingressTimes[ingressIndex])) {
                    if (nextIngressCheck.isGreater(releaseTimes[idDict.get(ingressSchedule.get(i).id)])) {
                        nextIngressCheck = releaseTimes[idDict.get(ingressSchedule.get(i).id)];
                    }
                }
                else if (egressTimes[egressDict.get(ingressSchedule.get(i).egress)].isGreater(ingressTimes[ingressIndex])) {
                    if (nextIngressCheck.isGreater(egressTimes[egressDict.get(ingressSchedule.get(i).egress)])) {
                        nextIngressCheck = releaseTimes[idDict.get(ingressSchedule.get(i).id)];
                    }
                }
                else {
                    //ingress and egress will now be free at the same time
                    ingressTimes[ingressIndex] = EpsilonFraction.addFractions(ingressTimes[ingressIndex],
                            new EpsilonFraction(ingressSchedule.get(i).timeUnits, ingressSchedule.get(i).epsilon));
                    egressTimes[egressDict.get(ingressSchedule.get(i).egress)] = ingressTimes[ingressIndex];
                    beNodeSchedules.get(egressDict.get(ingressSchedule.get(i).egress)).add(ingressSchedule.get(i));
                    ingressSchedule.remove(i);
                    jobFound = true;
                    //record the next available egress, just in case none are currently available
                }
            }
            //if no applicable egress is available, let the job wait until there is one
            if (!jobFound) {
                ingressTimes[ingressIndex] = nextIngressCheck;
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

    public static void printSchedule(List<Job> schedule) {
        int currentTime = 0;
        int currentTimeEps = 0;
        for (Job job : schedule) {
            log.info("BENode " + job.egress + " (t = " + currentTime + " e" + currentTimeEps + "): Job " +
                    job.id + " processed in " + job.timeUnits + " e" + job.epsilon);
            currentTime += job.timeUnits;
            currentTimeEps += job.epsilon;
        }
    }

    public static void printSchedules(ArrayList<ArrayList<Job>> beNodeSchedules) {
        for (ArrayList<Job> schedule : beNodeSchedules) {
            CalculateSchedules.printSchedule(schedule);
        }
    }
}
