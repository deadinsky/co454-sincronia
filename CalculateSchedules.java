import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class CalculateSchedules {
    static Logger log = Logger.getLogger(CalculateSchedules.class.getName());
    public static ArrayList<ArrayList<Job>> parseSchedules(String fileName, boolean isSortedByIngress) {
        ArrayList<ArrayList<Job>> schedules = new ArrayList<>();
        //determines if the schedules are grouped by ingress or egress
        int offset = isSortedByIngress ? 0 : 1;
        int gressLength = 0;
        Map<String, Integer> gressDict = new HashMap<>();
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
            //https://github.com/sincronia-coflow/workload-generator
            //first line: <number of ingress/egress ports> <number of coflows>
            //other lines: <(coflow) id> <releaseTime> <> <> <> (for each flow: <ingress> <egress> <size (timeUnits)>)
            while (scheduleReader.hasNextLine()) {
                String currentLine[] = scheduleReader.nextLine().split(" ");
                int coflowId = Integer.parseInt(currentLine[0]);
                int releaseTime = Integer.parseInt(currentLine[1]);
                for (int i = 5; i < currentLine.length; i += 3) {
                    String gress = currentLine[i + offset];
                    int gressIndex = gressDict.getOrDefault(gress, -1);
                    if (gressIndex == -1) {
                        gressIndex = gressLength;
                        gressDict.put(gress, gressLength);
                        gressLength++;
                    }
                    Integer ingress = Integer.parseInt(currentLine[i]);
                    String egress = currentLine[i+1];
                    int timeUnits = 0;
                    int epsilon = 0;
                    //this is to account for epsilon inclusion; generator will only touch "else"
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
                            epsilon = Integer.valueOf(epsilonString.substring(0, epsilonString.length() - 1))
                                    * (isNegative ? -1 : 1);
                        }
                    } else {
                        timeUnits = Integer.valueOf(currentLine[i+2]);
                    }
                    schedules.get(gressIndex).add(
                            new Job(coflowId, ingress, egress, 1, releaseTime, timeUnits, epsilon, 0, 0));
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
        int originalWeights[] = new int[localIds.length];
        EpsilonFraction releaseTimes[] = new EpsilonFraction[localIds.length];
        EpsilonFraction finishTimes[] = new EpsilonFraction[localIds.length];
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
                    originalWeights[index] = job.weight;
                    weights[index] = new EpsilonFraction(originalWeights[index]);
                    releaseTimes[index] = new EpsilonFraction(job.releaseTime);
                    finishTimes[index] = EpsilonFraction.zero;
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
            //schedule said job's coflow last
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
            EpsilonFraction nextReleaseCheck = EpsilonFraction.max;
            EpsilonFraction nextEgressCheck = EpsilonFraction.max;
            boolean jobFound = false;
            //find a job from the next available ingress that has an open egress with a released job
            for (int i = 0; !jobFound && i < ingressSchedule.size(); i++) {
                Job ingressJob = ingressSchedule.get(i);
                int flowIndex = idDict.get(ingressJob.id);
                int egressIndex = egressDict.get(ingressJob.egress);
                //record the next release time, just in case no job is released
                if (releaseTimes[flowIndex].isGreater(ingressTimes[ingressIndex])) {
                    if (nextReleaseCheck.isGreater(releaseTimes[flowIndex])) {
                        nextReleaseCheck = releaseTimes[flowIndex];
                    }
                }
                //record the next available egress, just in case none are currently available
                else if (egressTimes[egressIndex].isGreater(ingressTimes[ingressIndex])) {
                    if (nextEgressCheck.isGreater(egressTimes[egressIndex])) {
                        nextEgressCheck = egressTimes[egressIndex];
                    }
                }
                else {
                    //modify these for backend output
                    ingressJob.executionTime = ingressTimes[ingressIndex].numInt;
                    ingressJob.executionEps = ingressTimes[ingressIndex].numEps;
                    //ingress and egress will now be free at the same time
                    ingressTimes[ingressIndex] = EpsilonFraction.addFractions(ingressTimes[ingressIndex],
                            new EpsilonFraction(ingressJob.timeUnits, ingressJob.epsilon));
                    egressTimes[egressIndex] = ingressTimes[ingressIndex];
                    beNodeSchedules.get(egressIndex).add(ingressJob);
                    if (ingressTimes[ingressIndex].isGreater(finishTimes[flowIndex])) {
                        finishTimes[flowIndex] = ingressTimes[ingressIndex];
                    }
                    ingressSchedule.remove(i);
                    jobFound = true;
                }
            }
            //if no applicable egress is available, let the job wait until there is one
            if (!jobFound) {
                if (nextReleaseCheck.isGreater(nextEgressCheck)) {
                    ingressTimes[ingressIndex] = nextEgressCheck;
                } else {
                    ingressTimes[ingressIndex] = nextReleaseCheck;
                }
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
        //calculate wCCT
        long wCCTInt = 0;
        long wCCTEps = 0;
        double awCCT = 0;
        for (int i = 0; i < localIds.length; i++) {
            wCCTInt += finishTimes[i].numInt * originalWeights[i];
            wCCTEps += finishTimes[i].numEps * originalWeights[i];
            awCCT += ((double) finishTimes[i].numInt / localIds.length) * originalWeights[i];
        }
        System.out.println("wCCT = " + wCCTInt + (wCCTEps == 0 ? "" : (wCCTEps < 0 ? "" : "+") + wCCTEps + "e"));
        System.out.println("coflows = " + localIds.length);
        System.out.println("average wCCT = " + awCCT);
        return beNodeSchedules;
    }

    public static void printSchedule(List<Job> schedule) {
        for (Job job : schedule) {
            log.info("BENode " + job.egress + " (t = " + job.executionTime + " e" + job.executionEps + "): Job " +
                    job.id + " processed in " + job.timeUnits + " e" + job.epsilon);
        }
    }

    public static void printSchedules(ArrayList<ArrayList<Job>> beNodeSchedules) {
        for (ArrayList<Job> schedule : beNodeSchedules) {
            CalculateSchedules.printSchedule(schedule);
        }
    }
}
