import com.google.common.collect.Collections2;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class BruteForceSolver {

	public static int calculateCCT(int coflowCompletionTime[], int[] weights) {
		int wCCT = 0;
		for (int i = 0; i < coflowCompletionTime.length; i++) {
			wCCT += weights[i] * coflowCompletionTime[i];
		}
		return wCCT;
	}
	//assumption: each coflow flow has a unique ingress-egress pair
    public static int calculateCCTFromJobOrdering(ArrayList<ArrayList<Job>> schedules,
												  int schedulesSize, int bestWCCT, int[] weights,
												  int coflowCompletionTime[], int[] ingressTimes, int[] egressTimes,
												  int[] schedulesIndex, int[] ingressCount, int[][] egressIngressCount) {
        int wCCT = -1;
        int breakIndex = -1;
        int i = 0;
        //we want to progress the schedule in conflict-free jobs as much as possible before dictating egress priority
        while (i != breakIndex) {
			//breakIndex makes sure no changes are made for one full iteration before moving to the next part
			if (breakIndex == -1) {
				breakIndex = i;
			}
			//egress isn't receiving any other jobs
			if (schedulesIndex[i] == schedules.get(i).size()) {
				i = (i + 1) % schedulesSize;
				continue;
			}
			Job candidateJob = schedules.get(i).get(schedulesIndex[i]);
			int candidateIngress = candidateJob.ingress;
			int newIngressTime = Math.max(ingressTimes[candidateIngress], egressTimes[i]) + candidateJob.timeUnits;
			int newEgressTime = Math.max(newIngressTime, (schedulesIndex[i] == schedules.get(i).size() - 1 ?
					0 : schedules.get(i).get(schedulesIndex[i] + 1).releaseTime));
			//cheat 1: if an egress has an ingress next with no other egresses needing that ingress
			//then it can take it for free
			boolean canTakeNextJob = (egressIngressCount[i][candidateIngress] == ingressCount[candidateIngress]);
			//cheat 2: if an egress' job is shorter or equal to the next time any egress could theoretically
			//need that ingress, then it takes it for free
			if (!canTakeNextJob) {
				canTakeNextJob = true;
				for (int j = 0; j < schedulesSize && canTakeNextJob; j++) {
					ArrayList<Job> otherSchedule = schedules.get(j);
					int scheduleSize = otherSchedule.size();
					//egress isn't receiving any other jobs
					if (j == i || scheduleSize == schedulesIndex[j]) {
						continue;
					}
					int timeOffset = 0;
					int indexOffset = 0;
					while (egressTimes[i] + timeOffset < newEgressTime && scheduleSize >
							schedulesIndex[j] + indexOffset) {
						Job otherJob = otherSchedule.get(schedulesIndex[j] + indexOffset);
						int otherIngress = otherJob.ingress;
						if (candidateIngress == otherIngress) {
							canTakeNextJob = false;
							break;
						}
						int newTimeOffset = 0;
						//ingress needs to be free and job must be released
						newTimeOffset = Math.max(ingressTimes[otherIngress] -
								(egressTimes[i] + timeOffset), newTimeOffset);
						newTimeOffset = Math.max(otherJob.releaseTime -
								(egressTimes[i] + timeOffset), newTimeOffset);
						timeOffset += newTimeOffset + otherJob.timeUnits;
						indexOffset++;
					}
				}
			}
			if (canTakeNextJob) {
				//could reset entire loop here but chose to keep iterating through each entry equally
				ingressCount[candidateIngress]--;
				egressIngressCount[i][candidateIngress]--;
				coflowCompletionTime[candidateJob.id] = newIngressTime;
				ingressTimes[candidateIngress] = newIngressTime;
				egressTimes[i] = newEgressTime;
				schedulesIndex[i]++;
				breakIndex = -1;
			}
			i = (i + 1) % schedulesSize;
		}
        //may save time if we end schedules with super large wCCTs early
		//note that the wCCT of this schedule must be greater or equal to
		//I tested it empirically on the toy schedule and it seemed to save time
		int potentialWCCT = calculateCCT(coflowCompletionTime, weights);
        if (bestWCCT != -1 && potentialWCCT >= bestWCCT) {
        	return potentialWCCT;
		}
        //we went through all of the cheats, now test each variation depending on which egress goes next
        for (i = 0; i < schedulesSize; i++) {
			//egress isn't receiving any other jobs
			if (schedulesIndex[i] == schedules.get(i).size()) {
				continue;
			}
			Job candidateJob = schedules.get(i).get(schedulesIndex[i]);
			int candidateIngress = candidateJob.ingress;
			int newIngressTime = Math.max(ingressTimes[candidateIngress], egressTimes[i]) + candidateJob.timeUnits;
			int newEgressTime = Math.max(newIngressTime, (schedulesIndex[i] == schedules.get(i).size() - 1 ?
					0 : schedules.get(i).get(schedulesIndex[i] + 1).releaseTime));
			//need to clone everything
			int newCoflowCompletionTime[] = coflowCompletionTime.clone();
			int newIngressTimes[] = ingressTimes.clone();
			int newEgressTimes[] = egressTimes.clone();
			int newSchedulesIndex[] = schedulesIndex.clone();
			int newIngressCount[] = ingressCount.clone();
			int newEgressIngressCount[][] = Arrays.stream(egressIngressCount).map(int[]::clone).toArray(int[][]::new);
			newIngressCount[candidateIngress]--;
			newEgressIngressCount[i][candidateIngress]--;
			newCoflowCompletionTime[candidateJob.id] = newIngressTime;
			newIngressTimes[candidateIngress] = newIngressTime;
			newEgressTimes[i] = newEgressTime;
			newSchedulesIndex[i]++;
			int newWCCT = calculateCCTFromJobOrdering(schedules,
					schedulesSize, bestWCCT, weights, newCoflowCompletionTime, newIngressTimes,
					newEgressTimes, newSchedulesIndex, newIngressCount, newEgressIngressCount);
			if (wCCT == -1 || newWCCT < wCCT) {
				wCCT = newWCCT;
			}
		}
        //means schedule iteration is over
        if (wCCT == -1) {
        	return potentialWCCT;
		}
        return wCCT;
    }
	public static int getSchedulePermutations(ArrayList<ArrayList<Job>> schedule,
											  int schedulesSize, int index, int bestWCCT, int[] weights,
											  ArrayList<ArrayList<ArrayList<Job>>> jobPermutations,
											  int coflowCompletionTime[], int[] ingressTimes,
											  int[] schedulesIndex, int[] ingressCount, int[][] egressIngressCount) {
		int wCCT = bestWCCT;
    	if (index < schedulesSize) {
			for (ArrayList<Job> perm : jobPermutations.get(index)) {
				schedule.set(index, perm);
				int newWCCT = getSchedulePermutations(schedule,
						schedulesSize, index + 1, wCCT, weights, jobPermutations,
						coflowCompletionTime, ingressTimes, schedulesIndex, ingressCount, egressIngressCount);
				if (wCCT == -1 || newWCCT < wCCT) {
					wCCT = newWCCT;
				}
			}
		} else {
    		//need to copy all of the int arrays so modification does not effect reusing them for other schedules
			int egressTimes[] = new int[schedulesSize];
			for (int i = 0; i < schedulesSize; i++) {
				if (schedule.get(i).size() > 0) {
					egressTimes[i] = schedule.get(i).get(0).releaseTime;
				}
			}
    		wCCT = calculateCCTFromJobOrdering(schedule, schedulesSize, wCCT, weights,
					coflowCompletionTime.clone(), ingressTimes.clone(), egressTimes,
					schedulesIndex.clone(), ingressCount.clone(),
					Arrays.stream(egressIngressCount).map(int[]::clone).toArray(int[][]::new));
		}
		return wCCT;
	}

	//for purposes of time and use cases, epsilon will not be considered in this solver
	//it was also assume the format of the generator (e.g. 1 to n for ingresses and ids)
	public static int calculateOptimalCCT(ArrayList<ArrayList<Job>> schedules) {
		HashMap<Integer, Integer> ingressesDict = new HashMap<>();
		HashMap<Integer, Integer> idsDict = new HashMap<>();
		ArrayList<Integer> weights = new ArrayList<>();
		int schedulesSize = schedules.size();
		int ingressTimes[] = new int[schedulesSize];
		int schedulesIndex[] = new int[schedulesSize];
		int ingressCount[] = new int[schedulesSize];
		int egressIngressCount[][] = new int[schedulesSize][schedulesSize];
		int globalIngressIndex = 0;
		int globalIdIndex = 0;
		for (int i = 0; i < schedulesSize; i++) {
			ingressTimes[i] = 0;
			schedulesIndex[i] = 0;
			for (int j = 0; j < schedulesSize; j++) {
				egressIngressCount[i][j] = 0;
			}
            //reindex all of the ingresses and ids for convenient indexing, remove epsilons for emphasis
			for (Job job : schedules.get(i)) {
				int ingressIndex = ingressesDict.getOrDefault(job.ingress, -1);
				if (ingressIndex == -1) {
					ingressesDict.put(job.ingress, globalIngressIndex);
					ingressIndex = globalIngressIndex;
					globalIngressIndex++;
				}
				job.ingress = ingressIndex;
				int idIndex = idsDict.getOrDefault(job.id, -1);
				if (idIndex == -1) {
					idsDict.put(job.id, globalIdIndex);
					idIndex = globalIdIndex;
					globalIdIndex++;
					weights.add(job.weight);
				}
				job.id = idIndex;
				job.epsilon = 0;
				egressIngressCount[i][ingressIndex]++;
			}
		}
		//will be used for shortcut if only one egress needs a particular ingress
		for (int i = 0; i < schedulesSize; i++) {
			ingressCount[i] = 0;
			for (int j = 0; j < schedulesSize; j++) {
				//read it as "the number of jobs from this ingress is equal to the sum of jobs
				//at each egress which need a job from this ingress"
				ingressCount[i] += egressIngressCount[j][i];
			}
		}
		int coflowCompletionTime[] = new int[globalIdIndex];
		for (int i = 0; i < coflowCompletionTime.length; i++) {
			coflowCompletionTime[i] = 0;
		}
        //permutations for every single schedule
		ArrayList<ArrayList<ArrayList<Job>>> jobPermutations = new ArrayList<>(schedulesSize);
		ArrayList<ArrayList<Job>> schedule = new ArrayList<>(schedulesSize);
		//convert them all to arraylists beforehand to save time
		for (int i = 0; i < schedulesSize; i++) {
			Collection<List<Job>> permutations = Collections2.permutations(schedules.get(i));
			ArrayList<ArrayList<Job>> convertedPermutations = new ArrayList<>(permutations.size());
			for (List<Job> perm : permutations) {
				convertedPermutations.add(new ArrayList<Job>(perm));
			}
			jobPermutations.add(convertedPermutations);
			schedule.add(null);
		}
		int wCCT = getSchedulePermutations(schedule, schedulesSize, 0, -1,
				weights.stream().mapToInt(i->i).toArray(), jobPermutations,
				coflowCompletionTime, ingressTimes, schedulesIndex, ingressCount, egressIngressCount);
		System.out.println("optimal wCCT: " + wCCT);
		System.out.println("coflows = " + globalIdIndex);
		System.out.println("average wCCT = " + ((float) wCCT) / globalIdIndex);
		return wCCT;
	}

    public static void main(String [] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: java FENode schedule_file");
			System.exit(-1);
		}
		BasicConfigurator.configure();

		ArrayList<ArrayList<Job>> schedules = CalculateSchedules.parseSchedules(args[0], false);
		int wCCT = BruteForceSolver.calculateOptimalCCT(schedules);
		return;
    }
}
