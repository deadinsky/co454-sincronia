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

	public static int calculateCCT(int coflowCompletionTime[]) {
		int wCCT = 0;
		for (int i = 0; i < coflowCompletionTime.length; i++) {
			wCCT += coflowCompletionTime[i];
		}
		return wCCT;
	}
	//assumption: each coflow flow has a unique ingress-egress pair
    public static int calculateCCTFromJobOrdering(ArrayList<ArrayList<Job>> schedules, int coflowCompletionTime[],
												  int[] ingressTimes, int[] egressTimes, int[] schedulesIndex,
												  int[] ingressCount, int[][] egressIngressCount) {
        int wCCT = -1;
        boolean willSplitAfterLoop = false;
        //we want to progress the schedule in conflict-free jobs as much as possible before dictating egress priority
        while (!willSplitAfterLoop) {
			willSplitAfterLoop = true;
			for (int i = 0; i < schedules.size(); i++) {
				//egress isn't receiving any other jobs
				if (schedulesIndex[i] == schedules.get(i).size()) {
					continue;
				}
				Job candidateJob = schedules.get(i).get(schedulesIndex[i]);
				int candidateIngress = candidateJob.ingress;
				int newTime = Math.max(ingressTimes[candidateIngress], egressTimes[i]) + candidateJob.timeUnits;
				//cheat 1: if an egress has an ingress next with no other egresses needing that ingress
				//then it can take it for free
				boolean canTakeNextJob = (egressIngressCount[i][candidateIngress] == ingressCount[candidateIngress]);
				//cheat 2: if an egress' job is shorter or equal to the next time any egress could theoretically
				//need that ingress, then it takes it for free
				if (!canTakeNextJob) {
					canTakeNextJob = true;
					for (int j = 0; j < schedules.size() && canTakeNextJob; j++) {
						ArrayList<Job> otherSchedule = schedules.get(j);
						int scheduleSize = otherSchedule.size();
						//egress isn't receiving any other jobs
						if (j == i || scheduleSize == schedulesIndex[j]) {
							continue;
						}
						int timeOffset = 0;
						int indexOffset = 0;
						while (egressTimes[i] + timeOffset < newTime && scheduleSize > schedulesIndex[j] + indexOffset) {
							Job otherJob = otherSchedule.get(schedulesIndex[j] + indexOffset);
							int otherIngress = otherJob.ingress;
							if (candidateIngress == otherIngress) {
								canTakeNextJob = false;
								break;
							}
							timeOffset += Math.max(ingressTimes[otherIngress] - (egressTimes[i] + timeOffset), 0)
									+ otherJob.timeUnits;
							indexOffset++;
						}
					}
				}
				if (canTakeNextJob) {
					ingressCount[candidateIngress]--;
					egressIngressCount[i][candidateIngress]--;
					coflowCompletionTime[candidateJob.id] = newTime;
					ingressTimes[candidateIngress] = newTime;
					egressTimes[i] = newTime;
					schedulesIndex[i]++;
					willSplitAfterLoop = false;
					continue;
				}
			}
		}
        for (int i = 0; i < schedules.size(); i++) {
			if (schedulesIndex[i] == schedules.get(i).size()) {
				continue;
			}
			Job candidateJob = schedules.get(i).get(schedulesIndex[i]);
			int candidateIngress = candidateJob.ingress;
			int newTime = Math.max(ingressTimes[candidateIngress], egressTimes[i]) + candidateJob.timeUnits;
			//need to clone everything
			int newCoflowCompletionTime[] = coflowCompletionTime.clone();
			int newIngressTimes[] = ingressTimes.clone();
			int newEgressTimes[] = egressTimes.clone();
			int newSchedulesIndex[] = schedulesIndex.clone();
			int newIngressCount[] = ingressCount.clone();
			int newEgressIngressCount[][] = Arrays.stream(egressIngressCount).map(int[]::clone).toArray(int[][]::new);
			newIngressCount[candidateIngress]--;
			newEgressIngressCount[i][candidateIngress]--;
			newCoflowCompletionTime[candidateJob.id] = newTime;
			newIngressTimes[candidateIngress] = newTime;
			newEgressTimes[i] = newTime;
			newSchedulesIndex[i]++;
			int newWCCT = calculateCCTFromJobOrdering(schedules, newCoflowCompletionTime,
					newIngressTimes, newEgressTimes, newSchedulesIndex, newIngressCount, newEgressIngressCount);
			if (wCCT == -1 || newWCCT < wCCT) {
				wCCT = newWCCT;
			}
		}
        //means schedule iteration is over
        if (wCCT == -1) {
        	wCCT = calculateCCT(coflowCompletionTime);
		}
        return wCCT;
    }
	public static int getSchedulePermutations(ArrayList<ArrayList<Job>> currentSchedule,
											  ArrayList<Collection<List<Job>>> jobPermutations, int index,
											  int coflowCompletionTime[], int[] ingressTimes, int[] egressTimes,
											  int[] schedulesIndex, int[] ingressCount, int[][] egressIngressCount) {
		int wCCT = -1;
    	if (index < jobPermutations.size()) {
			for (List<Job> perm : jobPermutations.get(index)) {
				ArrayList<ArrayList<Job>> newSchedule = new ArrayList<>();
				for (ArrayList<Job> schedule : currentSchedule) {
					newSchedule.add(schedule);
				}
				newSchedule.add(new ArrayList(perm));
				int newWCCT = getSchedulePermutations(newSchedule, jobPermutations, index + 1, coflowCompletionTime,
						ingressTimes, egressTimes, schedulesIndex, ingressCount, egressIngressCount);
				//for testing purposes
				if (wCCT == -1 || newWCCT < wCCT) {
					wCCT = newWCCT;
				}
			}
		} else {
    		//need to copy all of the int arrays so modification does not effect reusing them for other schedules
    		wCCT = calculateCCTFromJobOrdering(currentSchedule, coflowCompletionTime.clone(),
					ingressTimes.clone(), egressTimes.clone(), schedulesIndex.clone(), ingressCount.clone(),
					Arrays.stream(egressIngressCount).map(int[]::clone).toArray(int[][]::new));
		}
		return wCCT;
	}

	//for purposes of time and use cases, epsilon will not be considered in this solver
	//it was also assume the format of the generator (e.g. 1 to n for ingresses and ids)
	public static int calculateOptimalCCT(ArrayList<ArrayList<Job>> schedules) {
		//LinkedHashSet<Integer> ingressesSet = new LinkedHashSet<>();
		//LinkedHashSet<Integer> idsSet = new LinkedHashSet<>();
		//ArrayList<String> egressesList = new ArrayList<>();
		int ingressTimes[] = new int[schedules.size()];
		int egressTimes[] = new int[schedules.size()];
		int schedulesIndex[] = new int[schedules.size()];
		int ingressCount[] = new int[schedules.size()];
		int egressIngressCount[][] = new int[schedules.size()][schedules.size()];
		int numCoflowsMinusOne = 0;
		for (int i = 0; i < schedules.size(); i++) {
			ingressTimes[i] = 0;
			egressTimes[i] = 0;
			schedulesIndex[i] = 0;
			for (int j = 0; j < schedules.size(); j++) {
				egressIngressCount[i][j] = 0;
			}
            //egressesList.add(schedules.get(i).get(0).egress);
            //reindex all of the ingresses and ids for convenient indexing, remove epsilons for emphasis
			for (Job job : schedules.get(i)) {
				job.ingress -= 1;
				job.id -= 1;
				job.epsilon = 0;
				egressIngressCount[i][job.ingress]++;
				numCoflowsMinusOne = Math.max(numCoflowsMinusOne, job.id);
                //ingressesSet.add(job.ingress);
                //idsSet.add(job.id);
			}
		}
		//will be used for shortcut if only one egress needs a particular ingress
		for (int i = 0; i < schedules.size(); i++) {
			ingressCount[i] = 0;
			for (int j = 0; j < schedules.size(); j++) {
				ingressCount[i] += egressIngressCount[j][i];
			}
		}
		//Integer ingresses[] = ingressesSet.toArray(new Integer[0]);
        //Integer ids[] = idsSet.toArray(new Integer[0]);
        //String egresses[] = egressesList.toArray(new String[0]);
		int coflowCompletionTime[] = new int[numCoflowsMinusOne+1];
		for (int i = 0; i < coflowCompletionTime.length; i++) {
			coflowCompletionTime[i] = 0;
		}
        //permutations for every single schedule
		ArrayList<Collection<List<Job>>> jobPermutations = new ArrayList<>();
		for (int i = 0; i < schedules.size(); i++) {
			jobPermutations.add(Collections2.permutations(schedules.get(i)));
		}
		int wCCT = getSchedulePermutations(new ArrayList<ArrayList<Job>>(), jobPermutations, 0, coflowCompletionTime,
				ingressTimes, egressTimes, schedulesIndex, ingressCount, egressIngressCount);
		System.out.println("optimal wCCT: " + wCCT);
		System.out.println("coflows = " + (numCoflowsMinusOne+1));
		System.out.println("average wCCT = " + ((float) wCCT) / (numCoflowsMinusOne+1));
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
