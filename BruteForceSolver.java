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
    //assumption: each ingress-id-egress pair is unique
    public static int calculateCCTFromJobOrdering(ArrayList<ArrayList<Job>> schedules,
												  int[] ingressTimes, int[] egressTimes, int[] schedulesIndex,
												  int[][] egressIngressCount, int[] ingressCount) {
        int wCCT = 0;
        return wCCT;
    }
	public static int getSchedulePermutations(ArrayList<ArrayList<Job>> currentSchedule,
											  ArrayList<Collection<List<Job>>> jobPermutations, int index,
											  int[] ingressTimes, int[] egressTimes, int[] schedulesIndex,
											  int[][] egressIngressCount, int[] ingressCount) {
    	if (index < jobPermutations.size()) {
			for (List<Job> perm : jobPermutations.get(index)) {
				ArrayList<ArrayList<Job>> newSchedule = new ArrayList<>();
				for (ArrayList<Job> schedule : currentSchedule) {
					newSchedule.add(schedule);
				}
				newSchedule.add(new ArrayList(perm));
				int wCCT = getSchedulePermutations(newSchedule, jobPermutations, index + 1,
						ingressTimes, egressTimes, schedulesIndex, egressIngressCount, ingressCount);
				return wCCT;
			}
		} else {
		}
		return 0;
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
		int egressIngressCount[][] = new int[schedules.size()][schedules.size()];
		int ingressCount[] = new int[schedules.size()];
		for (int i = 0; i < schedules.size(); i++) {
			ingressTimes[i] = 0;
			egressTimes[i] = 0;
			schedulesIndex[i] = 0;
			for (int j = 0; j < schedules.size(); j++) {
				egressIngressCount[i][j] = 0;
			}
            //egressesList.add(schedules.get(i).get(0).egress);
            //reindex all of the ingresses and ids for convenient indexing
			for (Job job : schedules.get(i)) {
				job.ingress -= 1;
				job.id -= 1;
				egressIngressCount[i][job.ingress]++;
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
        //permutations for every single schedule
		ArrayList<Collection<List<Job>>> jobPermutations = new ArrayList<>();
		for (int i = 0; i < schedules.size(); i++) {
			jobPermutations.add(Collections2.permutations(schedules.get(i)));
		}
		getSchedulePermutations(new ArrayList<ArrayList<Job>>(), jobPermutations, 0,
				ingressTimes, egressTimes, schedulesIndex, egressIngressCount, ingressCount);
		//calculate wCCT
		int wCCT = -1;

		/*for (int i = 0; i < localIds.length; i++) {
			wCCT += finishTimes[i].numInt * originalWeights[i];
		}*/
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
		System.out.println("optimal wCCT: " + wCCT);
		//System.out.println("coflows = " + ids.size());
		//System.out.println("average wCCT = " + ((float) wCCT) / ids.size());
    }
}
