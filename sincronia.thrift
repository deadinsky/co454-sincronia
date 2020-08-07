struct Job {
  1: i32 id,
  2: i32 ingress,
  3: string egress,
  4: i32 weight,
  5: i32 releaseTime,
  6: i32 timeUnits,
  7: i32 epsilon,
  8: i32 executionTime,
  9: i32 executionEps
}

exception IllegalArgument {
  1: string message;
}

service SincroniaService {
 i32 sendJobs (1: list<Job> schedule, 2: i32 ingress);
 oneway void setClients (1: i32 numClients);
 oneway void initialize (1: string hostname, 2: i32 port, 3: i32 numThreads);
 oneway void ping();
}