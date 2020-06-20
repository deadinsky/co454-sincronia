struct Job {
  1: i32 index,
  2: i32 timeUnits,
  3: i32 epsilon
}

exception IllegalArgument {
  1: string message;
}

service SincroniaService {
 i32 sendJobs (1: list<Job> schedule);
 oneway void setClients (1: i32 numClients);
 oneway void initialize (1: string hostname, 2: i32 port, 3: i32 numThreads);
 oneway void ping();
}