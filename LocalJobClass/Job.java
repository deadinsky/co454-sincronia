//simple triple
public class Job {
    public int id;
    public int ingress;
    public String egress;
    public int weight;
    public int releaseTime;
    public int timeUnits;
    public int epsilon;
    public long executionTime;
    public long executionEps;
    Job (int _id, int _ingress, String _egress, int _weight, int _releaseTime,
         int _timeUnits, int _epsilon, long _executionTime, long _executionEps) {
        id = _id;
        ingress = _ingress;
        egress = _egress;
        weight = _weight;
        releaseTime = _releaseTime;
        timeUnits = _timeUnits;
        epsilon = _epsilon;
        //should only be set for backend purposes
        executionTime = _executionTime;
        executionEps = _executionEps;
    }
}
