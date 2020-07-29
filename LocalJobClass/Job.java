//simple triple
public class Job {
    public int id;
    public String egress;
    public int weight;
    public int releaseTime;
    public int timeUnits;
    public int epsilon;
    Job (int _id, String _egress, int _weight, int _releaseTime, int _timeUnits, int _epsilon) {
        id = _id;
        egress = _egress;
        weight = _weight;
        releaseTime = _releaseTime;
        timeUnits = _timeUnits;
        epsilon = _epsilon;
    }
}
