// test case 1:
//     T1: r([id=1]); [id=1]=[id=1]+10; w([id=1])
//     T2: r([id=1]); r([id=2]); [id=2]=[id=1]+[id=2]; w([id=2])
//     T3: r([id=1]); r([id=2]); r([id=3]); [id=3]=[id=1]+[id=2]; w([id=3])
public class main {
    public static void main(String args[]) {
        System.out.println("\nStarting experiment.\n");
        Utils util = new Utils();
        Agent[] agents = new Agent[3];
        Client client = new Client(agents);
        int[] ports = new int[] { 5500, 5501, 5502 };
        for (int i = 0; i < 3; i++)
            agents[i] = new Agent("Wenbo Li", "postgres", ports[i]);
        System.out.println("Databases connected.\n");
        // clear and initiate DBs
        util.clearAllAndAddTestValues(agents);
        // set leader and make connections
        client.leader = agents[0];
        client.leader.agents = agents;

        //simualte leader failure
        client.leader.simulateFailure=true;

        // We use time stamp of BEGIN to schedual transactions. i.e. using a queue
        int lastSuccessT = 0;
        int Tct = 0;
        while (!client.Ts.isEmpty()) {
            // send T to leader
            if (client.processesTransactions()) {
                //true: COMMIT; false:ABORT
                System.out.println("T" + (++Tct) + ": 2PC COMMIT.");
                lastSuccessT = Tct;
            } else
                System.out.println("T" + (++Tct) + ": 2PC ABORT. Rolling back all DBs to T" + (lastSuccessT) + "...");
        }
        // close all connections
        util.closeConnection(agents);
        System.out.println("...Done!\n");
    }
}