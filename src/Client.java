import java.util.LinkedList;
import java.util.Queue;
import java.util.Hashtable;

public class Client {
    public Queue<String> Ts;
    public Queue<Integer> readSetID;
    public Queue<Integer> readSetValue;
    public Queue<Integer> writeSetID;
    public Queue<Integer> writeSetValue;
    public Hashtable<String, Integer> ht = new Hashtable<>();
    public Agent leader;
    public Agent[] agents;

    public Client(Agent[] agents) {
        this.agents = agents;
        readSetID = new LinkedList<Integer>();
        readSetValue = new LinkedList<Integer>();
        writeSetID = new LinkedList<Integer>();
        writeSetValue = new LinkedList<Integer>();
        Ts = new LinkedList<String>();
        Ts.add("read(1,a);" +
                "add(a,a,10);" +
                "set(1,a+10)");
                
        Ts.add("read(1,a);" +
                "read(2,b);" +
                "add(c,a,b);" +
                "set(2,c)");

        Ts.add("read(1,a);" +
                "read(2,b);" +
                "add(c,a,b);" +
                "set(3,c)");
    }

    public void read(String idd, String saveTo) {
        int id = Integer.parseInt(idd);
        int curr = leader.handleRead(id);
        ht.put(saveTo, curr);//a -> 10
        // add to read set
        readSetID.add(id);
        readSetValue.add(curr);
    }

    public void updateAdd(String saveTo, String temp1, String temp2) {
        int curr = 0;
        try {
            curr += Integer.parseInt(temp1);
        } catch (NumberFormatException e) {
            curr += ht.get(temp1);
        }
        try {
            curr += Integer.parseInt(temp2);
        } catch (NumberFormatException e) {
            curr += ht.get(temp2);
        }
        ht.put(saveTo, curr);//a -> 20
    }

    public void mutiply(String saveTo, String temp1, String temp2) {
        int curr = 1;
        try {
            curr *= Integer.parseInt(temp1);
        } catch (NumberFormatException e) {
            curr *= ht.get(temp1);
        }
        try {
            curr *= Integer.parseInt(temp2);
        } catch (NumberFormatException e) {
            curr *= ht.get(temp2);
        }
        ht.put(saveTo, curr);
    }

    public void set(String idd, String value) {
        // add to wite set
        writeSetID.add(Integer.parseInt(idd));
        try {
            writeSetValue.add(Integer.parseInt(value));
        } catch (NumberFormatException e) {
            writeSetValue.add(ht.get(value));
        }
    }

    public boolean processesTransactions() {
        readSetID.clear();
        readSetValue.clear();
        writeSetID.clear();
        writeSetValue.clear();
        // We use TO protocal to schedual transactions. i.e. using a queue
        String nextT = Ts.peek();
        String[] temp = nextT.split(";");
        // transaction starts processing
        for (String t : temp) {
            String[] curr = t.split("[(,+)]");
            // read
            switch (curr[0]) {
                case "read":
                    read(curr[1], curr[2]);
                    break;
                case "add":
                    updateAdd(curr[1], curr[2], curr[3]);
                    break;
                case "set":
                    set(curr[1], curr[2]);
                    break;
                case "muti":
                    mutiply(curr[1], curr[2], curr[3]);
                    break;
            }
        }
        try {
            // transaction completes processing, send it to leader for 2pc.
            leader.receiveReplicationLog(readSetID, readSetValue, writeSetID,
                    writeSetValue);
            boolean rslt = leader.handleCOMMITRequest();//true COMMIT; false ABORT
            Ts.poll();
            return rslt;
        } catch (Exception e) {
            System.out.println("No respond from leader. It may have crashed...");
            System.out.println("Leader changed.");
            // if any DB already commited, REDO all
            for (Agent agent : agents) {
                if (agent.commited) {
                    // set new leader
                    leader = agent;
                    leader.agents = agents;
                    // redo all
                    System.out.println("Performing REDO.");
                    leader.REDO();
                    Ts.poll();
                    return true;
                }
            }
            // if none DB committed, ABORT all, restart transaction
            // set new leader
            for (Agent agent : agents) {
                if (agent != leader) {
                    leader = agent;
                    leader.agents = agents;
                    break;
                }
            }
            System.out.println("Performing UNDO.");
            leader.rollBackAll();
            return false;
        }
    }
}
