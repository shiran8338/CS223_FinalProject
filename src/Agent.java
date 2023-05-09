
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.Queue;
import java.util.Hashtable;
import java.util.LinkedList;

public class Agent {
    // var for leader only
    public Agent[] agents;
    private Hashtable<Integer, Integer> prevWriteSet;
    // var for all agents
    public Connection c;
    public boolean commited;
    public boolean simulateFailure;
    public Queue<Integer> ReplicationLogWriteID;
    public Queue<Integer> ReplicationLogWriteValue;
    public Queue<Integer> ReplicationLogReadID;
    public Queue<Integer> ReplicationLogReadValue;

    public Agent(String userName, String dbName, int portNumber) {
        simulateFailure=false;
        commited = false;
        prevWriteSet = new Hashtable<>();
        ReplicationLogWriteID = new LinkedList<Integer>();
        ReplicationLogWriteValue = new LinkedList<Integer>();
        ReplicationLogReadID = new LinkedList<Integer>();
        ReplicationLogReadValue = new LinkedList<Integer>();
        c = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://0.0.0.0:" + portNumber + "/" + dbName,
                            userName, "");
            // turn off autoCommit since we are implementing our own 2pc
            c.setAutoCommit(false);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to connect server. Exiting...");
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        // System.out.println("Database connected.\n");
    }

    // method for leader only
    public boolean handleCOMMITRequest() {
        // send Log to agent
        for (Agent agent : agents) {
            if (agent != this)
                agent.receiveReplicationLog(ReplicationLogReadID, ReplicationLogReadValue, ReplicationLogWriteID,
                        ReplicationLogWriteValue);
        }
        // check if readSet == prevWriteSet
        if (!prevWriteSet.isEmpty()) {
            while (!ReplicationLogReadID.isEmpty()) {
                int currValue = ReplicationLogReadValue.poll();
                Integer prevValue = prevWriteSet.get(ReplicationLogReadID.poll());
                if (prevValue != null && prevValue != currValue)
                    return false;
            }
        }
        // clear prevWriteSet
        prevWriteSet.clear();
        // begin voting
        initT();
        // if passed, then 2pc is implemented here
        boolean COMMIT = true;
        while (!ReplicationLogWriteID.isEmpty()) {
            int id = ReplicationLogWriteID.poll();
            int rslt = ReplicationLogWriteValue.poll();
            // copy writeSet to prevWriteSet
            prevWriteSet.put(id, rslt);
            for (Agent agent : agents) {
                // 2pc to let clients vote
                if (!agent.setValue(id, rslt)) {
                    COMMIT = false;
                    break;
                }

                //simulate leader failure
                // if (simulateFailure)
                //     System.out.println(1/0);
                    
                // log after success
                agent.ReplicationLogWriteID.poll();
                agent.ReplicationLogWriteValue.poll();
            }
            if (!COMMIT)
                break;
        }
        if (!COMMIT) {
            rollBackAll();// if anyone aborted, rollback all
            prevWriteSet.clear();
        } else
            commitAll();// if all prepared, commit all
        return COMMIT;
    }

    public boolean commitAll() {
        for (Agent agent : agents) {
            try {
                agent.c.commit();
                agent.commited = true;
            } catch (Exception e) {
                System.err.println(e.getClass().getName() + ": " + e.getMessage());
                return false;
            }
        }
        return true;
    }

    public boolean rollBackAll() {
        for (Agent agent : agents) {
            Statement stmt = null;
            try {
                stmt = agent.c.createStatement();
                // stmt.executeUpdate("BEGIN;" + "SAVEPOINT savepoint;");
                stmt.executeUpdate("ABORT;");
                stmt.close();
            } catch (Exception e) {
                System.out.println("Failed to rollback all nodes. Exiting...");
                System.err.println(e.getClass().getName() + ": " + e.getMessage());
                System.exit(0);
            }
        }
        return true;
    }

    // method for all agents
    public void receiveReplicationLog(Queue<Integer> readSetID, Queue<Integer> readSetValue, Queue<Integer> writeSetID,
            Queue<Integer> writeSetValue) {
        commited=false;
        deepCopyQueue(readSetID, ReplicationLogReadID);
        deepCopyQueue(readSetValue, ReplicationLogReadValue);
        deepCopyQueue(writeSetID, ReplicationLogWriteID);
        deepCopyQueue(writeSetValue, ReplicationLogWriteValue);
    }

    public Integer handleRead(int id) {
        Statement stmt = null;
        try {
            stmt = c.createStatement();
            //read from snapshot
            ResultSet rs = stmt.executeQuery("SELECT * FROM SNAPSHOT;");
            while (rs.next()) {
                if (rs.getInt("id") == id) {
                    int rslt = rs.getInt("value");
                    // System.out.println(id+", "+rslt);
                    rs.close();
                    stmt.close();
                    return rslt;
                }
            }
            rs.close();
            stmt.close();
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        // System.out.println("Print done.\n");
        return null;
    }

    public void createTable(String tableName) {
        Statement stmt = null;
        try {
            stmt = c.createStatement();
            String sql = "CREATE TABLE "+tableName+
                    " (ID INT PRIMARY KEY     NOT NULL," +
                    " VALUE            INT     NOT NULL)";
            stmt.executeUpdate(sql);
            stmt.close();
        } catch (Exception e) {
            System.out.println("The below error can be ignored.");
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        // System.out.println("Table created.\n");
    }

    public boolean insertValue(int id, int value, String tableName) {
        Statement stmt = null;
        try {
            stmt = c.createStatement();
            String sql = "INSERT INTO "+tableName+" (ID,VALUE) "
                    + "VALUES (" + id + ", " + value + ");";
            stmt.executeUpdate(sql);
            stmt.close();
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            return false;
        }
        // System.out.println("Value inserted.\n");
        return true;
    }

    public void closeConnection() {
        try {
            c.close();
        } catch (Exception e) {
            System.out.println("Failed to close connecting with DBs. Exiting...");
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    public void printAllEntries() {
        Statement stmt = null;
        try {
            stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM MYTABLE;");
            System.out.println();
            while (rs.next()) {
                int id = rs.getInt("id");
                int value = rs.getInt("value");
                System.out.println("ID = " + id);
                System.out.println("VALUE = " + value);
            }
            rs.close();
            stmt.close();
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        // System.out.println("Print done.\n");
    }

    public boolean setValue(int id, int newValue) {
        // //simulate node voting ABORT(false)
        // if (id==1)
        // return false;
        Statement stmt = null;
        try {
            stmt = c.createStatement();
            String sql = "UPDATE MYTABLE set VALUE = " + newValue + " where ID=" + id + ";";
            sql += "UPDATE SNAPSHOT set VALUE = " + newValue + " where ID=" + id + ";";
            stmt.executeUpdate(sql);
            // print updated table
            // printAllEntries(c);
            stmt.close();
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            return false;
        }
        // System.out.println("Value updated.\n");
        return true;
    }

    public void clearDB(String tableName) {
        Statement stmt = null;
        try {
            stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM "+tableName+";");
            while (rs.next()) {
                int id = rs.getInt("id");
                deleteValue(id,tableName);
            }
            // print updated table
            // printAllEntries(c);
            rs.close();
            stmt.close();
        } catch (Exception e) {
            System.out.println("Failed to clear DBs. Exiting...");
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        // System.out.println("DB is now empty.\n");
    }

    public boolean deleteValue(int id,String tableName) {
        Statement stmt = null;
        try {
            stmt = c.createStatement();
            String sql = "DELETE from "+tableName+" where ID = " + id + ";";
            stmt.executeUpdate(sql);
            // print updated table
            // printAllEntries(c);
            stmt.close();
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            return false;
        }
        return true;
        // System.out.println("Value Deleted.\n");
    }

    public boolean commit() {
        try {
            c.commit();
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            return false;
        }
        return true;
    }

    public void abort() {
        Statement stmt = null;
        try {
            stmt = c.createStatement();
            stmt.executeUpdate("ABORT;");
            stmt.close();
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    public void initT() {
        for (Agent agent : agents) {
            Statement stmt = null;
            try {
                stmt = agent.c.createStatement();
                stmt.executeUpdate("BEGIN;");
                stmt.close();
            } catch (Exception e) {
                System.err.println(e.getClass().getName() + ": " + e.getMessage());
            }
        }
    }

    public void deepCopyQueue(Queue<Integer> from, Queue<Integer> to) {
        to.clear();
        int size = from.size();
        while (size-- > 0) {
            int curr = from.poll();
            from.add(curr);
            to.add(curr);
        }
    }

    public void REDO()
    {
        for (Agent agent : agents)
        {
            while (!agent.ReplicationLogWriteID.isEmpty()) {
                int id = agent.ReplicationLogWriteID.poll();
                int rslt = agent.ReplicationLogWriteValue.poll();
                agent.setValue(id, rslt);
            }
        }
    }
}