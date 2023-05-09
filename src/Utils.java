
import java.util.Queue;

public class Utils {

    public void clearAllAndAddTestValues(Agent[] agents) {
        for (Agent agent : agents) {
            agent.clearDB("MYTABLE");
            agent.clearDB("SNAPSHOT");
            for (int i = 1; i <= 4; i++) {
                agent.insertValue(i, i * 100, "MYTABLE");
                agent.insertValue(i, i * 100, "SNAPSHOT");
            }
            agent.commit();
        }
        System.out.println("Databases are ready for experiment.");
    }

    public void closeConnection(Agent[] agents) {
        for (Agent c : agents)
            c.closeConnection();
    }
}
