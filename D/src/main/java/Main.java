import streaming.FootballStatistics;
import streaming.FootballStatisticsProxy;

/**
 * @author Imran, Muhammad
 */
public class Main {

    /**
     * The entry point to the program.
     *
     * @param args provide path to the data file.
     */
    public static void main(String[] args) throws IllegalAccessException {
        if (args.length == 0) {
            throw new IllegalArgumentException("Please provide the data file path in the first argument.");
        }
        String filePath = args[0].trim();
        FootballStatistics statistics = FootballStatisticsProxy.newInstance(FootballStatistics.newInstance(filePath));

        statistics.writeAvertedGoalEvents();
        statistics.writeHighestAvgDistanceCovered();
    }
}
