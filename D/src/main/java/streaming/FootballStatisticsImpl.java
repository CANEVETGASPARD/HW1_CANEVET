package streaming;

import model.DebsFeature;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The implementation of {@link FootballStatistics} to perform analytics on DEBS dataset.
 *
 * @author Imran, Muhammad
 */
public class FootballStatisticsImpl implements FootballStatistics {
    /*
        The StreamExecutionEnvironment is the context in which a streaming program is executed.
    */
    final StreamExecutionEnvironment STREAM_EXECUTION_ENVIRONMENT =
            StreamExecutionEnvironment.getExecutionEnvironment();
    /*
        File path of the dataset.
    */
    private final String filePath;
    /**
     * stream of events to be evaluated lazily
     */
    private DataStream<DebsFeature> events;

    /**
     * @param filePath dataset file path as a {@link String}
     */
    FootballStatisticsImpl(String filePath) {
        this.filePath = filePath;
    }

    /**
     * write the events that show that ball almost came near goal (within penalty area),
     * but the ball was kicked out of the penalty area by a player of the opposite team.
     */
    @Override
    public void writeAvertedGoalEvents() {
        // TODO: Write your code here.

    }

    /**
     * Highest average distance of player A1 (of Team A) ran in every 5 minutes duration. You can skip 1 minute duration between every two durations.
     */
    @Override
    public void writeHighestAvgDistanceCovered() {
        // TODO: Write your code here.

    }

    /**
     * Creates {@link StreamExecutionEnvironment} and {@link DataStream} before each streaming task
     * to initialize a stream from the beginning.
     */
    @Override
    public void initStreamExecEnv() {

         /*
          Setting the default parallelism of our execution environment.
          Feel free to change it according to your environment/number of operators, sources, and sinks you would use.
          However, it should not have any impact on your results whatsoever.
         */
        STREAM_EXECUTION_ENVIRONMENT.setParallelism(2);

        /*
          Event time is the time that each individual event occurred on its producing device.
          This time is typically embedded within the records before they enter Flink.
         */
        STREAM_EXECUTION_ENVIRONMENT.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*
          Reads the file as a text file.
         */
        DataStream<String> dataStream = STREAM_EXECUTION_ENVIRONMENT.readTextFile(filePath);

        /*
          Creates DebsFeature for each record.
          ALTERNATIVELY You can use Tuple type (For that you would need to change generic type of 'event').
         */
        events = dataStream.map(DebsFeature::fromString);
    }
}

