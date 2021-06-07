package streaming;

import model.DebsFeature;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.util.Collector;

import java.math.BigInteger;


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


    //apply function to display information of the window
    public class WindowDisplay implements AllWindowFunction<Tuple3<Long, BigInteger, Integer>, String, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<Tuple3<Long, BigInteger, Integer>> input, Collector<String> out) {
            long count = 0;
            for (Tuple3<Long, BigInteger, Integer> in: input) {
                count++;
            }
            out.collect("Window: " + window + "count: " + count);
        }
    }

    //apply function that computes a slinding average and output a tuple with starting, ending timestamp, averageDistance and a key to output the max
    public class WindowAverage implements AllWindowFunction<Tuple4<Long, BigInteger, Integer,Integer>, Tuple4<BigInteger,BigInteger,Double,String>, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<Tuple4<Long, BigInteger, Integer,Integer>> input, Collector<Tuple4<BigInteger,BigInteger,Double,String>> out) {
            long count = 0;
            long sum = 0;
            BigInteger startingWindow = input.iterator().next().f1;
            BigInteger endingWindow = input.iterator().next().f1;

            for (Tuple4<Long, BigInteger, Integer,Integer> in: input) {
                if (startingWindow.compareTo(in.f1) == 1){
                    startingWindow = in.f1;
                } else if(endingWindow.compareTo(in.f1) == -1){
                    endingWindow = in.f1;
                }
                count++;
                sum += Math.sqrt(in.f2*in.f2 + in.f3*in.f3);
            }

            Double average = (double) sum/count;
            out.collect(new Tuple4<>(startingWindow,endingWindow,average,""));
        }
    }

    @Override
    public void writeAvertedGoalEvents() throws Exception{
        // TODO: Write your code here.


    }
    /**
     * Highest average distance of player A1 (of Team A) ran in every 5 minutes duration. You can skip 1 minute duration between every two durations.
     */
    @Override
    public void writeHighestAvgDistanceCovered() throws Exception{
        // TODO: Write your code here.

        // map and filter the event to have only events from the interesting player (sensor 16 and 47) during the math
        DataStream<Tuple4<Long, BigInteger, Integer,Integer>> filteredEvent = events.map(new MapFunction<DebsFeature, Tuple4<Long, BigInteger, Integer,Integer>>() {
            public Tuple4<Long, BigInteger, Integer,Integer> map(DebsFeature debsFeature) {
                return new Tuple4<>(debsFeature.getSensorId(),debsFeature.getTimeStamp(),debsFeature.getPositionX(),debsFeature.getPositionY());
            }
        }).filter(line -> (line.f0 == 47 || line.f0 ==16)).filter(line -> (line.f1.compareTo(new BigInteger("10753295594424116"))==1 && line.f1.compareTo(new BigInteger("12557295594424116"))==-1) || (line.f1.compareTo(new BigInteger("13086639146403495"))==1 && line.f1.compareTo(new BigInteger("14879639146403495"))==-1));

        //make second column as timeStamp
        DataStream<Tuple4<Long, BigInteger, Integer,Integer>> filteredEventWithTimestamp = filteredEvent.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<Long, BigInteger, Integer,Integer>>forMonotonousTimestamps().withTimestampAssigner((event, ts) -> event.f1.longValue()));

        // if we want to see the number of element in the window
        //filteredEventWithTimestamp.windowAll(SlidingEventTimeWindows.of(Time.seconds(5*1000000000), Time.seconds(1000000000))).apply(new MyWindowFunction()).print();

        //using sliding window, important to be aware that out timestamp are in picosecond and not milisecond we have to multiply by 1000000000
        //finally we apply our slinding apply average and key among the last element in order to output the max average distance in a five seconds window
        DataStream<Tuple4<BigInteger,BigInteger,Double,String>> MaxAverageDistanceStream = filteredEventWithTimestamp.windowAll(SlidingEventTimeWindows.of(Time.seconds(5*1000000000), Time.seconds(1000000000))).apply(new WindowAverage()).keyBy(3).maxBy(2).setParallelism(1);
        MaxAverageDistanceStream.writeAsCsv("output/taskI");
        STREAM_EXECUTION_ENVIRONMENT.execute();
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

