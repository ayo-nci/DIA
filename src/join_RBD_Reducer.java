import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class join_RBD_Reducer
        extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String RBD = "";
        String ratings_data = "";
        String directors_data = "";
        String basics_data = "";
        for (Text value : values) {
            String line = value.toString();
            String[] rdtitle = line.split(":");
            String file_tracker = rdtitle[0];

            switch (file_tracker)
            {
                case "R":
                    ratings_data = rdtitle[1];
                    break;
                case "B":
                    basics_data = rdtitle[1];
                    break;
                case "D":
                    directors_data = rdtitle[1];
                    break;
                default:
                    return;
            }
        }
        RBD = directors_data + ":" + ratings_data + ":" + basics_data;
        context.write(new Text(key), new Text(RBD));
    }
}



