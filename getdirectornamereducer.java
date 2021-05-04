import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class getdirectornamereducer
        extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String dname = "";
        String director_id_data = "";
        String director_name_data = "";

        for (Text value : values) {
            String line = value.toString();
            String[] dcrewdata = line.split(":");

            String file_tracking = dcrewdata[0];
            switch (file_tracking) {
                case "C":
                    director_name_data = dcrewdata[1];
                    break;
                case "I":
                    director_id_data = dcrewdata[1] + ":" + line;
                    break;
                default:
                    return;
                }
            dname = director_name_data + ":" + director_id_data;
        }

        context.write(new Text(key), new Text(dname));
    }
}



