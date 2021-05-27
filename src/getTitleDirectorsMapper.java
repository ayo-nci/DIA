import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class getTitleDirectorsMapper
        extends Mapper<Object, Text, Text, Text> {
    //@Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        if (line.contains("directors")) {
            return;
        }
        String[] dtitle = line.split("\t");
        //String tconst = dtitle[0];
        //String directors = dtitle[1];
        String directors_line = "D" + ":" + dtitle[1];
        context.write(new Text(dtitle[0]), new Text(directors_line));

    }
}



