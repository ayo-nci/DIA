import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class getBasicsMapper
        extends Mapper<Object, Text, Text, Text> {
    //@Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        if (line.contains("isAdult")) {
            return;
        }
        String[] btitle = line.split("\t");
        /*String tconst = btitle[0];
        String titleType = btitle[1];
        String primaryTitle = btitle[2];
        String startYear = btitle[5];
        String runtime = btitle[7];
        String genres = btitle[8]; */
        String basicfeatures = btitle[1] + "\t" + btitle[2] + "\t" + btitle[5] + "\t" + btitle[7] + "\t" + btitle[8];
        String basics_line = "B" + ":" + basicfeatures;

        context.write(new Text(btitle[0]), new Text(basics_line));
    }
}
/*
String line = value.toString();
        if (line.contains("isAdult")) {
            return;
        }
        String[] btitle = line.split("\t");
        String tconst = btitle[0];
        String basics_line = "B" + ":" + line;

 */


