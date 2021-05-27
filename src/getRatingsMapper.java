import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class getRatingsMapper
        extends Mapper<Object, Text, Text, Text> {
    //@Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {


        String line = value.toString();
        if (line.contains("averageRating")) {
            return;
        }

        String[] rtitle = line.split("\t");
        //String tconst = rtitle[0];
        //String averageRating = rtitle[1];
        //String numVotes = rtitle[2];
        //We will get the product rating for each title by multiplying the
        //average rating and the number of votes for the title.
        //By this way, we take into account how popular a movie is
        //and how entertaining it is to those who voted for it
        float prod_rating = Float.parseFloat(rtitle[1]) * Float.parseFloat(rtitle[2]);
        String ratings_line = "R" + ":" + Float.toString(prod_rating);
        context.write(new Text(rtitle[0]), new Text(ratings_line));
    }
}

/*

  Scanner sc = new Scanner(System.in);
        //String line = null;
        sc.nextLine(); //Skip the header
        while (sc.hasNextLine()) {
            String line = value.toString();
            String[] title = line.split("\t");
            String tconst = title[0];
            String ratings_line = "R" + ":" + line;
        }

String[] rtitle = line.split("\t");
        String tconst = rtitle[0];
        String averageRating = rtitle[1];
        String numVotes = rtitle[2];
        //We will get the product rating for each title by multiplying the
        //average rating and the number of votes for the title.
        //By this way, we take into account how popular a movie is
        //and how entertaining it is to those who voted for it
        int prod_rating = Integer.parseInt(averageRating) * Integer.parseInt(numVotes);
        String ratings_line = "R" + ":" + Integer.toString(prod_rating);
        context.write(new Text(tconst), new Text(ratings_line));


 String line = value.toString();
        if (line.contains("averageRating")) {
            return;
        }
        String[] rtitle = line.split("\t");
        String tconst = rtitle[0];

        String ratings_line = "R" + ":" + line;
        context.write(new Text(tconst), new Text(ratings_line));



 String[] rtitle = line.split("\t");
        //String tconst = rtitle[0];
        //String averageRating = rtitle[1];
        //String numVotes = rtitle[2];
        //We will get the product rating for each title by multiplying the
        //average rating and the number of votes for the title.
        //By this way, we take into account how popular a movie is
        //and how entertaining it is to those who voted for it
        float prod_rating = Float.parseFloat(rtitle[1]) * Float.parseFloat(rtitle[2]);
        String ratings_line = "R" + ":" + Float.toString(prod_rating);
        context.write(new Text(rtitle[0]), new Text(ratings_line));


        String line = value.toString();

        String[] rtitle = line.split("\t");
        //String tconst = rtitle[1];
        //String rating = rtitle[0];
        String ratings_line = "R" + ":" + rtitle[0];
        context.write(new Text(rtitle[1]), new Text(ratings_line));


 */



