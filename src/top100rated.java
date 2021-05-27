import java.io.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class top100rated extends Mapper<Object,
        Text, Text, LongWritable> {

    private TreeMap<Long, String> tmap;

    @Override
    public void setup(Context context) throws IOException,
            InterruptedException
    {
        tmap = new TreeMap<Long, String>();
    }

    @Override
    public void map(Object key, Text value,
                    Context context) throws IOException,
            InterruptedException {
        // input data format => movie_name
        // no_of_views (tab seperated)
        // we split the input data
        String line = value.toString();

        if (line.contains("\\N")) {
            return;
        }
        String[] titleline = line.split(":");
        //System.out.println("Title line is: " + titleline);
        if (titleline.length < 7) {
            return;
        }
        String movie_dets = titleline[7];
        String[] movie_base = movie_dets.split("[\t]", 0);

        if (movie_base.length < 5) {
            return;
        }
        /*System.out.println("Movie base is: " + movie_base);
        System.out.println("Movie type is: " + movie_base[0]);
        System.out.println("Movie name is: " + movie_base[1]);
        System.out.println("Movie year is: " + movie_base[2]);
        System.out.println("Movie duration is: " + movie_base[3]);
        System.out.println("Movie genre is: " + movie_base[4]);
        System.out.println("Movie length is: " + movie_base.length);
        */
        int movie_year = Integer.parseInt(movie_base[2]);
        double movie_rating = 0.0F;
        if (movie_year > 1950) {
            if (titleline[6].isEmpty()) {
                movie_rating = 0.0;
            } else movie_rating = Float.parseFloat(titleline[6]);
            //System.out.println("Movie year is " + movie_year);
            //System.out.println("Movie rating is " + movie_rating);
            //System.out.println("Line is " + line);
        }
            String movie_name = line;
            //long no_of_views = Long.parseLong(String.valueOf(Math.round(prod_rating)));
            long no_of_views = (long) movie_rating;
            // insert data into treeMap,
            // we want top 10 rated movies
            // so we pass no_of_views as key
            tmap.put(no_of_views, movie_name);

            // we remove the first key-value
            // if it's size increases 10
            if (tmap.size() > 100) {
                tmap.remove(tmap.firstKey());
            }
        }


    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException
    {
        for (Map.Entry<Long, String> entry : tmap.entrySet())
        {

            long count = entry.getKey();
            String name = entry.getValue();

            context.write(new Text(name), new LongWritable(count));
        }
    }
}



