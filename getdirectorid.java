import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class getdirectorid
        extends Mapper<Object, Text, Text, Text> {
    //@Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {


        String line = value.toString();

        String[] titleline = line.split(":");
        String title = titleline[0];
        String nconst = title.substring(10);
        String tconst = title.substring(0,9);
        nconst = nconst.strip();
        tconst = tconst.strip();
        String crewid = "";

//     List<String> list = Collections.singletonList(nconst);

       if (nconst.contains(",")){
           String[] ntmp = nconst.split("[,]",0);
           for (String s:ntmp) {
               nconst = s;
               //System.out.println("Loop string nconst is  " + nconst +  ":::::::" + "Tconst is " + tconst);
               //System.out.println("Tconst is " + tconst);
               crewid = "I" + ":" + nconst + ":" + tconst + ":" + line + "loop";
             //  System.out.println("Loop Crew id is: " + crewid);
           }
       }
        else {
           //System.out.println("nconst is  " + nconst +  ":::::::" + "Tconst is " + tconst);
           crewid = "I" + ":" + nconst + ":" + tconst + ":" + line;
           //System.out.println("Crew id is: " + crewid);
        }

        //String crewid = "I" + ":" + nconst + ":" + tconst + ":" + line + ":" + "tracker";
        context.write(new Text(nconst), new Text(crewid));
    }
}

/*
 String line = value.toString();

        String[] titleline = line.split(":");
        String title = titleline[0];
        // String line2 = title.toString();
        String[] title_director = title.split("\t");
        String nconst_tmp = title_director[2];
        String tconst = title_director[1];
        String[] nconst_id = nconst_tmp.split(",");
        String nconst = "";

        if (nconst_id.length > 1) {
            for (String s : nconst_id) {
                nconst = s;
            }
        }
        else {
            nconst = nconst_id[0];
        }
        String crewid = "I" + ":" + nconst + ":" + tconst + ":" + line + ":" + "tracker";
        context.write(new Text(nconst), new Text(crewid));

 */



