package Task2;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

public class SortHashMap {
    private List<Entry<String,Float>> list = new LinkedList<Entry<String,Float>>();

    public static  List<Entry<String,Float>> sortHashMap(HashMap<String,Float> map){
        SortHashMap sorthashmap = new SortHashMap();

        sorthashmap.list.addAll(map.entrySet());

        Collections.sort(sorthashmap.list,new Comparator<Entry<String,Float>>(){
            public int compare(Entry obj1,Entry obj2){
                float score1 = Float.parseFloat(obj1.getValue().toString());
                float score2 = Float.parseFloat(obj2.getValue().toString());
                if(score1 < score2)
                    return 1;
                else if(score1 == score2)
                    return 0;
                else
                    return -1;
            }
        });

        return sorthashmap.list;
    }
}