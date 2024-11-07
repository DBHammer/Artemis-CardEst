package ecnu.dbhammer.test;

import com.opencsv.CSVWriter;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class rankRandomCSV {
    public static void main(String[] args) {
        rankRandom();
    }

    public static void rankRandom(){
        String filedir = "D:\\artemis\\data0508\\oceanbase\\join-more\\5\\cycle-97";
        String outdir = "D:\\artemis\\data0508\\oceanbase\\join-more\\5\\cycle-97";
        String joinOrderTimeFile = outdir + File.separator + "joinOrderRank_oceanbase" + ".csv";
        File file = new File(filedir);
        String output = "";
        String output_type = "";
        String[] queryFiles = file.list();
        Map<Integer, String> queryFilesMap = new HashMap<>();
        for(String queryFile : queryFiles){
            if(!queryFile.contains("query")) continue;
            int index1 = queryFile.indexOf("_");
            int index2 = queryFile.indexOf(".");
            int queryNum = Integer.parseInt(queryFile.substring(index1+1, index2));
            queryFilesMap.put(queryNum, queryFile);
        }
        for(int i=1;i<=50;i++) {
            String lineTxt;
            List<String[]> strList = new ArrayList<>();
            String[] oriQuery = {"", "", ""};
            List<Pair<String, Long>> pairs = new ArrayList<>();
            try {
                BufferedReader reader = new BufferedReader(new FileReader(filedir+File.separator+
                        queryFilesMap.get(i)));//打开对应query的csv文件
                reader.readLine();//第一行信息，为标题信息，不用

                while ((lineTxt = reader.readLine()) != null) {
                    //过滤空行
                    if (lineTxt.matches("[\\s]*") || lineTxt.matches("[ ]*##[\\s\\S]*")) {
                        continue;
                    }
                    //oSystem.out.println(lineTxt);
                    String[] item = lineTxt.split("\",\"");//CSV格式文件为逗号分隔符文件，这里根据逗号切分
                    String timeStr = item[2].replaceAll("\"","");
                    long time = Long.parseLong(timeStr);
                    pairs.add(Pair.of(item[1],time));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            //排序
            //使用匿名内部类
            pairs.sort(new Comparator() {
                @Override
                public int compare(Object o1, Object o2) {
                    Pair<String, Long> s1 = (Pair) o1;
                    Pair<String, Long> s2 = (Pair) o2;
                    return s1.getRight().compareTo(s2.getRight());
                }
            });

            int rank = 0;
            for (int j = 0; j < pairs.size(); j++) {
                if(!pairs.get(j).getLeft().contains("select")) {
                    rank = j + 1;
                    break;
                }
            }
            String[] line = new String[3];
            line[0] = Integer.toString(rank);
            line[1] = Long.toString(pairs.get(rank-1).getRight());
            line[2] = Long.toString(pairs.get(0).getRight());

            try (FileOutputStream fos = new FileOutputStream(joinOrderTimeFile,true);
                 OutputStreamWriter osw = new OutputStreamWriter(fos,
                         StandardCharsets.UTF_8);
                 CSVWriter writer = new CSVWriter(osw)) {
                writer.writeNext(line);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void getSameRandom(){
        String filedir = "D:\\artemis\\data0508\\oceanbase\\joinOrderRank-5-97\\cycle";
        String filedir2 = "D:\\artemis\\data0508\\postgresql\\joinOrderRank-5-385\\cycle";
        String outdir = "D:\\artemis\\data0508\\postgresql\\joinOrderRank-5-97\\cycle";

        File file = new File(filedir);

        String[] queryFiles = file.list();
        Map<Integer, String> queryFilesMap = new HashMap<>();
        for(String queryFile : queryFiles){
            if(!queryFile.contains("query")) continue;
            int index1 = queryFile.indexOf("_");
            int index2 = queryFile.indexOf(".");
            int queryNum = Integer.parseInt(queryFile.substring(index1+1, index2));
            queryFilesMap.put(queryNum, queryFile);
        }

//        for(int i=1;i<=50;i++) {
//            String lineTxt;
//            List<String[]> strList = new ArrayList<>();
//            String[] oriQuery = {"", "", ""};
//            List<String>  = new ArrayList<>();
//            try {
//                BufferedReader reader = new BufferedReader(new FileReader(filedir+File.separator+
//                        queryFilesMap.get(i)));//打开对应query的csv文件
//                reader.readLine();//第一行信息，为标题信息，不用
//
//                while ((lineTxt = reader.readLine()) != null) {
//                    //过滤空行
//                    if (lineTxt.matches("[\\s]*") || lineTxt.matches("[ ]*##[\\s\\S]*")) {
//                        continue;
//                    }
//                    //oSystem.out.println(lineTxt);
//                    String[] item = lineTxt.split("\",\"");//CSV格式文件为逗号分隔符文件，这里根据逗号切分
//                    String sql = item[1].replaceAll("\"","");
//                    int index1 = sql.indexOf("LEADING");
//
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            try{
//                BufferedReader reader2 = new BufferedReader(new FileReader(filedir2+File.separator+
//                        queryFilesMap.get(i)));//打开对应query的csv文件
//                reader2.readLine();//第一行信息，为标题信息，不用
//                String[] line = new String[3];
//                String outputfile = outdir + File.separator + queryFilesMap.get(i);
//                while ((lineTxt = reader2.readLine()) != null) {
//                    //过滤空行
//                    if (lineTxt.matches("[\\s]*") || lineTxt.matches("[ ]*##[\\s\\S]*")) {
//                        continue;
//                    }
//                    String[] item = lineTxt.split("\",\"");//CSV格式文件为逗号分隔符文件，这里根据逗号切分
//                    String rnk = item[0].replaceAll("\"","");
//                    if(rankList.contains(Integer.parseInt(rnk))){
//                        line[0] = rnk;
//                        line[1] = item[1].replaceAll("\"","");
//                        line[2] = item[2].replaceAll("\"","");
//                        try (FileOutputStream fos = new FileOutputStream(outputfile,true);
//                             OutputStreamWriter osw = new OutputStreamWriter(fos,
//                                     StandardCharsets.UTF_8);
//                             CSVWriter writer = new CSVWriter(osw)) {
//                            writer.writeNext(line);
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    }
//                }
//            }catch (Exception e){
//                e.printStackTrace();
//            }
//
//
//        }
    }
}
