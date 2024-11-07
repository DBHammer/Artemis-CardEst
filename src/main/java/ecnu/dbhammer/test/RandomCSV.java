package ecnu.dbhammer.test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import com.opencsv.CSVWriter;

public class RandomCSV {
    public static void main(String[] args) {
        String filedir = "D:\\artemis\\data0508\\oceanbase\\join-more\\5\\cycle-385";
        String outdir = "D:\\artemis\\data0508\\oceanbase\\join-more\\5\\cycle-97";
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
            try {
                BufferedReader reader = new BufferedReader(new FileReader(filedir+File.separator+
                        queryFilesMap.get(i)));//打开对应query的csv文件
                reader.readLine();//第一行信息，为标题信息，不用

                while ((lineTxt = reader.readLine()) != null) {
                    //过滤空行
                    if (lineTxt.matches("[\\s]*") || lineTxt.matches("[ ]*##[\\s\\S]*")) {
                        continue;
                    }
                    String[] item = lineTxt.split("\",\"");//CSV格式文件为逗号分隔符文件，这里根据逗号切分
                    if(!item[1].contains("select"))
                        oriQuery = item;
                    else
                        strList.add(item);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            Collections.shuffle(strList);
            //各joinOrder与执行时间写入文件，方便后面分析
            String[] csvHeader = {"Rank", "SQL", "Time(ms)"};
            List<String[]> lineList = new ArrayList<>();
            lineList.add(csvHeader);
            lineList.add(oriQuery);

            for (int j = 0; j < 97; j++) {
                lineList.add(strList.get(j));
            }

            //写入文件
            output = outdir;
            File dir = new File(output);
            if (!dir.exists()) {
                dir.mkdir();
            }

            String joinOrderTimeFile = outdir + File.separator + "query_" + i + ".csv";
            try (FileOutputStream fos = new FileOutputStream(joinOrderTimeFile);
                 OutputStreamWriter osw = new OutputStreamWriter(fos,
                         StandardCharsets.UTF_8);
                 CSVWriter writer = new CSVWriter(osw)) {
                writer.writeAll(lineList);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

