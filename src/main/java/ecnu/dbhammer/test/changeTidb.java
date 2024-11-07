package ecnu.dbhammer.test;

import com.opencsv.CSVWriter;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class changeTidb {
    public static void main(String[] args) {
        File file = new File("D:\\artemis\\data0508\\tidb\\joinOrderRank-9\\star");
        String output = "";
        String output_type = "";
        String[] queryFiles = file.list();
        Map<Integer, String> queryFilesMap = new HashMap<>();
        System.out.println("%%%");
        for(String queryFile : queryFiles){
            if(!queryFile.contains("query")) continue;
            int index1 = queryFile.indexOf("_");
            int index2 = queryFile.indexOf(".");
            int queryNum = Integer.parseInt(queryFile.substring(index1+1, index2));
            queryFilesMap.put(queryNum, queryFile);

        }

        for(int i=1;i<=queryFilesMap.size();i++) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(file + File.separator + queryFilesMap.get(i)));//打开对应query的csv文件
                reader.readLine();//第一行信息，为标题信息，不用
                int queryIndex = 0;
                List<String> joinOrderQuerys = new LinkedList<>();
                String lineTxt = "";
                List<String[]> outList = new ArrayList<>();
                String[] csvHeader = {"Rank", "SQL", "Time(ms)"};
                outList.add(csvHeader);
                while ((lineTxt = reader.readLine()) != null) {
                    //过滤空行

                    if (lineTxt.matches("[\\s]*") || lineTxt.matches("[ ]*##[\\s\\S]*")) {
                        continue;
                    }
                    String[] item = lineTxt.split("\",\"");//CSV格式文件为逗号分隔符文件，这里根据逗号切分
                    String query = item[1];
                    //Table [tableName=table_4, tableSize=0],Table [tableName=table_15, tableSize=0],Table [tableName=table_0, tableSize=0]

                    if(!lineTxt.contains("select")){
                        String outText = "";
                        String[] items = query.split("]");
                        for(int j=0;j<items.length;j++){

                            if(items[j].contains("table_")){
                                int index1 = items[j].indexOf("_");
                                int index2 = items[j].indexOf(",",index1);

                                String name = "table"+items[j].substring(index1,index2);
                                if(j==0) outText += name;
                                else outText += ","+name;
                            }
                        }
                        item[1] = outText;
                        outList.add(item);
                    }else
                        outList.add(item);
                }

                String joinOrderTimeFile =  "D:\\artemis\\data0508\\tidb-2\\joinOrderRank-9\\star\\query_" + i + ".csv";
                try (FileOutputStream fos = new FileOutputStream(joinOrderTimeFile);
                     OutputStreamWriter osw = new OutputStreamWriter(fos,
                             StandardCharsets.UTF_8);
                     CSVWriter writer = new CSVWriter(osw)) {
                    writer.writeAll(outList);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

