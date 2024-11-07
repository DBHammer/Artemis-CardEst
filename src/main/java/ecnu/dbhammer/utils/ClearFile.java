package ecnu.dbhammer.utils;

import ecnu.dbhammer.configuration.Configurations;

import java.io.*;

public class ClearFile {
    public static void clearFile(String queryExecuteResultPath){

        File file = new File(queryExecuteResultPath);
        if(!file.exists()) {
            System.out.println("文件不存在,创建");
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            System.out.println(queryExecuteResultPath+"文件已存在,清空");
            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(queryExecuteResultPath), Configurations.getEncodeType()))) {
                bw.write("");
                bw.flush();
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }
}
