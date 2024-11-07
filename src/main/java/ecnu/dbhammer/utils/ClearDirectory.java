package ecnu.dbhammer.utils;

import ecnu.dbhammer.configuration.Configurations;
import org.apache.commons.io.FileUtils;

import java.io.File;

public class ClearDirectory {
    public static boolean deleteDir(String path){
        File file = new File(path);

        if(!file.exists()){//判断是否待删除目录是否存在
            System.err.println(path+"文件夹不存在!");
            return false;
        }

        String[] content = file.list();//取得当前目录下所有文件和文件夹
        for(String name : content){
            File temp = new File(path, name);
            if(temp.isFile()) {
                if ((name.split("\\.")[1].equals("txt"))||(name.split("\\.")[1].equals("png"))
                ||(name.split("\\.")[1].equals("dot"))||(name.split("\\.")[1].equals("csv"))) {//为了安全只删除txt文件
                    temp.delete();
                }
            }else{
                if(temp.isDirectory()){
                    try {
                        FileUtils.deleteDirectory(temp);
                    }catch (Exception e){
                        e.printStackTrace();
                    }

                }
            }
        }
        return true;
    }

    public static void main(String[] args) {
        String fileDirectory = Configurations.getDataoutputDir();
        boolean status = deleteDir(fileDirectory);
        if(status){
            System.out.println("清空文件夹成功！");
        }else{
            System.out.println("清空文件夹失败！");
        }

    }

}
