package ecnu.dbhammer.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.jcraft.jsch.*;

/**
 * SFTP连接类，用来在Windows和Linux之间传输文件
 * 
 */
public class SFTPConnection
{
    private  JSch jsch;
    private  Session session;

    public SFTPConnection(){
        //jsch = new JSch();
    }

    /**
     * 连接到指定的IP
     * 
     * @throws JSchException
     */
    public void connect(String serverUser, String serverPassword, String host, int port) throws JSchException
    {
        jsch = new JSch();// 创建JSch对象
        session = jsch.getSession(serverUser, host, port);// 根据用户名、主机ip、端口号获取一个Session对象
        session.setPassword(serverPassword);// 设置密码

        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);// 为Session对象设置properties
        session.setTimeout(20000);// 设置超时
        session.connect();// 通过Session建立连接
    }

    /**
     * 关闭连接
     */
    public  void close()
    {
        session.disconnect();
    }

    /**
     * 执行相关的命令
     * 
     * @throws JSchException
     */
    public  String[] execCmd(String command) throws JSchException
    {
        List<String> result = new ArrayList<String>();
        BufferedReader reader = null;
        Channel channel = null;
        try
        {
            if (command != null)
            {
                channel = session.openChannel("exec");
                ((ChannelExec) channel).setCommand(command);
                channel.connect();

                InputStream in = channel.getInputStream();
                reader = new BufferedReader(new InputStreamReader(in));
                String buf = null;
                while ((buf = reader.readLine()) != null)
                    result.add(buf);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            try
            {
                reader.close();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            channel.disconnect();
        }
        return result.toArray(new String[0]);
    }

    /**
     * 上传文件
     *
     * @param directory 上传的目录
     * @param uploadFile 要上传的文件
     * @throws JSchException
     * @throws SftpException
     * @throws FileNotFoundException
     */
    public  void upload(String directory, String uploadFile) throws JSchException, FileNotFoundException, SftpException
    {

        ChannelSftp channelSftp = (ChannelSftp) session.openChannel("sftp");
        channelSftp.connect();
        SftpATTRS attrs = null;
        try{
            attrs = channelSftp.stat(directory);
        }catch (Exception e){
            e.printStackTrace();
        }
        if(attrs != null)
            channelSftp.rm(directory+"/*"); //清空文件夹
        else
            channelSftp.mkdir(directory);
        // System.out.println(channelSftp.isConnected());
        channelSftp.cd(directory);
        File file = new File(uploadFile);
        if (file.isDirectory())
        {
            String[] files = file.list();
            for (String ele : files)
            {
                File file1 = new File(file.getAbsolutePath() + "/" + ele);
                if (file1.isDirectory())
                    upload(directory, file1.getAbsolutePath());
                else
                    channelSftp.put(new FileInputStream(file1), file1.getName());
            }

        }
        if (file.isFile()){
            channelSftp.put(new FileInputStream(file), file.getName());
        }
        channelSftp.disconnect();
    }

    /**
     * 下载文件
     * 
     * @param src
     * @param dst
     * @throws JSchException
     * @throws SftpException
     */
    public  void download(String src, String dst) throws JSchException, SftpException
    {

        // src linux服务器文件地址，dst 本地存放地址
        ChannelSftp channelSftp = (ChannelSftp) session.openChannel("sftp");
        channelSftp.connect();
        channelSftp.get(src, dst);
        channelSftp.quit();
    }

    /**
     * 删除文件
     *
     * @param directory 要删除文件所在目录
     * @param deleteFile 要删除的文件
     * @throws SftpException
     * @throws JSchException
     */
    public void delete(String directory, String deleteFile) throws SftpException, JSchException
    {
        ChannelSftp channelSftp = (ChannelSftp) session.openChannel("sftp");
        channelSftp.connect();
        channelSftp.cd(directory);
        channelSftp.rm(deleteFile);
    }

    //清空文件夹
    public void clearDir(String directory) throws SftpException, JSchException
    {
        ChannelSftp channelSftp = (ChannelSftp) session.openChannel("sftp");
        channelSftp.connect();
        channelSftp.rm(directory+"/*");
        channelSftp.disconnect();
    }
}
