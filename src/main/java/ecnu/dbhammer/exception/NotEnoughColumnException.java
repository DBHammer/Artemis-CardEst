package ecnu.dbhammer.exception;

/**
 * @author xiangzhaokun
 * @ClassName NotEnoughColumnException.java
 * @Description 代码规范化的一些异常类，该包下可实现更多的异常类，并显示信息，使得代码更加规范
 * @createTime 2021年12月03日 16:09:00
 */
public class NotEnoughColumnException extends Exception{
    public NotEnoughColumnException(String message){
        super(message);
    }


}
