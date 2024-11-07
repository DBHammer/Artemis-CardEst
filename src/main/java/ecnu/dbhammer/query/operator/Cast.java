package ecnu.dbhammer.query.operator;

import ecnu.dbhammer.data.DataType;
import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.utils.Randomly;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Random;

import static java.math.RoundingMode.HALF_UP;

/**
 * @author xiangzhaokun
 * @ClassName Cast.java
 * @Description 算子类，operator包下的都是数据库算子类
 */
public class Cast implements operatorToken{

    private Attribute attribute;

    private DataType dataType;

    private int decimalM;
    private int decimalD;

    public Cast(Attribute attribute){
        this.attribute = attribute;
        this.dataType = DataType.DECIMAL;//暂时先用Decimal,
        //TODO Cast其他类型
    }

    public String getResult(BigDecimal bigDecimal){
        boolean isMinus = false;
        if(bigDecimal.compareTo(BigDecimal.ZERO)<0){
            bigDecimal = bigDecimal.multiply(new BigDecimal(-1));
            isMinus = true;
        }
        StringBuilder maxAns = new StringBuilder();
        for(int i=0;i<decimalM-decimalD;i++){
            maxAns.append("9");
        }
        if(decimalD>0){
            maxAns.append(".");
            for(int i=0;i<decimalD;i++){
                maxAns.append("9");
            }
        }
        DecimalFormat decimalFormat = new DecimalFormat("0.0000000000000000000000000000000#");
        String afterTrans = decimalFormat.format(bigDecimal);
        bigDecimal = new BigDecimal(afterTrans);
        bigDecimal = bigDecimal.setScale(decimalD,HALF_UP);

        //System.out.println("分割后："+decimalD);


        //System.out.println(Arrays.toString(bigDecimal.toPlainString().split("\\.")));


        String left = bigDecimal.toPlainString().split("\\.")[0];//
        String right;
        if(decimalD==0){
            right="";
        }else {
            right = bigDecimal.toPlainString().split("\\.")[1];//数据小数部分
        }
        if(left.length() + right.length() > decimalM){
            if(isMinus){
                return "-"+maxAns;
            }else{
                return maxAns.toString();
            }
        }else{
            if(isMinus){
                return "-"+bigDecimal.toString();
            }else{
                return bigDecimal.toString();
            }
        }
    }

    @Override
    public String getExpressionText() {
        if(this.dataType != DataType.DECIMAL)
        return "cast(" + attribute.getFullAttrName() + " as " + dataType+")";
        else{
            int options = Randomly.fromOptions(0,1);
            if(options==1) {
                //M表示精度，范围[1,65]
                //D表示小数点后的位数，D<=M,范围[0,30]
                //Decimal(M,D)表示可以存储D位小数的M位数
                Random random = new Random();
                decimalM = (int) (Math.random() * 65 + 1);


                decimalD = (int) (Math.random() * Math.min(decimalM, 30));


                return "cast(" + attribute.getFullAttrName() + " as " + dataType + "(" + decimalM + "," + decimalD + ")" + ")";
            }else {
                decimalM = 10;
                decimalD = 0;
                return "cast(" + attribute.getFullAttrName() + " as " + dataType+")";
            }
        }
    }
}
