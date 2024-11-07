//package ecnu.dbhammer.utils;
//
//import com.wolfram.jlink.KernelLink;
//import com.wolfram.jlink.MathLinkException;
//import com.wolfram.jlink.MathLinkFactory;
//
//public class MathematicaComputation {
//    public static void computeTest(KernelLink ml){
//        String[] str = new String[2];
//        str[0]="Reduce[( + (10.00 * k^2 + 671.00 * k + -7565.00) / 1.73 + 298.01 <= 49681239.72) && ( Mod[ k ,4] == 0) &&( 0 <= k < 6301), k , Integers , Method -> {\"DiscreteSolutionBound\" -> 9999999999999999}]//ToString";
//
//        str[1]="Reduce[( + (10.00 * k^2 + 671.00 * k + -7565.00) / 1.73 + 298.01 <= 49681239.72) &&( 0 <= k < 6301), k , Integers , Method -> {\"DiscreteSolutionBound\" -> 9999999999999999}]//ToString";
//
//        for(int i=0;i<2;i++) {
//            System.out.println("第"+i+"次计算");
//            // 交给mathematica计算
//            long mathStart = System.currentTimeMillis();
//
//            try {
//                ml.evaluate(str[i]);
//                ml.waitForAnswer();
//                String resultFromMath = ml.getString();
//                //ml.abandonEvaluation();
//                long mathEnd = System.currentTimeMillis();
//                System.out.println("mathmetical消耗时间：" + (mathEnd - mathStart) + "ms");
//                resultFromMath = resultFromMath.replaceAll(" ", "");
//                System.out.println("Mathmetical计算结果：");
//                System.out.println(resultFromMath);
//            } catch (MathLinkException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public static void main(String[] args) {
//
//        // 利用mathematica求解
//        long mathStartAll = System.currentTimeMillis();
//
//        System.setProperty("com.wolfram.jlink.libdir", "/Applications/Mathematica.app/Contents/SystemFiles/Links/JLink");
//        KernelLink ml = null;
//        try {
//            ml = MathLinkFactory.createKernelLink("-linkmode launch -linkname '\"/Applications/Mathematica.app/Contents/MacOS/MathKernel\" -mathlink'");
//            ml.discardAnswer();// empty the compute enviroment
//        } catch (MathLinkException e) {
//            e.printStackTrace();
//        }
//        computeTest(ml);
//        long mathEndAll = System.currentTimeMillis();
//        System.out.println(mathEndAll-mathStartAll);
//
//
//    }
//}
