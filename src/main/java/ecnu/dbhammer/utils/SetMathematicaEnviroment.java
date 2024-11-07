//package ecnu.dbhammer.utils;
//
//import com.wolfram.jlink.KernelLink;
//import com.wolfram.jlink.MathLinkException;
//import com.wolfram.jlink.MathLinkFactory;
//
//import ecnu.dbhammer.constant.LogLevelConstant;
//import ecnu.dbhammer.log.RecordLog;
//
//public class SetMathematicaEnviroment {
//    public static KernelLink SetLink(){
//        String jLinkPath = null;
//        String kernelLink = null;
//        if (System.getProperty("os.name").matches("Windows.*")){
//            jLinkPath = "D:\\Program Files\\Wolfram Research\\Mathematica\\12.0\\SystemFiles\\Links\\JLink";
//            kernelLink = "-linkmode launch -linkname 'D:\\Program Files\\Wolfram Research\\Mathematica\\12.0\\MathKernel.exe'";
//        }else if (System.getProperty("os.name").matches("Mac OS X")){
//            jLinkPath = "/Applications/Mathematica.app/Contents/SystemFiles/Links/JLink";
//            kernelLink = "-linkmode launch -linkname '\"/Applications/Mathematica.app/Contents/MacOS/MathKernel\" -mathlink'";
//        }else{
//            jLinkPath = "/usr/local/Wolfram/Mathematica/12.0/SystemFiles/Links/JLink";
//            kernelLink = "-linkmode launch -linkname '\"/usr/local/Wolfram/Mathematica/12.0/Executables/MathKernel\" -mathlink'";
//        }
//        System.setProperty("com.wolfram.jlink.libdir", jLinkPath);
//
//        KernelLink mathematicaLink = null;
//        try {
//            mathematicaLink = MathLinkFactory.createKernelLink(kernelLink);
//            // empty the compute enviroment
//            mathematicaLink.discardAnswer();
//        } catch (MathLinkException e) {
//            e.printStackTrace();
//        }
//        return mathematicaLink;
//    }
//}
