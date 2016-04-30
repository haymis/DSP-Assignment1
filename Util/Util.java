package Util;

import org.apache.commons.codec.binary.Base64;


/**
 * Created by tomerh on 4/30/16.
 */
public class Util {

    public static String Base64BootstartBuilder(String InstanceType, String JavaArgs, String MainClass) {
        StringBuilder sb = new StringBuilder();
        sb.append("        #!/bin/sh\n" +
                "        BIN_DIR=/tmp/bin\n" +
                "        LOG_FILE=/tmp/log.txt\n" +
                "        now=$(date)\n" +
                "        echo $now >> $LOG_FILE\n" +
                "        echo \"Start '");
        sb.append(InstanceType);
        sb.append("' bootstrap.sh\" >> $LOG_FILE" +
                "        cp /tmp/dependencies.backup/* /usr/lib/jvm/jre-1.7.0-openjdk.x86_64/lib/ext/\n" +
                "        cd /tmp/bin/\n" +
                "        echo \"Start downloading file from storage:\" >> $LOG_FILE\n" +
                "        wget http://srulix.net/~haymi/Data.jar >> $LOG_FILE" +
                "        echo \"Running '");
        sb.append(InstanceType);
        sb.append("' \" >> $LOG_FILE\n" +
                "        java -cp /tmp/bin/Data.jar ");
        sb.append(JavaArgs);
        sb.append(" ");
        sb.append(MainClass);
        sb.append("\nshutdown -h now\n");
        return new String(Base64.encodeBase64(sb.toString().getBytes()));
    }
}

//
//
//#!/bin/sh
//        BIN_DIR=/tmp/bin
//        LOG_FILE=/tmp/log.txt
//
//        now=$(date)
//        echo $now >> $LOG_FILE
//        echo "Start Worker bootstrap.sh" >> $LOG_FILE
//
//        # Copy preinstalled depenedencies to the 'ext' folder:
//        cp /tmp/dependencies.backup/* /usr/lib/jvm/jre-1.7.0-openjdk.x86_64/lib/ext/
//
//# Make main app
//cd /tmp/bin/
//
//echo "Start downloading file from storage:" >> $LOG_FILE
//wget http://srulix.net/~haymi/Data.jar >> $LOG_FILE
//
//# Run Manager:
//echo "Running Worker::" >> $LOG_FILE
//java -cp /tmp/bin/Data.jar -Xms128m -Xmx768m Worker.Worker
