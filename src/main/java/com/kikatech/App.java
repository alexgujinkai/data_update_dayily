package com.kikatech;

import com.kikatech.krupp.athena.AthenaQuery;
import com.kikatech.krupp.string.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.sql.SQLException;
import java.util.List;

/**
 * Athena query demo
 */
public class App {
    private static Logger log = LogManager.getLogger(App.class);
    private static final String ACCESS_KEY = "AKIAJVUJAI67WU3CQBSQ";

    public static void main(String[] args) throws SQLException {
        AthenaQuery query = new AthenaQuery(ACCESS_KEY);
        String cdate = args[0];
        String sql = String.format("SELECT cdate,'new' AS type,channel,nation,appversion,modelname,COUNT(distinct deviceuid) AS uv\n" +
                "FROM dnu\n" +
                "WHERE cdate = %s\n" +
                "AND app_key IN ('ef619d28d539ce998fe24dbf4a920dfa', 'c2fc588737ffd00004184fffa230da24', '780e860da928a3c5d8e7ff81d46eeb60', '70dfa592c132bf07771f420c18b7d77d', '08c0d547d43e452ca2d0be743e757ffe', '49466e63a770cb3655e89a93c028f194', '0f9bbfc003de2aa752dfa267831deb41', '6b451fda0a10c5c7a029ac3875ca6f68', '3417544264aab7fc1f4c2cf51422c1de', 'f5c5669d904753cc7c4f38cdc4ae5b95', '56bcfd92719a6fd4c7971d05452bcbfc', '89c89930c0769411ff7eda418dfd1b67', 'c6c866e4ec65c455ac9c89f06714232f', 'b9b9c8c08a1f3b6ec83dfa80080f15b4', '668440552123d1f32488d023e1242ccf', '1e1dac200531c39bb5cf867c9214c21f', '41ee7a30fe5e171a547cd5f636ebdf88', '7789d822a974e61d43ad4e016d841581')\n" +
                "GROUP BY cdate,channel,nation,appversion,modelname;",cdate);
        log.info(sql);
        List<String[]> result = query.query(sql);
        try{
            BufferedWriter bw = new BufferedWriter(new FileWriter("/home/gujinkai/test/yuzhuang.csv"));
            for (String[] line : result) {
                for (String str : line) {
                    bw.write(str);
                    bw.write(",");
                }
                bw.write("\n");
                log.info(ToString.from(line));
            }
            bw.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }


}
