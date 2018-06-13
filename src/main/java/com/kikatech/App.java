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
    private static String ACCESS_KEY = "";

    public static void main(String[] args) throws SQLException {
        ACCESS_KEY =args[0];
        AthenaQuery query = new AthenaQuery(ACCESS_KEY);
        String cdate = args[1];
        String sql = String.format("SELECT b.app_key,\n" +
                "         b.cdate,\n" +
                "         b.increase,\n" +
                "         c.DAU,\n" +
                "         c.DAU_popup,\n" +
                "         d.send_word_count,\n" +
                "         e.send_word_count_im,\n" +
                "         0,\n" +
                "         f.send_user,\n" +
                "         f.send_count,\n" +
                "         f.send_count / (f.send_user * 1.0),\n" +
                "         f.send_count/(c.DAU_popup * 1.0),\n" +
                "         0,\n" +
                "         f.pop_user,\n" +
                "         f.pop_user / (c.DAU_popup * 1.0),\n" +
                "         f.pop_count,\n" +
                "         f.pop_send_user,\n" +
                "         f.pop_send_user / (f.pop_user * 1.0),\n" +
                "         f.pop_send_count,\n" +
                "         f.pop_send_count / (f.pop_count * 1.0) AS ctr,\n" +
                "         f.pop_send_count / (f.pop_user * 1.0),\n" +
                "         f.pop_send_count / (f.pop_send_user * 1.0),\n" +
                "         f.sticker1_uv,\n" +
                "         f.sticker1_send_user,\n" +
                "         f.sticker1_send_count,\n" +
                "         c.sticker2_DAU,\n" +
                "         f.sticker2_uv,\n" +
                "         f.sticker2_send_user,\n" +
                "         f.sticker2_send_count,\n" +
                "         f.fun_top_pv,\n" +
                "         f.fun_top_uv,\n" +
                "         f.fun_top_pv/(f.fun_top_uv * 1.0) as fun_top_ctr,\n" +
                "         f.world_cup_show_pv,\n" +
                "         f.world_cup_show_uv,\n" +
                "         f.world_cup_send_pv,\n" +
                "         f.world_cup_send_uv,\n" +
                "         f.world_cup_send_pv/(f.world_cup_show_pv * 1.0) as world_cup_ctr,\n" +
                "         f.world_cup_send_uv/(f.world_cup_show_uv * 1.0)\n" +
                "         from \n" +
                "    (\n" +
                "        SELECT cdate,\n" +
                "         count(distinct deviceuid) AS increase,\n" +
                "         app_key\n" +
                "    FROM dnu\n" +
                "    WHERE app_key in ('e2934742f9d3b8ef2b59806a041ab389','78472ddd7528bcacc15725a16aeec190','4e5ab3a6d2140457e0423a28a094b1fd')\n" +
                "      AND cdate  = %s\n" +
                "    GROUP BY  cdate,app_key\n" +
                "    ORDER BY  cdate desc\n" +
                "    )b\n" +
                "\n" +
                "JOIN \n" +
                "    ( \n" +
                "        SELECT cdate, app_key,\n" +
                "\n" +
                "         count(distinct (case\n" +
                "        WHEN app_key LIKE 'e29%%'\n" +
                "            AND appversion>='4.8.1.611' THEN\n" +
                "        deviceuid\n" +
                "        WHEN app_key LIKE '784%%'\n" +
                "            AND appversion>='5.5.8.1620' THEN\n" +
                "        deviceuid\n" +
                "        WHEN app_key LIKE '4e5%%'\n" +
                "            AND appversion>='3.4.252' THEN\n" +
                "        deviceuid end)) AS sticker2_DAU,\n" +
                "        \n" +
                "        count(distinct deviceuid) AS DAU,\n" +
                "        \n" +
                "        count(distinct (case\n" +
                "        WHEN app_key LIKE 'e29%%'\n" +
                "            AND appversion>='4.8.1.670' THEN\n" +
                "        deviceuid\n" +
                "        WHEN app_key LIKE '784%%'\n" +
                "            AND appversion>='5.5.8.1670' THEN\n" +
                "        deviceuid\n" +
                "        WHEN app_key LIKE '4e5%%'\n" +
                "            AND appversion>='3.4.263' THEN\n" +
                "        deviceuid end)) AS DAU_popup\n" +
                "        \n" +
                "    FROM service_merge_daily\n" +
                "    WHERE app_key in ('e2934742f9d3b8ef2b59806a041ab389','78472ddd7528bcacc15725a16aeec190','4e5ab3a6d2140457e0423a28a094b1fd')\n" +
                "      AND cdate = %s\n" +
                "            AND iskeyboard=1\n" +
                "    GROUP BY  cdate,app_key\n" +
                "    ORDER BY  cdate DESC \n" +
                "\n" +
                "    )c\n" +
                "    ON b.cdate = c.cdate\n" +
                "        AND b.app_key = c.app_key\n" +
                "JOIN \n" +
                "    (\n" +
                "        SELECT cdate,\n" +
                "         count(deviceuid)AS send_word_count,\n" +
                "         app_key\n" +
                "    FROM word_trace_desensitization\n" +
                "    WHERE app_key in ('e2934742f9d3b8ef2b59806a041ab389','78472ddd7528bcacc15725a16aeec190','4e5ab3a6d2140457e0423a28a094b1fd')\n" +
                "      AND cdate  = %s\n" +
                "    GROUP BY  cdate,app_key\n" +
                "    ORDER BY  cdate DESC \n" +
                "    )d\n" +
                "    ON d.cdate = c.cdate\n" +
                "        AND d.app_key = c.app_key\n" +
                "JOIN \n" +
                "    (SELECT cdate,\n" +
                "         count(deviceuid) AS send_word_count_im,\n" +
                "         app_key\n" +
                "    FROM word_trace_desensitization\n" +
                "    WHERE app_key in ('e2934742f9d3b8ef2b59806a041ab389','78472ddd7528bcacc15725a16aeec190','4e5ab3a6d2140457e0423a28a094b1fd')\n" +
                "     AND cdate  = %s\n" +
                "            AND application in('com.facebook.orca','com.whatsapp', 'com.facebook.katana', 'org.telegram.messenger', 'com.google.android.talk', 'com.twitter.android', 'com.google.android.apps.plus', 'com.google.android.apps.messaging', 'com.bbm', 'com.android.mms', 'com.facebook.lite', 'com.samsuing.android.messaging', 'jp.naver.line.android', 'com.tencent.mobileqq', 'com.tencent.mm')\n" +
                "    GROUP BY  cdate,app_key\n" +
                "    ORDER BY  cdate desc\n" +
                "    )e\n" +
                "    ON d.cdate = e.cdate\n" +
                "        AND d.app_key = e.app_key\n" +
                "\n" +
                "JOIN \n" +
                "    (\n" +
                "\n" +
                "SELECT cdate,\n" +
                "         app_key,\n" +
                "         \n" +
                "         count(case\n" +
                "    WHEN layout = 'keyboard_sticker2_suggestion_pop'\n" +
                "        AND item = 'send' THEN\n" +
                "    deviceuid end) AS pop_send_count, \n" +
                "    \n" +
                "    count(distinct case\n" +
                "    WHEN layout = 'keyboard_sticker2_suggestion_pop'\n" +
                "        AND item = 'send' THEN\n" +
                "    deviceuid end) AS pop_send_user,\n" +
                "    \n" +
                "    count(case\n" +
                "    WHEN (layout = 'keyboard_sticker' or layout = 'keyboard_gif' )\n" +
                "        AND item = 'show' THEN\n" +
                "    deviceuid end) AS sticker1_pv,\n" +
                "    \n" +
                "    count(distinct case\n" +
                "    WHEN (layout = 'keyboard_sticker' or layout = 'keyboard_gif' )\n" +
                "        AND item = 'show' THEN\n" +
                "    deviceuid end) AS sticker1_uv,\n" +
                "    \n" +
                "    count( case\n" +
                "    WHEN (layout = 'keyboard_sticker' or layout = 'keyboard_gif' )\n" +
                "        AND (item = 'online_send' or item = 'send'  or item = 'app_send') THEN\n" +
                "    deviceuid end) AS sticker1_send_count,\n" +
                "    \n" +
                "    count(distinct case\n" +
                "    WHEN (layout = 'keyboard_sticker' or layout = 'keyboard_gif' )\n" +
                "        AND (item = 'online_send' or item = 'send'  or item = 'app_send') THEN\n" +
                "    deviceuid end) AS sticker1_send_user,\n" +
                "    \n" +
                "    \n" +
                "    count( case\n" +
                "    WHEN (layout = 'keyboard_sticker2' or layout = 'keyboard_sticker2_recommend' or layout = 'keyboard_sticker2_content' )\n" +
                "        AND (opertype = 'show' ) THEN\n" +
                "    deviceuid end) AS sticker2_pv,\n" +
                "    \n" +
                "    count(distinct case\n" +
                "    WHEN (layout = 'keyboard_sticker2' or layout = 'keyboard_sticker2_recommend' or layout = 'keyboard_sticker2_content' )\n" +
                "        AND (opertype = 'show' ) THEN\n" +
                "    deviceuid end) AS sticker2_uv,\n" +
                "    \n" +
                "    count( case\n" +
                "    WHEN (layout = 'keyboard_sticker2_recommend' or layout = 'keyboard_sticker2_content' )\n" +
                "        AND (item = 'send' ) THEN\n" +
                "    deviceuid end) AS sticker2_send_count,\n" +
                "    \n" +
                "     count(distinct case\n" +
                "    WHEN (layout = 'keyboard_sticker2_recommend' or layout = 'keyboard_sticker2_content' )\n" +
                "        AND (item = 'send' ) THEN\n" +
                "    deviceuid end) AS sticker2_send_user,\n" +
                "    \n" +
                "    count( case\n" +
                "    WHEN (layout = 'sticker2_store_trending' or layout = 'sticker2_store_all' or layout = 'sticker2_detail' )\n" +
                "        AND (item = 'add' ) THEN\n" +
                "    deviceuid end) AS sticker_add,\n" +
                "    \n" +
                "    count(distinct case\n" +
                "    WHEN (layout = 'sticker2_store_trending' or layout = 'sticker2_store_all' or layout = 'sticker2_detail' )\n" +
                "        AND (item = 'add' ) THEN\n" +
                "    deviceuid end) AS sticker_add_user,\n" +
                "\n" +
                "   count( case\n" +
                "    WHEN (layout = 'sticker2_suggestion')\n" +
                "        AND (item = 'pop' ) THEN\n" +
                "    deviceuid end) AS pop_count,\n" +
                "\n" +
                "    count( distinct case\n" +
                "    WHEN (layout = 'sticker2_suggestion')\n" +
                "        AND (item = 'pop' ) THEN\n" +
                "    deviceuid end) AS pop_user,\n" +
                "\n" +
                "    count( case\n" +
                "    WHEN layout IN ('emoji','keyboard_sticker','keyboard_gif','keyboard_sticker2_suggestion_pop','keyboard_sticker2','keyboard_sticker2_content','keyboard_sticker2_recommend')\n" +
                "            AND item in('emoji_recent_recommendation_send','online_send','send','app_send')\n" +
                "             THEN  deviceuid end) AS send_count,\n" +
                "\n" +
                "    count(distinct case\n" +
                "    WHEN layout IN ('emoji','keyboard_sticker','keyboard_gif','keyboard_sticker2_suggestion_pop','keyboard_sticker2','keyboard_sticker2_content','keyboard_sticker2_recommend')\n" +
                "            AND item in('emoji_recent_recommendation_send','online_send','send','app_send')\n" +
                "             THEN  deviceuid end) AS send_user ,  \n" +
                "\n" +
                "    count(case when layout = 'fun_top' and item = 'click' then deviceuid end) as fun_top_pv, \n" +
                "    count(distinct case when layout = 'fun_top' and item = 'click' then deviceuid end) as fun_top_uv, \n" +
                "    count(case when layout = 'world_cup' and item = 'show' then deviceuid end) as world_cup_show_pv,\n" +
                "    count(distinct case when layout = 'world_cup' and item = 'show' then deviceuid end) as world_cup_show_uv,\n" +
                "    count(case when layout = 'world_cup' and item = 'send' then deviceuid end) as world_cup_send_pv,\n" +
                "    count(distinct case when layout = 'world_cup' and item = 'send' then deviceuid end) as world_cup_send_uv\n" +
                "FROM event\n" +
                "WHERE app_key in ('e2934742f9d3b8ef2b59806a041ab389','78472ddd7528bcacc15725a16aeec190','4e5ab3a6d2140457e0423a28a094b1fd')\n" +
                "      AND cdate  = %s\n" +
                "and     layout IN ('sticker2_suggestion' ,'keyboard_sticker2_suggestion_pop' ,'keyboard_sticker','keyboard_gif'  ,'keyboard_sticker2','keyboard_sticker2_recommend','keyboard_sticker2_content', 'sticker2_store_trending','sticker2_store_all','sticker2_detail','emoji','world_cup','fun_top')\n" +
                "        AND item in('pop' ,'send' ,'show' ,'online_send','app_send'  ,'add','container','content','emoji_recent_recommendation_send','click')\n" +
                "GROUP BY  cdate, app_key   )f\n" +
                "    ON f.cdate = e.cdate\n" +
                "        AND f.app_key = e.app_key\n" +
                "        \n" +
                "        \n" +
                "   order by b.app_key,b.cdate desc;",cdate,cdate,cdate,cdate,cdate);
        log.info("sql:\n"+sql);
        List<String[]> result = query.query(sql);
        try{
            BufferedWriter bw = new BufferedWriter(new FileWriter("/home/gujinkai/date/内容发送数据日报1.0.txt"));
            for (String[] line : result) {
                for (String str : line) {
                    bw.write(str);
                    bw.write("\t");
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
