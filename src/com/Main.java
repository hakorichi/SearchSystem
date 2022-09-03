package com;
import java.text.SimpleDateFormat;
import java.util.*;


public class Main
{
    public static void main(String[] args)
    {
            List<String> pageList = new ArrayList<String>();
            pageList.add("https://lenta.ru");
            Map<String, List<String>> parentUrls = new HashMap<String, List<String>>();
            parentUrls.put("", pageList);
            try
            {
                Crawler my_crawler = new Crawler("DATA_BASE_.db", false);
                my_crawler.dropDB();
                my_crawler.createIndexTables();
                my_crawler.crawl(parentUrls, 1);
                my_crawler.finalize();
                Date date = new Date();
                SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
                System.out.println(formatter.format(date) + " - Индексация завершена");
            }
            catch (Exception e)
            {
                Date date = new Date();
                SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
                System.out.println(formatter.format(date) + " - Индексация завершенас ошибкой:");
                e.printStackTrace();
            }



    }
}
