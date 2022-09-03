package com;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.text.SimpleDateFormat;
import java.util.*;

public class Main_Form  extends JFrame
{


    private JPanel Panel;
    private JCheckBox CheckBox_1;
    private JCheckBox CheckBox_2;
    private JCheckBox CheckBox_3;
    private JCheckBox CheckBox_4;
    private JCheckBox CheckBox_5;
    private JCheckBox CheckBox_6;
    private JCheckBox CheckBox_7;
    private JButton Button_Start;
    private JTextArea TextArea_list;
    private JTextField TExtField_Depth;
    private JCheckBox CheckBox_8;
    private JCheckBox CheckBox_9;


    public static void main(String[] args)
    {
        JFrame frame = new JFrame("App");
        frame.setContentPane(new Main_Form().Panel);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);


    }

    public Main_Form()
    {

        TextArea_list.setText
                ("http://lenta.ru\n" +
                        "https://www.drom.ru/\n" +
                        "https://ngs.ru/\n" +
                        "https://anna-news.info/\n" +
                        "https://ru.wikipedia.org/\n" +
                        "http://habr.com\n");


        Button_Start.addActionListener(new ActionListener()
        {
            @Override
            public void actionPerformed(ActionEvent e)
            {

                List<String> pageList = Arrays.asList(TextArea_list.getText().split("\n"));
                Map<String, List<String>> parentUrls = new HashMap<String, List<String>>();
                parentUrls.put("", pageList);

                int Depth = Integer.parseInt(TExtField_Depth.getText());
                try
                {
                    Crawler my_crawler = new Crawler("DATA_BASE_.db", false);

                    my_crawler.NoNumbers = CheckBox_1.isSelected();
                    my_crawler.NoPrepositions = CheckBox_2.isSelected();
                    my_crawler.NoUnions = CheckBox_3.isSelected();
                    my_crawler.NoInterjections = CheckBox_4.isSelected();
                    my_crawler.NoEnglishWord = CheckBox_5.isSelected();
                    my_crawler.NoNames = CheckBox_6.isSelected();
                    my_crawler.NoPronouns = CheckBox_7.isSelected();
                    my_crawler.IndexesValue = CheckBox_8.isSelected();
                    my_crawler.IndexesName = CheckBox_9.isSelected();

                    my_crawler.dropDB();
                    my_crawler.createIndexTables();
                    my_crawler.crawl(parentUrls, Depth);
                    my_crawler.finalize();
                    Date date = new Date();
                    SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
                    System.out.println(formatter.format(date) + " - Индексация завершена");
                }
                catch (Exception error)
                {
                    Date date = new Date();
                    SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
                    System.out.println(formatter.format(date) + " - Индексация завершенас ошибкой:");
                    error.printStackTrace();
                }

            }
        });
    }


}
