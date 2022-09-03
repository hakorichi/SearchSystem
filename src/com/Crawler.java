package com;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Scanner;

public class Crawler
{
    private Connection conn; // соединение с БД в локальном файле
    private boolean debug;
    private int location;

    private List<String> filter_Prepositions = new ArrayList<String>();
    private List<String> filter_Unions = new ArrayList<String>();
    private List<String> filter_Interjections = new ArrayList<String>();
    private List<String> Eng_letter = new ArrayList<String>();
    private List<String> Names_Array = new ArrayList<String>();
    private List<String> Pronouns_Array = new ArrayList<String>();


    public boolean NoNumbers = true;
    public boolean NoPrepositions = true;
    public boolean NoUnions = true;
    public boolean NoInterjections = true;
    public boolean NoEnglishWord = true;
    public boolean NoNames = true;
    public boolean NoPronouns = true;

    public boolean IndexesName = true;
    public boolean IndexesValue = true;

    /* Конструктор инициализации паука с параметрами БД */
    protected Crawler(String fileName, boolean debug) throws SQLException
    {
        this.debug = debug;
        if (debug) System.out.println("Конструктор");
        String db_url = "jdbc:sqlite:" + fileName; // формируемая строка дляподключения к локальному файлу14
        this.conn = DriverManager.getConnection(db_url);
        this.conn.setAutoCommit(true); // включить режим автоматической фиксации (commit) изменений БД

        filter_Prepositions.addAll(Arrays.asList(new String[]{"от","в", "без", "до", "для", "за", "через", "над", "по",  "о", "про", "на", "из", "у", "около", "под", "к", "перед", "при", "с", "между" , "из-за"}));

        filter_Unions.addAll(Arrays.asList(new String[]{"а", "но", "да", "и", "или", "либо", "то", "не", "так", "тоже", "также", "как", "только", "когда", "потому", "что", "если", "хотя", "на", "ни"}));

        filter_Interjections.addAll(Arrays.asList(new String[]{"ах", "ух", "эх", "эй", "э", "гм", "увы", "ого", "м-да", "бис", "ба", "фи", "эге", "аи", "тс", "цыц", "вон", "алло"}));

        Eng_letter.addAll(Arrays.asList(new String[]{"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z",
                                                     "A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z",}));

        Pronouns_Array.addAll(Arrays.asList(new String[]{"я", "мы", "ты", "он", "она", "оно", "они", "себя", "мой", "твой", "ваш", "наш","свой","его","ее", "их", "то", "это", "тот", "этот", "такой", "таков", "столько", "весь", "всякий", "сам", "самый", "каждый", "любой", "иной", "другой", "кто","что","какой","каков","чей","сколько", "никто", "ничто", "некого", "нечего", "никакой", "ничей", "нисколько", "кто-то", "кое-кто", "кто-нибудь", "кто-либо", "что-то", "кое-что", "что-нибудь", "что-либо", "какой-то", "какой-либо", "какой-нибудь", "некто", "нечто", "некоторый", "некий"}));

        try
        {
            Scanner sc = new Scanner(new File("Names.txt"));
            while (sc.hasNextLine())
            {
                Names_Array.add(sc.nextLine());
            }
        }
        catch(FileNotFoundException ex)
        { }


    }

    /* Метод финализации работы с БД */
    protected void finalize() throws SQLException
    {
        conn.close(); // закрыть соединение
    }

    /* Удаление таблиц в БД */
    protected void dropDB() throws SQLException
    {
        if (debug) System.out.println("Удаление таблиц");
        Statement statement = this.conn.createStatement(); // получить Statement для выполнения SQL-запроса
        String request;
        // Удалить таблицу wordList из БД
        request = "DROP TABLE IF EXISTS wordList;";
        if (debug) System.out.println("\t" + request);
        statement.execute(request);
        // Удалить таблицу URLList из БД
        request = "DROP TABLE IF EXISTS URLList;";
        if (debug) System.out.println("\t" + request);
        statement.execute(request);
        // Удалить таблицу wordLocation из БД
        request = "DROP TABLE IF EXISTS wordLocation;";
        if (debug) System.out.println("\t" + request);
        statement.execute(request);
        // Удалить таблицу linkBetweenURL из БД
        request = "DROP TABLE IF EXISTS linkBetweenURL;";
        if (debug) System.out.println("\t" + request);
        statement.execute(request);
        // Удалить таблицу linkWord из БД
        request = "DROP TABLE IF EXISTS linkWord;";
        if (debug) System.out.println("\t" + request);
        statement.execute(request);
    }

    /* Инициализация таблиц в БД */
    protected void createIndexTables() throws SQLException
    {
        if (debug) System.out.println("Создание таблиц");
        Statement statement = this.conn.createStatement(); // получить Statement для выполнения SQL-запроса
        String request;
        // 1. Таблица wordList -------------------------------------------------
                // Создание таблицы wordList в БД
                request = "CREATE TABLE IF NOT EXISTS wordList(rowId INTEGER PRIMARY KEY AUTOINCREMENT, word TEXT NOT NULL, isFiltred BOOLEAN DEFAULT FALSE);"; //Сформировать SQL-запрос
        if (debug) System.out.println("\t" + request);
        statement.execute(request); // Выполнить SQL-запрос
        // 2. Таблица URLList --------------------------------------------------
                // Создание таблицы URLList в БД
        request = "CREATE TABLE IF NOT EXISTS URLList(rowId INTEGER PRIMARY KEY AUTOINCREMENT, URL TEXT NOT NULL, isIndexed BOOLEAN DEFAULT FALSE);"; //Сформировать SQL-запрос
        if (debug) System.out.println("\t" + request);
        statement.execute(request); // Выполнить SQL-запрос
        // 3. Таблица wordLocation ---------------------------------------------
                // Создание таблицы wordLocation в БД
                request = "CREATE TABLE IF NOT EXISTS wordLocation(rowId INTEGER PRIMARY KEY AUTOINCREMENT, fk_wordId INTEGER NOT NULL, fk_URLId INTEGER NOT NULL, location INTEGER NOT NULL);"; // Сформировать SQL-запрос
        if (debug) System.out.println("\t" + request);
        statement.execute(request); // Выполнить SQL-запрос
        // 4. Таблица linkBetweenURL -------------------------------------------
                // Создание таблицы linkBetweenURL в БД
                request = "CREATE TABLE IF NOT EXISTS linkBetweenURL(rowId INTEGER PRIMARY KEY AUTOINCREMENT, fk_FromURL_Id INTEGER NOT NULL, fk_ToURL_Id INTEGERNOT NULL);"; // Сформировать SQL-запрос
        if (debug) System.out.println("\t" + request);
        statement.execute(request); // Выполнить SQL-запрос
        // 5. Таблица linkWord -------------------------------------------------
                // Создание таблицы linkWord в БД
                request = "CREATE TABLE IF NOT EXISTS linkWord(rowId INTEGER PRIMARY KEY AUTOINCREMENT, fk_wordId INTEGER NOT NULL, fk_linkId INTEGER NOT NULL);"; //Сформировать SQL-запрос
        if (debug) System.out.println("\t" + request);
        statement.execute(request); // Выполнить SQL-запрос
    }


    /* Вспомогательный метод для получения идентификатора, добавления и
   обновления записи */
    private int SelectInsertUpdate(String table, String field, String value, int num, boolean createNew, boolean updateVal) throws Exception
    {
        Statement statement = this.conn.createStatement(); // получить Statementдля выполнения SQL-запроса
        String request;

        if (field.equals("LAST_INSERT_ROWID()"))
            { // получение Id последнегодобавленного элемента
                request = "SELECT LAST_INSERT_ROWID();";
                if (debug) System.out.println("\t\t\t" + request);
                ResultSet resultRow = statement.executeQuery(request);
                if (resultRow.next()) return
                        resultRow.getInt("LAST_INSERT_ROWID()");
            }

        if (!createNew && !updateVal)
            { // запрос Id элемента, отвечающегоусловиям
                        request = "SELECT rowId FROM " + table + " WHERE ";
                String fields[] = field.split(", ");
                String values[] = value.split(", ");
                for (int i = 0; i < num; i++)
                {
                    request += fields[i] + " = " + values[i];
                    if (i + 1 != num) request += " AND ";
                }
                request += ";";
                if (debug) System.out.println("\t\t\t" + request);
                 ResultSet resultRow = statement.executeQuery(request);
                if (resultRow.next()) return resultRow.getInt("rowId");
            }
        else
            if (!updateVal)
            { // занесение нового элемента в таблицу
            request = "INSERT INTO " + table + " (" + field + ") VALUES (" + value + ");";
            if (debug) System.out.println("\t\t\t" + request);
            statement.execute(request);
            return SelectInsertUpdate("", "LAST_INSERT_ROWID()", "", 0, false,
                    false);
            }
            else
                { // обновления значений полей определенного элемента
                    request = "UPDATE " + table + " SET ";
                    String fields[] = field.split(", ");
                    String values[] = value.split(", ");
                    int i;
                    for (i = 0; i < num; i++)
                    {
                        request += fields[i] + " = " + values[i];
                        if (i + 1 != num) request += ", ";
                    }
                    request += " WHERE " + fields[i] + " = " + values[i] + ";";
                    if (debug) System.out.println("\t\t\t" + request);
                    statement.execute(request);
                }
        return -1;
    }

    /* Проиндексирован ли URL */
    private boolean isIndexed(String url) throws Exception
    {
        Statement statement = this.conn.createStatement(); // получить Statementдля выполнения SQL-запроса
        String request = "SELECT isIndexed FROM URLList WHERE URL = '" + url + "';";
        if (debug) System.out.println("\t\t\t" + request);
        ResultSet resultRow = statement.executeQuery(request);
        if (resultRow.next() && resultRow.getBoolean("isIndexed")) return true;
        else return false;
    }

    /* Занесение слов в таблицы wordList и wordLocation */
    private void addWord(int urlId, String word) throws Exception
    {
        int wordId = SelectInsertUpdate("wordList", "word", "'" + word + "'", 1,false, false);
        if (wordId == -1)
        {
            wordId = SelectInsertUpdate("wordList", "word, isFiltred", "'" + word + "', " + Filter(word), 1, true, false);
        }
        SelectInsertUpdate("wordLocation", "fk_wordId, fk_URLId, location",wordId + ", " + urlId + ", " + location, 2, true, false);
        location++;
    }


    boolean Filter(String word)
    {
        if(isEngWord_F(word) && NoEnglishWord) return true;
        if(isDigit(word) && NoNumbers) return true;
        if(filter_Prepositions.contains(word) && NoPrepositions) return true;
        if(filter_Unions.contains(word) && NoUnions) return true;
        if(filter_Interjections.contains(word) && NoInterjections) return true;
        if(Names_Array.contains(word) && NoNames) return true;
        if(Pronouns_Array.contains(word) && NoPronouns) return true;
        return false;
    }


    static boolean isDigit(String word)
    {
        try {
            Integer.parseInt(word);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    boolean isEngWord(String word)
    {
        for (String Letter: Eng_letter)
        {
            if(word.contains(Letter)) return true;
        }
        return false;
    }

    boolean isEngWord_F(String word)
    {

        if (word =="")  return false;
        return Eng_letter.contains(word.charAt(0));
    }

    /* Занесение ссылки с одной страницы на другую и текста в таблицы
   linkBetweenURL и linkWord */
    private void addLinkRef(int urlFromId, int urlToId, String[] linkText)
            throws Exception
    {
        if (linkText == null) // если текст ссылки пустой
            SelectInsertUpdate("linkBetweenURL", "fk_FromURL_Id, fk_ToURL_Id",urlFromId + ", " + urlToId, 2, true, false);
        else
            {
            int linkBetweenId = SelectInsertUpdate("linkBetweenURL", "fk_FromURL_Id, fk_ToURL_Id", urlFromId + ", " + urlToId, 2, true, false);
            for (String word : linkText) {
                if (word.length() == 0) continue;
                int wordId = SelectInsertUpdate("wordList", "word", "'" + word + "'", 1, false, false);
                SelectInsertUpdate("linkWord", "fk_wordId, fk_linkId", wordId + ", " + linkBetweenId, 2, true, false);
            }
        }
    }

    /* Вспомогательный метод для формирования ссылки на следующую страницу */
    private String generateLink(String url, String nextUrl)
    {
        nextUrl = nextUrl.replace('\\', '/');
        if (nextUrl.startsWith("http") && nextUrl.length() > 6) { // абсолютнаяссылка начинается с http или httpsurl = "";
        } else if (nextUrl.startsWith(".")) { // относительная ссылка, дляперемещения по каталогам
                    url = url.substring(0, url.lastIndexOf("/"));
            while (nextUrl.contains("/") && nextUrl.substring(0,
                    nextUrl.indexOf("/")).equals("..")) { // перемещение на каталог вверх
                url = url.substring(0, url.lastIndexOf("/") + 1);
                nextUrl = nextUrl.substring(nextUrl.indexOf("/") + 1);
            }
            if (nextUrl.startsWith(".")) { // текущий каталог
                nextUrl = nextUrl.substring(nextUrl.indexOf("/"));
            }
        } else if (nextUrl.startsWith("//")) { // ссылка относительно протоколатекущей страницы
            url = url.substring(0, url.indexOf("//"));
        } else if (nextUrl.startsWith("/")) { // ссылка относительно доменатекущей страницы
            if (url.indexOf("/", url.indexOf("//") + 2) != -1)
                url = url.substring(0, url.indexOf("//") + 2) +
                        url.substring(url.indexOf("//") + 2, url.indexOf("/", url.indexOf("//") + 2));
        } else { // невалидная ссылка
            url = "";
            nextUrl = "";
        }
        nextUrl = url + nextUrl;
        while (nextUrl.endsWith("/")) nextUrl = nextUrl.substring(0,
                nextUrl.length() - 1); // удаление "/" на конце ссылки
        if (nextUrl.contains("://www.")) nextUrl = nextUrl.replace("://www.",
                "://"); // удаление "www" из ссылки
        return nextUrl;
    }
    /* Разбиение текста на слова */
    private String[] separateWords(String text)
    {
        String[] words = text.split(" ");
        return words;
    }

    /* Очистка HTML-кода от тегов */
    private String[] getTextOnly(Element html_doc)
    {
        String[] replace_symbols = {",", ":", "\"", "\\.", "\\{", "}", "—", "-",
                "\n", "\\(", "\\)", "/", "«", "»", "!", "\\?"}; // исключаемые символыпунктуации из текста страницы
        String html = html_doc.outerHtml().replaceAll("<", " <"); // подстановкапробелов перед открытием и закрытием тегов каждого элемента, чтобы избежатьслияния слов

        html_doc = Jsoup.parse(html);
        String html_text = html_doc.text();

        for (String replace_symbol : replace_symbols) html_text = html_text.replaceAll(replace_symbol, " ");

        while (html_text.contains("  ")) html_text = html_text.replaceAll("  ", " "); // замена множественных пробелов





        if (html_text.length() == 0) return null; // если страница не содержиттекст
        return separateWords(html_text.replace('\'', '`').toLowerCase());
    }

    /* Очистка страницы от комментариев и тегов noindex */
    private static void removeComments(Node node)
    {
        for (int i = 0; i < node.childNodeSize();) {
            Node child = node.childNode(i);
            if (child.nodeName().equals("#comment")) {
                if (child.outerHtml().equals("<!--noindex-->")) // удаление тега noindex Яндекса
                while (!child.outerHtml().equals("<!--/noindex-->")) {
                    child.remove();
                    child = node.childNode(i);
                }
                child.remove();
            }
            else {
                removeComments(child);
                i++;
            }
        }
    }
    /* Индексирование страницы */
    private List<String> addToIndex(String url) throws Exception
    {
        if (debug) System.out.println("\t\tИндексирование страницы");
        url = url.replace('\\', '/');
        List<String> nextUrlSet = null;
        // Проверка, проиндексирована ли страница
        if (!isIndexed(url)) {
            // Изменить состояние текущей страницы на индексации на"проиндексировано"
            int urlId = SelectInsertUpdate("URLList", "URL", "'" + url + "'", 1,
                    false, false);
            if (urlId != -1) SelectInsertUpdate("URLList", "isIndexed, URL","TRUE, '" + url + "'", 1, false, true);
            else urlId = SelectInsertUpdate("URLList", "URL, isIndexed", "'" + url + "', TRUE", 1, true, false);

            // Запросить HTML-код
            Document html_doc;
            if (debug)
                System.out.println("\t\tПопытка открыть " + url);

            // блок контроля исключений при запросе содержимого страницы
            try
            {
                html_doc = Jsoup.connect(url).get(); // получить HTML-кодстраницы
                if (debug) System.out.print("\t\tWEB файл ");
            }
            catch (java.net.MalformedURLException e)
            { // если не удалось,страница может быть локальным файлом
                if (debug) System.out.print("\t\tЛокальный файл ");
                String fileName = url.substring(7);
                File input = new File(fileName);
                html_doc = Jsoup.parse(input, "UTF-8");
            }
            catch (Exception e)
            {
                // обработка исключений при ошибке запроса содержимого страницы
                System.out.println("\t\tОшибка. " + url);
                System.out.print(e);
                return null;
            }

            if (html_doc != null)
            {
                if (debug) System.out.println("открыт " + url);

                // создать множество (ArrayList) очередных адресов (уникальных -не повторяющихся)
                nextUrlSet = new ArrayList<String>();
                // Найти и удалить на странице блоки со скриптами, стилямиоформления, meta-тегами и ссылками на внешние ресурсы ('script', 'style','meta', 'link')
                html_doc.select("script, style, meta, link").remove();
                removeComments(html_doc);

                String words[] = getTextOnly(html_doc);

                location = 0;
                for (String word : words)
                    addWord(urlId, word);

                // Разобрать HTML-код на составляющие
                // Получить все теги <a>
                Elements links = html_doc.getElementsByTag("a");
                if(IndexesName)
                    links.addAll(html_doc.getElementsByTag("name"));
                if(IndexesValue)
                    links.addAll(html_doc.getElementsByTag("value"));

                for (Element tagA : links)
                { // обработать каждый тег <a>
                    String nextUrl = tagA.attr("href"); // получить содержимоеаттрибута "href"
                    // Проверка соответствия ссылок требованиям
                    nextUrl = generateLink(url, nextUrl);
                    if (nextUrl.length() == 0)
                    {
                        if (debug) System.out.println("\t\t\tссылка не валидная - пропустить " + nextUrl);
                    }
                    else
                        if (SelectInsertUpdate("URLList", "URL", "'" + nextUrl + "'", 1, false, false) == -1)
                    {
                        if (debug) System.out.println("\t\t\tссылка валидная - добавить " + nextUrl);
                                nextUrlSet.add(nextUrl); // добавить в множество очередных ссылок nextUrlSet
                        int nextUrlId = SelectInsertUpdate("URLList", "URL", "'"
                                + nextUrl + "'", 1, true, false);
                        // Добавление связи ссылок и их текста ссылки в таблицы linkBetweenURL и linkWord
                        String[] link_text = getTextOnly(tagA);
                        addLinkRef(urlId, nextUrlId, link_text); // добавить информацию о ссылке в БД - addLinkRef(urlId, nextUrlId, link_text)
                    }
                } // конец цикла обработки тега <a>



            }
            if (debug) System.out.println("\t\tСтраница проиндексирована");
        }
        return nextUrlSet;
    }


    /* Метод сбора данных.
     * Начиная с заданного списка страниц, выполняет поиск в ширину
     * до заданной глубины, индексируя все встречающиеся по пути страницы */
    protected void crawl(Map<String, List<String>> parentUrls, int maxDepth)
            throws Exception {
        if (debug) System.out.println("Начало обхода всех страниц");
        // для каждого уровня глубины currDepth до максимального maxDepth
        for (int currDepth = 0; currDepth < maxDepth; currDepth++) {
            if (debug) System.out.println("\t== Глубина " + (currDepth + 1) + "==");
                    Map<String, List<String>> nextParentUrls = new HashMap<String, List<String>>();
            for (int i = 0; i < parentUrls.size(); i++)
            {
                String parentUrl = (String) parentUrls.keySet().toArray()[i];
                int N = parentUrls.get(parentUrl) == null ? 0 :parentUrls.get(parentUrl).size(); // количество элементов, которые предстоит обойти в списке urlList

                // обход всех url на теущей глубине
                for (int j = 0; j < N; j++)
                {
                    List<String> urlList = parentUrls.get(parentUrl);
                    String url = urlList.get(j); // получить url-адрес из списка
                    Date date = new Date();
                    SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
                    System.out.println("\t" + (j + 1) + "/" + urlList.size() + " - " + formatter.format(date) + " - Индексируем страницу " + url);
                    nextParentUrls.put(url, addToIndex(url)); // конец обработки одной ссылки url
                }
            }
            // заменить содержимое parentUrls на nextParentUrls
            parentUrls = nextParentUrls;
            // конец обхода ссылкок parentUrls на текущей глубине
        }
        if (debug)
            System.out.println("\tВсе страницы проиндексированы");
    }
}