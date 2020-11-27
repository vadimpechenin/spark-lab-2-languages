import org.apache.log4j.{Level, Logger}
import org.apache.spark._

import java.io.FileWriter
import au.com.bytecode.opencsv.CSVWriter

object Main{
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    //Пути к настройкам и данным
    val Seq(masterURL, postsDataPath, listLanguagesDataPath) = args.toSeq
    val dataBig = 0
    if (dataBig == 1) {
      //val postsDataPath = "file:///d:\\posts.xml"
      //val postsDataPath = "file:///z:/D:/posts.xml"
      // val postsDataPath = "file:///z:/D:/PYTHON/Magistracy/1 семестр/Лабы по BD/Лаба 1 БД 1 семестр/data/posts_sample.xml"
    }
    //val listLanguagesDataPath = "file:///d:\\PYTHON\\Magistracy\\1 семестр\\Лабы по BD\\Лаба 1 БД 1 семестр\\data\\List_of_programming_languages.txt"

    val cfg = new SparkConf().setAppName("Test").setMaster(masterURL)

    val sc = new SparkContext(cfg) // An existing SparkContext.

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // Диапазон с 2010 года по сегодняшний день
    val years = 2010 to 2020 map (yearInteger => yearInteger.toString)
    // Колличество с верха списка по популярности
    val numberOfTopLanguages = 10

    // Получение тестовой выборки
    val postsData = sc.textFile(postsDataPath)//.mapPartitions(_.take(1000))
    // Количество записей в данных
    val numberOfPosts = postsData.count
    // Добавление индекса и фильтрация строк без индекса в отдельную переменную
    val postsDataFilter = postsData.zipWithIndex.filter { case (s, idx) => idx > 2 && idx < numberOfPosts - 1 }.map(_._1)

    // переменная, содержащая перечень языков программирования с википедии
    val languagesData = sc.textFile(listLanguagesDataPath)

    // Опускаем записи в нижний ригистр
    val languagesDataToLowerCase = languagesData.map { idx => idx.toLowerCase }.collect()
    // Парсинг строк файла
    val postsDataXml = postsDataFilter.map(rawrow => scala.xml.XML.loadString(rawrow))

    // Функция для извлечения тэга и даты создания
    def extractCouplesTagsDates(dat: scala.xml.Elem) = {
      val creationDate = dat.attribute("CreationDate")
      val tags = dat.attribute("Tags")
      (tags, creationDate)
    }
    // фильтр (если определена пара тег-дата)
    val couplesFilter = postsDataXml.map(extractCouplesTagsDates).filter {
      x => x._1.isDefined && x._2.isDefined
    }.map {
      x => (x._1.mkString, x._2.mkString)
    }
    //couplesFilter.take(2).foreach(println)
    // Анализ годов и распеределения тэгов по годам
    def distributionByYears(couple: (String, String)) = {
      val (tags, creationDate) = couple
      // год - первые символы строк.
      val year = creationDate.substring(0, 4)
      // массив тегов, результат разделения tags
      val tagsArray = tags.substring(4, tags.length - 4).split("&gt;&lt;")
      (year, tagsArray)
    }

    val distributionYearTags = couplesFilter.map(distributionByYears)
    //distributionYearTags.take(2).foreach(println)
    // Делаем один тэг из массива тэгов в парах
    val yearTagsLangString = distributionYearTags.flatMap { case (year, tags) => tags.map(tag => (year, tag)) }
    //yearTagsLangString.take(2).foreach(println)
    // Фильтр чтобы оставить только языки, находящиеся в файле languagesData.
    val yearLanguageName = yearTagsLangString.filter { case (year, tag) => languagesDataToLowerCase.contains(tag) }
    yearLanguageName.cache() // кэширование данных
    //yearLanguageName.take(2).foreach(println)

    // Индексация записей в RDD по годам
    val yearTagFrequency = years.map { year =>
      yearLanguageName.filter {
        case (tagYear, tag) => year == tagYear
      }.map {
        // добавлена единица для подсчета
        case (tagYear, tag) => (tag, 1)
      }
        .reduceByKey((acc, n) => acc + n)
        // В итоге получается сжатая информация о количестве упоминаний (frequency) языка (tag) по году (year)
        .map { case (tag, frequency) => (year, tag, frequency)
        }
    }

    // Конвертация RDD в формат кортежа
    val topOfYearTagFrequency = yearTagFrequency.map { yearTagFrequency =>
      yearTagFrequency
        .sortBy { case (year, tag, frequency) => -frequency }
        .take(numberOfTopLanguages)
    }

    // Извлечение вложенных списков, создание единого списка
    val ListResult = topOfYearTagFrequency.reduce((a, b) => a.union(b))

    // Преобразование в формат dataframe
    val resultDataFrame = sc.parallelize(ListResult).toDF("Год", "Язык", "Количество_упоминаний")

    println("Таблица по 10 топовым языкам")
    resultDataFrame.show(years.size * numberOfTopLanguages)
    // Сохранение при большом файле
    if (dataBig == 1) {
      //val outputFile = "D:\\PYTHON\\Magistracy\\1 семестр\\Лабы по BD\\Лаба 1 БД 1 семестр\\language_selection_result.csv"
      //val csvWriter = new CSVWriter(outputFile)
      //csvWriter.writeAll(ListResult)
      resultDataFrame.write.format("parquet").save("language_selection_result.parquet")
    }
    sc.stop()

  }
}