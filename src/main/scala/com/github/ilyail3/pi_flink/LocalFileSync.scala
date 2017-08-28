package com.github.ilyail3.pi_flink

import java.io._
import java.text.{DateFormat, SimpleDateFormat}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import java.util.{Date, TimeZone}

import org.apache.commons.csv.{CSVFormat, CSVParser, CSVPrinter}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex

class LocalFileSync(folder: File, levels: Seq[FiniteDuration]) extends RichSinkFunction[AvgPressureTemp] {
  @transient private var currentLevel: Array[Long] = null
  @transient private var level0Memory: mutable.Buffer[(Long, AvgPressureTemp)] = null
  @transient private var df: DateFormat = null

  private def time(index: Int): Long =
    (System.currentTimeMillis() / levels(index).toMillis) * levels(index).toMillis

  private def dumpFile(time: Long): Unit = {
    if (level0Memory.isEmpty) return

    val dateString = df.format(new Date(time))

    LocalFileSync.writer(new File(folder, dateString + "-L0.tsv"))(csvPrinter => {
      level0Memory.foreach {
        case (t, r) =>
          csvPrinter.printRecord(
            df.format(new Date(t)),
            "%.2f".format(r.averageTemp),
            "%.3f".format(r.averagePressure / 100.0),
            r.samples.toString
          )
      }
    })

    level0Memory.clear()
  }

  private def compact(time: Long, level: Int): Unit = {
    val dateEnd = time + levels(level).toMillis

    val compactFiles = folder.listFiles().flatMap(file => {
      file.getName match {
        case LocalFileSync.FileReg(date, fileLevel, gz) if fileLevel.toInt == level - 1 =>
          val fileDate = df.parse(date).getTime

          if (fileDate >= time && fileDate < dateEnd)
            Some(LocalFileSync.CompactFile(fileDate, file, gz != null))
          else
            None
        case _ => None
      }
    }).sortBy(_.date)

    val dateString = df.format(new Date(time))
    LocalFileSync.writer(new File(folder, s"$dateString-L$level.tsv.gz"))(csvPrinter => {
      compactFiles.foreach(cf => {
        LocalFileSync.read(cf.file)(parser => {
          val it = parser.iterator()
          // Drop header, don't duplicate it
          it.next()

          while (it.hasNext) {
            csvPrinter.printRecord(it.next())
          }
        })
      })
    })

    compactFiles.foreach(cf => FileUtils.forceDelete(cf.file))

  }

  override def invoke(value: AvgPressureTemp) = {
    levels.zipWithIndex.foreach {
      case (_, index) =>
        val current = time(index)

        if (current != currentLevel(index)) {
          if (index == 0) dumpFile(currentLevel(index))
          else compact(currentLevel(index), index)

          currentLevel(index) = current
        }

    }

    level0Memory += System.currentTimeMillis() -> value
  }

  override def open(parameters: Configuration) = {
    df = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss'Z'")
    df.setTimeZone(TimeZone.getTimeZone("UTC"))

    level0Memory = mutable.Buffer[(Long, AvgPressureTemp)]()

    currentLevel = new Array[Long](levels.size)

    levels.zipWithIndex.foreach {
      case (_, index) =>
        currentLevel(index) = time(index)
    }
  }

  override def close() = {
    dumpFile(currentLevel.head)
  }
}

object LocalFileSync {

  case class CompactFile(date: Long, file: File, gz: Boolean)

  private val FileReg: Regex = "^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}-[0-9]{2}-[0-9]{2}Z)-L([0-9]+)\\.tsv(\\.gz)?$".r
  private val CSVFileFormat = CSVFormat.DEFAULT
    .withRecordSeparator('\n')
    .withDelimiter('\t')
    .withHeader("time", "temp", "pressure", "samples")

  private def writer(file: File)(f: CSVPrinter => Unit): Unit = {
    val tmpFile = new File(file.getParent, "." + file.getName)

    val fos = new FileOutputStream(tmpFile)

    // Select gzip or not based on file extension, that's the safest way
    val gos =
      if (file.getName.endsWith(".gz"))
        new GZIPOutputStream(fos)
      else
        fos

    val w = new OutputStreamWriter(gos)
    val csvPrinter = new CSVPrinter(w, LocalFileSync.CSVFileFormat)

    try {
      f(csvPrinter)
    } finally {
      IOUtils.closeQuietly(csvPrinter)
      IOUtils.closeQuietly(w)
      if (gos != fos)
        IOUtils.closeQuietly(gos)
      IOUtils.closeQuietly(fos)
    }

    if (file.exists()) FileUtils.forceDelete(file)
    tmpFile.renameTo(file)
  }

  private def read(file: File)(f: CSVParser => Unit) {
    val fis = new FileInputStream(file)

    val is =
      if (file.getName.endsWith(".gz"))
        new GZIPInputStream(fis)
      else
        fis

    val reader = new InputStreamReader(is)

    val csvReader = new CSVParser(reader, LocalFileSync.CSVFileFormat)

    try {
      f(csvReader)
    } finally {
      IOUtils.closeQuietly(csvReader)
      IOUtils.closeQuietly(reader)
      if (is != fis)
        IOUtils.closeQuietly(is)
      IOUtils.closeQuietly(fis)
    }
  }
}