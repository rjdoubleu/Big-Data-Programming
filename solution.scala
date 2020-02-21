val _author_ = "Ryan Walden"
val _assignment_ = 2

// Optional Download
// import scala.io.Source
// Copy download link address from http://s000.tinyupload.com/?file_id=00242094171759066399 and paste below
// val linkAddress = ""
// val html = Source.fromURL(linkAddress)
// import java.io.PrintWriter
// new PrintWriter("flava.txt") { write(html.mkString); close }

// split file into lines RDD
// eg. "Oh captain my captain!\nSay hello to my little friend!" --> "Oh captain my captain!" , "Say hello to my little friend!"
val lines = sc.textFile("flava.txt")

// split lines into words RDD and filter empty words
// eg. "Oh captain my captain!" --> "Oh", "captain", "my", "captain!"
val words = lines.flatMap(_.split(" ")).filter(_ != "")

// remove all non-alphabetic characters from each word
// eg. "captain!" -> "captain"
val cleanWords = words.map(_.replaceAll("[^a-zA-Z]",""))

// set variables neccessary for occurence calculations
val occurence = 1
val totalWords = cleanWords.count.toFloat

// use the lowercase version as a key and give each word a single occurence count
// eg. "Say" --> ("say", 1, "Say")
val lowWords = cleanWords.map(word => (word.toLowerCase, occurence, word))

// consolidate words with the same lowercase value and add their occurence and their unique word
// eg. ("captain", 1, "captain") , ("captain", 1, "captain") --> ("captain", (2,"captain", "captain")
val repsAndOccs = lowWords.map{ case (key, occ, word) => ((key), (occ, word)) }.reduceByKey((a,b) => (a._1 + b._1, a._2 + "," + b._2))

// Format all data for display
// eg. "captain 20% 2 total occurence(s), 1 repersentation(s) {captain}"
val fmtReps = repsAndOccs.map(rep => rep._1 + f" ${rep._2._1/totalWords*100}%.0f%% "  + rep._2._1 + " total occurence(s), " +
rep._2._2.split(",").distinct.length + " repersentation(s) " + "{" + rep._2._2.split(",").distinct.mkString(", ") + "}")

// Display final RDD
fmtReps.foreach(println)