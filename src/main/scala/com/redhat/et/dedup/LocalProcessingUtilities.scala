package com.redhat.et.dedup

import java.io.{FilterReader, Reader}

import scala.collection.mutable.ListBuffer

object LocalProcessingUtilities {
  def listDirectories(dirname: String) : List[String] = {
    import java.io.File
    import java.io.FileFilter

    val filter = new FileFilter() {
      def accept(pathname : File) : Boolean = pathname.isDirectory
    }

    (new File(dirname))
      .listFiles(filter)
      .map {dirname + File.separator + _.getName}
      .toList
  }
}
