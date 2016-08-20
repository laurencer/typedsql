package com.rouesnel.typedsql.util

import java.io.{File, FileOutputStream}

import scala.reflect.macros.Context

/**
 * Persists the macro generated code.
 */
object PersistGeneratedMacroCode {
  def apply[C <: Context](c: C)(code: String): Unit = {
    val projectRoot  = new java.io.File("").getAbsoluteFile
    val targetDir   = new java.io.File(projectRoot, "target")
    val typedSqlDir = new java.io.File(targetDir, "typedsql")
    Option(c.enclosingPosition.source.file.file) match {
      case None      => c.warning(c.enclosingPosition, "Could not persist macro-generated output because the source file location is not available.")
      case Some(src) => {
        val parentDirectory = src.getParentFile
        val relativePath = projectRoot.toURI().relativize(parentDirectory.toURI()).getPath
        val targetDir = new File(projectRoot.getAbsoluteFile, "target")
        val generatedDir = new File(typedSqlDir, "generated")
        val rootedInTypedSqlTmp = new File(generatedDir, relativePath)
        val persisted = new File(rootedInTypedSqlTmp, src.getName)

        rootedInTypedSqlTmp.mkdirs()
        persisted.delete()
        persisted.createNewFile()
        c.info(c.enclosingPosition, s"Macro generated code can be found at ${persisted.getAbsoluteFile}", false)

        val fos = new FileOutputStream(persisted)
        try {
          fos.write(code.getBytes)
        } finally {
          fos.close()
        }
      }
    }
  }
}
