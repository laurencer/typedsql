package com.rouesnel.typedsql

import scala.annotation.StaticAnnotation

/**
  * Annotation that when applied to a method in a @SqlQuery will be accessible in that
  * query as a UDF.
  */
class UDF extends StaticAnnotation {}
