package com.rouesnel.typedsql.api

import com.rouesnel.typedsql.core._

case class CompilationRequest(
  query: String,
  parameters: Map[FieldName, ScalaTypeName],
  sources: Map[FieldName, StructType]
)

case class CompilationResponse(
  code: Option[String],
  schema: Option[StructType]
)

case class LogMessage(message: String)