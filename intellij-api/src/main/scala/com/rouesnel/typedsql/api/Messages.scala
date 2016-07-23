package com.rouesnel.typedsql.api

case class CompilationRequest(
  query: String,
  parameters: Map[FieldName, ScalaTypeName],
  sources: Map[FieldName, List[(FieldName, ScalaTypeName)]]
)

case class CompilationResponse(code: Option[String])

case class LogMessage(message: String)