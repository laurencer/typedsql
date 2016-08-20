package com.rouesnel.typedsql.hive

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Context
import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse.{ASTNode, ParseDriver, ParseUtils}
import org.slf4j.LoggerFactory

import scala.collection.convert.decorateAsScala._

/** Method for extracting and replacing function calls in a HiveQL statement */
object QueryRewriter {
  val log = LoggerFactory.getLogger(getClass)

  def replaceFunctionInvocations(cmd: String,
                                 conf: HiveConf,
                                 replacements: Map[String, String]): String = {
    val pd  = new ParseDriver()
    val ctx = new Context(conf)

    val rawTree = pd.parse(cmd, ctx)
    val root    = ParseUtils.findRootNonNullToken(rawTree)

    val lowercaseReplacements = replacements.map({ case (key, value) => key.toLowerCase -> value })

    def replaceFunctionName(original: String): String = {
      lowercaseReplacements
        .get(original.toLowerCase)
        .map(replacement => {
          log.info(s"Replacing UDF call from ${original} to ${replacement}")
          replacement
        })
        .getOrElse(original)
    }

    findFunctions(root)({
      case functionNode: ASTNode => {
        // First child is the name of the function (rest are parameters).
        val functionNameNode = functionNode.getChildren.get(0).asInstanceOf[ASTNode]
        val originalFunction = functionNameNode.token.getText
        functionNameNode.token.setText(replaceFunctionName(originalFunction))
      }
    })

    val rewritten = ctx.getTokenRewriteStream.toString
    log.debug(s"Rewrote query to: ${rewritten}")
    rewritten
  }

  /** Finds functions within an AST and calls visit on them */
  def findFunctions(node: Node)(visit: Node => Unit): Unit = {
    if (node.asInstanceOf[ASTNode].toString == "TOK_FUNCTION") {
      visit(node)
    }
    Option(node.getChildren)
      .map(_.asScala.toList)
      .getOrElse(Nil)
      .flatMap(Option(_))
      .foreach(child => {
        findFunctions(child)(visit)
      })
  }
}
