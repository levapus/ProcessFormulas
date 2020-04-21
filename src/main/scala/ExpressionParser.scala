import scala.io.StdIn
import scala.util.parsing.combinator.JavaTokenParsers

object ExpressionParser extends JavaTokenParsers {

  def readExpression(input: String) : Option[Float] = {
    parseAll(expr, input) match {
      case Success(result, _) =>
        Some(result)
      case other =>
        println(other)
        None
    }
  }

  private def expr : Parser[Float] = {
    (term<~"+")~expr ^^ { case l~r => l + r } |
      (term<~"-")~expr ^^ { case l~r => l - r } |
      term
  }

  private def term : Parser[Float] = {
    (factor<~"*")~term ^^ { case l~r => l * r } |
      (factor<~"/")~term ^^ { case l~r => l / r } |
      factor
  }

  private def factor : Parser[Float] = {
    "("~>expr<~")" |
      floatingPointNumber ^^ {_.toFloat} |
      failure("Expected a value")
  }

  def main(args: Array[String]) {
    println("""Please input the expressions. Type "q" to quit.""")
    var input: String = ""

    do {
      input = StdIn.readLine("> ")
      if (input != "q") {
        ExpressionParser.readExpression(input).foreach(f => println(f))
      }
    } while (input != "q")
  }
}