import org.apache.hadoop.hive.ql.exec.{UDF, UDAF, UDAFEvaluator, UDTF}
import org.apache.hadoop.io.Text

// ======================= 1. Hive UDF (Row-wise Word Count) =======================
class WordCountUDF extends UDF {
  def evaluate(text: Text): Int = {
    if (text == null) return 0
    text.toString.split("\\s+").length
  }
}

// ======================= 2. Hive UDAF (Aggregate Word Count Sum) =======================
class SumWordCountUDAF extends UDAF {
  class Evaluator extends UDAFEvaluator {
    private var sum: Int = _

    // Initialize sum
    def init(): Unit = { sum = 0 }

    // Process each row
    def iterate(value: Int): Boolean = {
      sum += value
      true
    }

    // Save partial aggregation
    def terminatePartial(): Int = sum

    // Merge partial aggregations
    def merge(otherSum: Int): Boolean = {
      sum += otherSum
      true
    }

    // Final aggregation result
    def terminate(): Int = sum
  }
}

// ======================= 3. Hive UDTF (Explode Words into Rows) =======================
class SplitWordsUDTF extends UDTF {
  def process(text: Text): Unit = {
    if (text != null) {
      val words = text.toString.split("\\s+")
      for (word <- words) {
        forward(Array[AnyRef](new Text(word))) // Emit each word as a separate row
      }
    }
  }
}
