//import com.twitter.scalding.{Job, Args, TextLine, Tsv, Csv, Osv}
//
//class ScaldingTraning(args : Args) extends Job(args) {
//  TextLine( args("input") )
//    .flatMap('line -> 'word) { line : String =>
//      line.toLowerCase.split("\\s+")
//    }
//    .groupBy('word) { _.size }
//    .write( Tsv( args("output") ) )
//
//  val inputSchema = List('productID, 'price, 'quantity)
//  val data = Csv( args("input") , "," , inputSchema ).read
//  data.write(Tsv(args("output.tsv")))
//}
