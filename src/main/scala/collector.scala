import sys.process._
import scala.xml._
import collection.mutable.ListBuffer
import java.net._
import java.io._
import scala.io._
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import fr.janalyse.ssh._


trait Logger {
  val loggerFile: String
  def log(msg: String)  {
    val out = new FileWriter(loggerFile, true)
    try {
      out.write(new java.util.Date + " " +  msg + "\n")
    }
    finally out.close()
  }
}


object collector extends App with Logger  {

  val loggerFile = "collector-error.log"
  
  if( (args.length != 0 && args.length != 2) || (args.length > 0 && args(0) != "-c") )  {
    println("Usage: collector -c config.file")
    log("Usage: collector -c config.file")
    sys.exit(-1)
  }

  val configFile = if(args.length == 2)  args(1)  else  "collector.xml"

  if( !(new java.io.File(configFile).exists) )  {
    println("Error: no such file " + configFile)
    log("Error: no such file " + configFile)
    sys.exit(-1)
  }

  val config: scala.xml.Elem = try {
    XML.loadFile(configFile)
  }  catch  {
    case _ : Throwable => println("Error in processing configuration file " + configFile)
                          log("Error in processing configuration file " + configFile)
                          sys.exit(-1)
  }
    
  if( (config \ "configuration").length != 1 )  {
    println("Error: no configuration in the file " + configFile)
    log("Error: no configuration in the file " + configFile)
    sys.exit(-1)
  }
    
  val configuration: Map[String,Any] = ((scala.xml.Utility.trim((config \ "configuration")(0)).child) map
    (c => c.label match {
      case "carbon" => "carbon" -> c.attributes.asAttrMap
      case "replacelist" => "replacelist" -> (c \ "replace" map
        (r => (r.attributes("what").toString, r.attributes("value").toString))).toList
      case _ => c.label -> c.text
    })
  ).toMap

  if( !configuration.contains("carbon") )  {
    println("Wrong carbon description in the configuration file " + configFile)
    log("Wrong carbon description in the configuration file " + configFile)
    sys.exit(-1)
  }

  val interval = if( configuration.contains("interval") )
                   configuration("interval").asInstanceOf[String].toInt
                 else
                   10

  val blockCmd = if( (config \ "vnxblock" \ "cmd").length > 0 )
                   (config \ "vnxblock" \ "cmd")(0).text
                 else
                   "" 

  val blockMethods: Map[String, Map[String,Any]] = (config \ "vnxblock" \ "method" map (m =>
    (m \ "name").text -> ( scala.xml.Utility.trim(m).child map ( c =>
      c.label match {
        case "paramlist" => "params" -> (c \\ "param" map (p => p.text)).toList
        case "headerlist" => "header" -> (c \\ "header" map (h =>
          ((h \ "@pos").text, (h \ "@sep").text))).toList
        case _ => c.label -> c.text
      })).toMap
  )).toMap
 
  val doBlock = !blockMethods.isEmpty && (blockCmd != "")

  val fileCmd = if( (config \ "vnxfile" \ "cmd").length > 0 )
                  (config \ "vnxfile" \ "cmd")(0).text
                else
                  "" 

  val fileMethods: Map[String,(String,String)] = (config \ "vnxfile" \\ "method" map
    (m => m.attributes("name").toString -> (m.attributes("cmd").toString, m.attributes("type").toString))).toMap

  val doFile = !fileMethods.isEmpty && (fileCmd != "")

  val vnxList: List[Map[String,Any]] = (config \ "vnx" map (v =>
    (scala.xml.Utility.trim(v).child map (vc => vc.label match {
      case "block" => "block" -> 
        (vc.child map (c => c.label match {
          case "methods" => "method" -> (c.child map (c => c.attributes.asAttrMap)).toList
          case _ => c.label -> c.text
        })).toMap
      case "file" => "file" ->
        (vc.child map (c => c.label match {
          case "servers" => "server" -> (c \ "server" map (c => c.text)).toList
          case "methods" => "method" -> (c \ "method" map (c => c.text)).toList
          case _ => c.label -> c.text
        })).toMap
      case _ => vc.label -> vc.text
    })).toMap
  )).toList

  if( vnxList.length == 0 )  {
    println("At least one VNX should be described in the configuration file " + configFile)
    log("At least one VNX should be described in the configuration file " + configFile)
    sys.exit(-1)
  }

  val system = ActorSystem("vnx")
  val vnxActList = (vnxList map (v => (v, system.actorOf( Props(new VNX(v, configuration,
                                                                        blockCmd, blockMethods,
                                                                        fileCmd, fileMethods)),
                                                          name = v("name").asInstanceOf[String] )
                   ))).toList

  while(true) {
    for( v <- vnxActList )  { 
      if( v._1.contains("block") && doBlock )  v._2 ! "block"
      if( v._1.contains("file") && doFile )  v._2 ! "file"
    }
    Thread.sleep(interval*60000)
  }

  system.shutdown
  sys.exit(0)
}


class VNX( vnx: Map[String,Any],
           configuration: Map[String,Any],
           blockCmd: String,
           blockMethods: Map[String, Map[String,Any]],
           fileCmd: String,
           fileMethods: Map[String,(String,String)] ) extends Actor with Logger  {

  val arg = "(#)([A-Za-z]+)".r
  val numbers = "[0-9]+".r
  val loggerFile: String = configuration.getOrElse("errorlog","collector-error.log").asInstanceOf[String]
  val carbon = configuration("carbon").asInstanceOf[Map[String,String]]
  val vname = vnx("name").asInstanceOf[String]
  val repl = configuration.getOrElse("replacelist",List(("",""))).asInstanceOf[List[(String,String)]]
  var sock: java.net.Socket = null
  var out: java.io.OutputStream = null


  def receive = {
    case "block" => block
    case "file" => file
    case _ => log("Strange message")
  }
   

  def block {
    
    val timestamp: Long = System.currentTimeMillis / 1000
    sock = try { new Socket( carbon("address"), carbon("port").toInt ) }
           catch { case e: Exception => null }
    out = if( sock != null )  sock.getOutputStream()  else  null
    if( out == null )  {
      log(vname + " block: Can't connect to the carbon")
      return
    }

    val vb = vnx("block").asInstanceOf[Map[String,Any]]
    for( m <- vb("method").asInstanceOf[List[Map[String,String]]] )  {
      if( blockMethods.contains(m("name")) )  {
        val basecmd = blockCmd.split(" ") map (c => c match {
                        case arg(a1,a2) => vb.getOrElse(a2,m.getOrElse(a2," ")).asInstanceOf[String]
                        case _ => c
                      })
        val cmd = basecmd ++ blockMethods(m("name"))("cmd").asInstanceOf[String].split(" ")
        val stdout = ListBuffer[String]()
        val status: Int = try { (cmd.toSeq run ProcessLogger(stdout append _)).exitValue }
                          catch {
                            case e: Exception => log(vname + ": Error in run: " +
                                                     cmd.mkString(" ") + "\n" + e.getMessage)
                                                 -1
                          }
        if( status == 0 )  {
          val result = stdout mkString "\n"
          blockMethods(m("name"))("type").asInstanceOf[String] match {
            case "simple" => simple(
                               "vnx." + vname + ".block." + m("title") + ".",
                               blockMethods(m("name"))("params").asInstanceOf[List[String]],
                               result,
                               timestamp
                             )
            case "flat" => flat(
                             blockMethods(m("name"))("pattern").asInstanceOf[String],
                             blockMethods(m("name"))("header").asInstanceOf[List[(String,String)]],
                             "vnx." + vname + ".block." + m("title") + ".",
                             blockMethods(m("name"))("params").asInstanceOf[List[String]],
                             result,
                             timestamp
                           )
          }
        }   else  {
          log(vname + ": Error in run: " + cmd.mkString(" ") + "\n" + stdout)
        }
      }   else  {
        log(vname + ": No method " + m("name") + " described in the configuration file" )
      }
    }
    if( sock != null )  sock.close
  }


  def file {
    
    val timestamp: Long = System.currentTimeMillis / 1000
    sock = try { new Socket( carbon("address"), carbon("port").toInt ) }
           catch { case e: Exception => null }
    out = if( sock != null )  sock.getOutputStream()  else  null
    if( out == null )  {
      log(vname + " file: Can't connect to the carbon")
      return
    }

    val vf = vnx("file").asInstanceOf[Map[String,Any]]
    for( server <- vf("server").asInstanceOf[List[String]] )  
      for( m <- vf("method").asInstanceOf[List[String]] )  {
        if( fileMethods.contains(m) )  {
          val basecmd = fileCmd.split(" ") map (c => c match {
                          case arg(a1,a2) => a2 match {
                                               case "server" => server
                                               case "cmd" => fileMethods(m)._1
                                               case _ => vf.getOrElse(a2," ").asInstanceOf[String] }
                          case _ => c
                        })
          val cmd = basecmd.mkString(" ")
          val cs = vf("cs").asInstanceOf[String]
          val username = vf("username").asInstanceOf[String]
          val password = vf("password").asInstanceOf[String]
          val result = SSH.once(cs,username,password)(_.execute(cmd)).split("\n")
          if( result.length > 1 )  {
            val header = fileMethods(m)._2 match {
                           case "composite" => result(0).split(",") map (h => replaceByList(h,repl).replace("\"","")
                                                 .replaceFirst(" ","."))
                           case "composite2" => result(0).split(",") map (h => replaceByList(h,repl).replace("\"","")
                                                 .replaceFirst(" ",".").replaceFirst(" ","."))
                           case "composite3" => result(0).split(",") map (h => replaceByList(h,repl).replace("\"","")
                                                 .replaceFirst(" ",".").replaceFirst(" ",".").replaceFirst(" ","."))
                           case _ => result(0).split(",") map (h => replaceByList(h,repl).replace("\"",""))
                         }
            val values = result(result.length-1).split(",") map (h => replaceByList(h,repl).replace("\"",""))
            for( i <- 1 until values.length if values(i) != "" )  {
              val msg = "vnx." + vname + ".file." + server + "." + m + "." +
                        header(i).replace(" ","") + " " + values(i) + " " + timestamp + "\r\n"
              try {
                out.write(msg.getBytes)
                out.flush
              }  catch  {
                case e: Exception => log(vname + ": Error in output to carbon")
              }
            }
          }   else  {
            log(vname + ": Incorrect answer for file method " + m + " for server " + server)
          }
        }   else  {
          log(vname + ": No method " + m + " described in the configuration file" )
        }
      }
    if( sock != null )  sock.close
  }


  def replaceByList( text: String, repl: List[(String, String)] ): String = {
    if( !repl.isEmpty )
      return replaceByList( text.replace(repl(0)._1,repl(0)._2), repl.tail )
    else
      return text
  }


  def simple( title: String,
              params: List[String],
              result: String,
              timestamp: Long ) {

    val values = result.split("\n")

    values.foreach( c => for( p <- params if(p.r.findFirstIn(c) != None) )  {
      val msg = title + replaceByList(p,repl).replace(" ","") + " " + numbers.findFirstIn(c).mkString +
                  " " + timestamp + "\r\n"
      try { 
        out.write(msg.getBytes)
        out.flush
      }  catch  {
        case e: Exception => log(title + ": Error in output to the carbon")
      }
    })

  }


  def flat( pattern: String,
            header: List[(String, String)],
            title: String,
            params: List[String],
            result: String,
            timestamp: Long ) {

    val pm = pattern.r
    val values = result.split("\n")

    for(i <- 0 until values.length if pm.findFirstIn(values(i)) != None &&
                                      i + header.length < values.length)  {
      val head = (values.slice(i,i+header.length).toList zip header map (h =>
                   h._2._1 match {
                             case "right" => (h._1.split(h._2._2))(1)
                             case "left" => (h._1.split(h._2._2))(0)
                             case _ => h._1
                 })).mkString
      values.drop(i + header.length).takeWhile(pm.findFirstIn(_) == None).foreach( c => 
        for( p <- params if(p.r.findFirstIn(c) != None) )  {
          val msg = title + replaceByList(head,repl).replace(" ","") + "." +
                      replaceByList(p,repl).replace(" ","") + " " +
                      numbers.findFirstIn(c).mkString + " " + timestamp + "\r\n"
          try {
            out.write(msg.getBytes)
            out.flush
          }  catch  {
            case e: Exception => log(title + ": Error in output to carbon")
          }
        }
      )
    }
  }

}
