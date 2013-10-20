
/**
 * Created with IntelliJ IDEA.
 * User: stevemayzak
 * Date: 10/19/13
 * Time: 6:18 PM
 *
 * The purpose of this script is to take an ordinary CSV file and convert it to JSON, then bulk load it into Elasticsearch.
 * I initially created this as a way to explore Elasticsearch and convert an exported google doc into ES.  It works great so far for
 * me but its a small file. I haven't tested it with anything major yet so for now, its quite specific to that use case.
 * I have made some top level properties for easier reuse.
 *
 * Note the imports, I'm using the HTTPBuilder to do the Index call.  I will change this to the groovy Elasticsearch
 * client in the future.
 */
import groovy.json.JsonBuilder
import groovyx.net.http.HTTPBuilder

import static groovyx.net.http.ContentType.JSON

//Simple debug and info to see whats happening as its happening. I recommend turning debug off for large batches
boolean debugOn = false
boolean infoOn  = true
def debug = { value ->  if(debugOn) println value }
def info = { value ->  if(infoOn) println value }

//Path to your CSV File
def csvFile = new File("../resource/PLACEHOLDER.CSV")
//the server URL for an Elasticsearch node in your cluster, can be localhost
def serverUrl = "http://changeme:9200"

//holder for the first row of text in the csv which will become the JSON fields
def columnHeaders = []
def indexName = "yourIndexNameForBulkInsert"
def typeName = "yourTypeNameForBulkInsert"
def jsonDocs = []
def maps = []

csvFile.eachLine {line, lineNum ->
    debug("working on csv line ${lineNum}: ${line}")
    //regex I found online to parse a CSV.  Should handle inline commas as long as the string is surrounded in quotes
    def row = line.split(/,(?=([^"]*"[^"]*")*(?![^"]*"))/)*.replaceAll(/"/, "")

    //set the columnheaders
    if(columnHeaders.size() <= 0){
        info("setting ${row.size()} values as column headers")
        columnHeaders = row
    }
    def map = [:]
    row.eachWithIndex{token, index ->
        //Build map to later be converted to JSON TODO add smarts for multi field values like tags etc.
        debug("setting token: ${token} at ${index}")
        map[columnHeaders[index]] = token
    }
    //don't store the column header
    if(lineNum > 1){
        //add the required json params first for a _bulk request specifying index, type and id
        /*{ "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" } }*/
        maps << ["index":["_index":indexName,"_type":typeName]]
        maps << map
    }
}

//convert map to JSON
def jsonStringToSend = new StringBuilder()
maps.each{ it ->
    def jsonString = new JsonBuilder(it)
    debug("json to be put int string: ${jsonString}")
    jsonStringToSend.append(jsonString).append("\n")
}
debug("finalString: ${jsonStringToSend}")

def httpClient = new HTTPBuilder("${serverUrl}/${indexName}/${typeName}/_bulk" )
info("Sending json to Server at address: ${httpClient.uri}")

def response = httpClient.post(body: jsonStringToSend.toString(),
        requestContentType: JSON ) { resp, result->
    return result
}
info("response : ${response}")
//shutdown the client
httpClient.shutdown()
