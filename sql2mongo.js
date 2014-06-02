var async = require('async')
  , byline = require('byline')

process.stdin.setEncoding('utf8')
var stream = byline(process.stdin)

var tableInfo = {}
var currentTable = false

stream.on('data',function(line){
  if(/^CREATE TABLE /.test(line)){
    currentTable = line.replace(/^CREATE TABLE `/,'').replace(/` \($/,'')
    tableInfo[currentTable] = {fields:[],loaded:false}
  }
  if(currentTable){
    if(/^  `.*` .*$/.test(line)){
      var l = line.replace(/^  `/,'').replace(/` .*$/,'')
      tableInfo[currentTable].fields.push(l)
    }
  }
  if(currentTable && /^\) ENGINE=/.test(line)){
    tableInfo[currentTable].loaded = true
    console.log('db.createCollection(\'' + currentTable + '\')')
    currentTable = false
  }
  if(/^INSERT INTO /.test(line)){
    var table = line.replace(/^INSERT INTO `/,'').replace(/` VALUES.*$/,'')
    var values = line.replace(/^INSERT INTO .* VALUES \(/,'').replace(/\);$/,'')
    var m = values.split(',')
    var cmd = 'db.' + table + '.insert({'
    async.timesSeries(tableInfo[table].fields.length,function(x,next){
      if(0 !== x)
        cmd = cmd + ','
      cmd = cmd + tableInfo[table].fields[x] + ':' + m[x]
      next()
    },function(){
      cmd = cmd + '})'
      console.log(cmd)
    })
  }
})

stream.on('end',function(line){
  //console.log(tableInfo)
})
