const fs = require('fs');
const es = require('event-stream');
const rq = require('request');
const minimist = require('minimist');
const moment = require('moment');
const Stats = require('fast-stats').Stats;

const defaultCluster = 'test.groupbycloud.com';

var argv = minimist(process.argv.slice(2), { default: { c: defaultCluster } } );
console.dir(argv);

if(Object.keys(argv).includes('h')){

  console.log(`
    Usage:
 
    node playback.js [LD JSON PLAYBACK FILE]... [OPTION]... 

    Options:
    -n : number of concurrent queries
    -d : delay (in ms) between sets of concurrent queries
    -c : domain/cluster (default test.groupbycloud.com ) 
    

    Output:
    - playback_{n}_{d}_{timestamp}.ldjson with data from each request
    - playback_{n}_{d}_{timestamp}.tsv with summary stats of load and performance
    - playback_{n}_{d}_{timestamp}.log 

    Example: 
    node playback queries.ldjson -c loadtest.com -n 10 -d 100

    Reference Configurations:
    850 qps :  n=100,d=0
    600 qps :  n=6,  d=10  (100*6)
    480 qps :  n=15, d=30  (33*15)
    140 qps :  n=15, d=100 (10*15)

  `);

  process.exit();
}


const filename = argv._[0];
const nConcurrent = argv.n;
const nDelay = argv.d;
const cluster = argv.c;
const timestamp = moment().format("MMDD_HHmmss");
const outfs = fs.createWriteStream(`playback_${nConcurrent}_${nDelay}_${timestamp}.ldjson`, { flags: 'a' });
const outlogfs = fs.createWriteStream(`playback_${nConcurrent}_${nDelay}_${timestamp}.log`, { flags: 'a' });
const outmetricfs = fs.createWriteStream(`playback_${nConcurrent}_${nDelay}_${timestamp}.tsv`, { flags: 'a' });

let lineNr = 0;
const statResponseTime = new Stats();
const statWait = new Stats();
const statDNS = new Stats();
const statTCP = new Stats();
const statFirstByte = new Stats();
const statDownload = new Stats();
const statTotal = new Stats();

const statsInterval = 5000;
let timeElapsed = 0;

const getMetricsLine = (name, srt) => {
  let summary = `${timeElapsed}\t${name}\t${srt.length}\t${srt.length/(statsInterval/1000)}\t${srt.percentile(50).toFixed(1)}\t${srt.percentile(75).toFixed(1)}\t${srt.percentile(95).toFixed(1)}\t${srt.range()[0].toFixed(1)}\t${srt.range()[1].toFixed(1)}\n`;
  return summary;
}


setInterval(function () {
  timeElapsed += statsInterval/1000;
  // stats
  // - timeElapsed
  // - type
  // - queries
  // - qps
  // - median
  // - perc75
  // - perc95
  // - min
  // - max
  let srt = statResponseTime;
  let tsvSummary = getMetricsLine("tall",srt);
  outmetricfs.write( tsvSummary );
  statResponseTime.reset();  

  srt = statWait;
  tsvSummary = getMetricsLine("twait",srt);
  outmetricfs.write( tsvSummary );
  statWait.reset();  

  srt = statDNS;
  tsvSummary = getMetricsLine("tdns",srt);
  outmetricfs.write( tsvSummary );
  statDNS.reset();  

  srt = statTCP;
  tsvSummary = getMetricsLine("ttcp",srt);
  outmetricfs.write( tsvSummary );
  statTCP.reset();  

  srt = statFirstByte;
  tsvSummary = getMetricsLine("t1stb",srt);
  outmetricfs.write( tsvSummary );
  statFirstByte.reset();  

  srt = statDownload;
  tsvSummary = getMetricsLine("tstdl",srt);
  outmetricfs.write( tsvSummary );
  statDownload.reset();  

  srt = statTotal;
  tsvSummary = getMetricsLine("ttot",srt);
  outmetricfs.write( tsvSummary );
  statTotal.reset();  

}, 5000)


//from: https://stackoverflow.com/questions/16010915/parsing-huge-logfiles-in-node-js-read-in-line-by-line

const getTimings = (response) => {

  if(response.elapsedTime)  statResponseTime.push( response.elapsedTime );

  if(response.timingPhases){
    statWait.push( response.timingPhases.wait )
    statDNS.push( response.timingPhases.dns )
    statTCP.push( response.timingPhases.tcp )
    statFirstByte.push( response.timingPhases.firstByte )
    statDownload.push( response.timingPhases.download )
    statTotal.push( response.timingPhases.total )  
  }


} ;

var s = fs.createReadStream(filename)
  .pipe(es.split())
  .pipe(es.mapSync(function(line){

    lineNr += 1;
    if(lineNr % nConcurrent === 0){
    	// pause the readstream
    	s.pause();
    }

    //parse the json
    const obj = JSON.parse(line);

    const lineNumber = lineNr;
    const timestamp = moment().toISOString();

    const requestConfig = {
      method: obj.method,
      time: true,
      uri : `https://${cluster}${obj.path}`,
      headers : { 'skip-caching' : 'true' }
    }

    if(obj.method === "POST"){
      requestConfig.body = obj.requestbody;
    }

    try {

      rq( requestConfig, (error, response, body) => {

        if(error){
         console.log(error);
         return;
        }

        try{
          getTimings(response);
          if(lineNumber % 100 === 0 && response && response.statusCode) {
           outlogfs.write(`${new Date()}: ${response.statusCode} ${lineNumber} \n`)
          }
          const outLine = `{  "ts" : "${timestamp}", "status" : ${response.statusCode || 0}, "time" : ${response.elapsedTime || 0}, "method" : ${obj.method}, "path": ${obj.path} } \n`
          outfs.write(outLine);
        } catch(err) {
          console.log(err);
        }
      });
    } catch(rqErr) {
     console.log(rqErr)
    }

    if(lineNr % nConcurrent === 0){
      setTimeout(function () {
          s.resume();
        }, nDelay)
    } 

  })
  .on('error', function(err){
    console.log(`Error while reading file. line# ${lineNr}`, err);
  })
  .on('end', function(){
    outlogfs.write('Read entire file.');
    //give any outstanding requests 10 seconds to complete
    setTimeout(function () {
        outlogfs.write('Exiting');
        process.exit(0);
      }, 10000)
  })
);
