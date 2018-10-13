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
const statResponseTimeSearch = new Stats();
const statResponseTimeRefinement = new Stats();
const statResponseTime = new Stats();

const statsInterval = 5000;
let timeElapsed = 0;

setInterval(function () {
  timeElapsed += statsInterval/1000;
  let srt = statResponseTimeSearch;
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
  let tsvSummary = `${timeElapsed}\tsrch\t${srt.length}\t${srt.length/(statsInterval/1000)}\t${srt.percentile(50)}\t${srt.percentile(75)}\t${srt.percentile(95)}\t${srt.range()[0]}\t${srt.range()[1]}\n`;
  outmetricfs.write( tsvSummary );
  statResponseTimeSearch.reset();
 
  srt = statResponseTimeRefinement;
  tsvSummary = `${timeElapsed}\trefn\t${srt.length}\t${srt.length/(statsInterval/1000)}\t${srt.percentile(50)}\t${srt.percentile(75)}\t${srt.percentile(95)}\t${srt.range()[0]}\t${srt.range()[1]}\n`;
  outmetricfs.write( tsvSummary );
  statResponseTimeRefinement.reset();  

  srt = statResponseTime;
  tsvSummary = `${timeElapsed}\tallq\t${srt.length}\t${srt.length/(statsInterval/1000)}\t${srt.percentile(50)}\t${srt.percentile(75)}\t${srt.percentile(95)}\t${srt.range()[0]}\t${srt.range()[1]}\n`;
  outmetricfs.write( tsvSummary );
  statResponseTime.reset();  

}, 5000)


//from: https://stackoverflow.com/questions/16010915/parsing-huge-logfiles-in-node-js-read-in-line-by-line

var s = fs.createReadStream(filename)
  .pipe(es.split())
  .pipe(es.mapSync(function(line){

    lineNr += 1;
    if(lineNr % nConcurrent === 0){
    	// pause the readstream
    	s.pause();
	}

    //parse the json
    //const obj = JSON.parse(line);

    //if(obj.parsedBody.originalQuery){ //refinement request
    if(line.startsWith('{\"originalQuery')) { //refinement request
      const lineNumber = lineNr;
      const timestamp = moment().toISOString();
      try {
        rq.post(     
          {   uri: `https://${cluster}/api/v1/search/refinements`
          , time: true
          , body: line
          , headers : { 'skip-caching' : 'true' }
        }, (error, response, body) => {

          if(error){
           console.log(error);
           return;
         }

         try{
          if(response.elapsedTime)  statResponseTimeRefinement.push( response.elapsedTime );
          if(response.elapsedTime)  statResponseTime.push( response.elapsedTime );
          if(lineNumber % 100 === 0 && response && response.statusCode) {
           outlogfs.write(`${new Date()}: ${response.statusCode} ${lineNumber} refinement \n`)
         }
         const outLine = `{  "ts" : "${timestamp}", "status" : ${response.statusCode || 0}, "time" : ${response.elapsedTime || 0}, "type" : "refinement" } \n`
         outfs.write(outLine);
       } catch(err) {
        console.log(err);
      }

    });
      } catch(rqErr) {
       console.log(rqErr)
     }

        } else { //search request

          const lineNumber = lineNr;
          const timestamp = moment().toISOString();
          try {
            rq.post(     
              {   uri: `https://${cluster}/api/v1/search`
              , time: true
              , body: line
              , headers : { 'skip-caching' : 'true' }
            }, (error, response, body) => {

              if(error){
               console.log(error);
               return;
             }

             try {
              if(response.elapsedTime)  statResponseTimeSearch.push( response.elapsedTime );
              if(response.elapsedTime)  statResponseTime.push( response.elapsedTime );
              if(lineNumber % 100 === 0 && response && response.statusCode) {
               outlogfs.write(`${new Date()}: ${response.statusCode} ${lineNumber} search \n`)
             }
             const outLine = `{  "ts" : "${timestamp}", "status" : ${response.statusCode || 0}, "time" : ${response.elapsedTime || 0}, "type" : "search" } \n`
             outfs.write(outLine);
           } catch(err) {
            console.log(err);
          }

        });
          } catch(rqErr) {
            console.log(rqErr);
         }

       }


       if(lineNr % nConcurrent === 0){
            //add timeout to throttle?
            setTimeout(function () {
                // resume the readstream, possibly from a callback
                s.resume();
              }, nDelay)
          } 

        })
  .on('error', function(err){
    console.log(`Error while reading file. line# ${lineNr}`, err);
  })
  .on('end', function(){
    outlogfs.write('Read entire file.')
  })
);
