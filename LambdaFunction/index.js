'use strict';
const aws = require('aws-sdk');
const async = require('async');

const s3 = new aws.S3({ apiVersion: '2006-03-01' });
const comprehend = new aws.Comprehend();
const translate = new aws.Translate();
const firehose = new aws.Firehose();

var sentimentStream = process.env.SENTIMENT_STREAM;
var entityStream = process.env.ENTITY_STREAM;

exports.handler = (event, context, lambdaCallback) => {
 
    const bucket = event.Records[0].s3.bucket.name;
    const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    const params = {
        Bucket: bucket,
        Key: key,
    };
    s3.getObject(params, (err, data) => {
        if (err) {
            console.log(err);
            lambdaCallback(err);
        } else {
            var tweets = data.Body.toString('utf8');

            //lets go through each line in the doc now.
            var tweetArray = tweets.split('\n');

            var count = 0;

            async.forEachSeries(
                tweetArray, 
                function(tweetLine, itemCallback)
                {
                  if(tweetLine.length > 0)
                    {
                      count++;
                    
                      var tweet = JSON.parse(tweetLine);
                      async.waterfall([
                        //*********************************
                        //
                        //  Translate if needed function call.
                        //
                        //*********************************
                        function(doneTranslatingCallback){
                          if(tweet.lang !== 'en')
                          {
                            var translateParams = {
                                SourceLanguageCode: tweet.lang,
                                TargetLanguageCode: 'en', 
                                Text: tweet.text 
                              };
                              translate.translateText(translateParams, function(err, data) {
                                if (err){
                                  console.log('Error Translating: ' + err, err.stack);
                                  doneTranslatingCallback(err);
                                } 
                                else{
                                  console.log('Translated tweet\n\ttranslated: ' + data.TranslatedText + '\n\tfrom: ' + tweet.text);
                                  //This is a async callback...so need to do the call here or us a wait method..
                                  //will call here since it's not a lot of callback chaingin.
                                  tweet.originalText = tweet.text;
                                  tweet.text = data.TranslatedText;
                                  doneTranslatingCallback(null);
                                }   
                                
                              });
                          }//end of if not english
                          else
                          {
                            doneTranslatingCallback(null);
                          }
                        },
                        //*********************************
                        //
                        //  Run Sentiment Analysis
                        //
                        //*********************************
                        function(sentimentCallback){
                            var params = {
                                  LanguageCode: 'en', /* required */
                                  Text: tweet.text
                                };
                            
                            comprehend.detectSentiment(params, function(err, data) {
                              if (err){
                                console.log(err, err.stack); // an error occurred
                                sentimentCallback(err);
                              }
                              else{
                                  var sentimentRecord = {
                                          DeliveryStreamName: sentimentStream, 
                                          Record: { 
                                                Data: JSON.stringify({
                                                      tweetid: tweet.id, /* required */
                                                      text: tweet.text,
                                                      originalText: tweet.originalText,
                                                      sentiment: data.Sentiment,
                                                      sentimentPosScore: Number((data.SentimentScore.Positive).toFixed(3)),
                                                      sentimentNegScore: Number((data.SentimentScore.Negative).toFixed(3)),
                                                      sentimentNeuScore: Number((data.SentimentScore.Neutral).toFixed(3)),
                                                      sentimentMixedScore: Number((data.SentimentScore.Mixed).toFixed(3))
                                                    }) + '\n'
                                            }//end of record
                                        };//end of params
                                        firehose.putRecord(sentimentRecord, function(err, data) {
                                          if (err)
                                          { 
                                            console.log(err, err.stack);
                                            sentimentCallback(err);
                                          }
                                          else
                                            sentimentCallback(null);
                                        });
                                }    
                            });
                        },
                        //*********************************
                        //
                        //  Run Entity Extraction
                        //
                        //*********************************
                        function(entityCallback){
                           var params = {
                                  LanguageCode: 'en', /* required */
                                  Text: tweet.text
                                };
                                
                          comprehend.detectEntities(params, function(err, data) {
                              if (err){
                                console.log(err, err.stack); // an error occurred
                                entityCallback(err);
                              }
                              else{
                                  data.Entities.forEach(function(entity){
                                       var entityRecord = {
                                          DeliveryStreamName: entityStream, 
                                          Record: { 
                                                Data: JSON.stringify({
                                                      tweetid: tweet.id, /* required */
                                                      entity: entity.Text,
                                                      type: entity.Type,
                                                      score: entity.Score
                                                    }) + '\n'
                                            }//end of record
                                        };//end of params
                                        firehose.putRecord(entityRecord, function(err, data) {
                                          if (err) console.log(err, err.stack);
                                        });
                                  });
                                  entityCallback(null);
                                }
                            });
                        }
                    ],
                    //******************************************************
                    //
                    // Callback handler for waterfall flow -- this is when we call back to the item forEach...
                    //
                    //******************************************************
                    function (err, result) {
                        if(err)
                        {
                          console.log('exception processing "' + tweetLine + '" ' + err);
                        }
                        itemCallback();
                    });
                  }//is the tweet > 0
                }, 
                //******************************************************
                //
                // Callback handler for forEach block -- this is when we want to finish the lambda exec...
                //
                //******************************************************
                function(error)
                {
                  console.log('processed ' + count + ' tweets');
                  lambdaCallback(null, true);
                });//end async for each...
        }
    });
};