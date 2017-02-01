var uuid = require('node-uuid');
var AWS = require("aws-sdk");
var Promise = require('bluebird');
var _ = require('lodash');
var config = require('./config');


AWS.config.update({region:'us-east-1'});
AWS.config.setPromisesDependency(Promise);

var redis = require('redis');
Promise.promisifyAll(redis.RedisClient.prototype);
Promise.promisifyAll(redis.Multi.prototype);


exports.manage = function(event,context,cb){
    context.callbackWaitsForEmptyEventLoop = false;
    console.log(event);
    console.log(context);

    var total = new Date();
    var start  = new Date();

    var lambdaManager = new LambdaManager(config.redisHost,config.redisPort,config.redisPassword);

    start = new Date();

    var promise = null;
    var params = event.params;

    if (event.command == "addJob") {
        promise = lambdaManager.addJob(params.functionName,params.data);
    } else if (event.command == "jobFinished") {
        promise = lambdaManager.jobFinished(params.functionName);
    } else if (event.command == "reset"){
        promise = lambdaManager.reset();
    }

    promise.catch(function(err){
        console.log("Error: ",err);
    })
    .finally(lambdaManager.quit.bind(lambdaManager)).finally(function(){
        console.log("Command handle took: ", (new Date() - start) / 1000);
        cb();
    });


};

class LambdaManager{
    constructor(redisHost,redisPort,redisPassword){
        this.client = redis.createClient(redisPort, redisHost, {no_ready_check: true,password:redisPassword});
        this.lambda = new AWS.Lambda();

    }
    reset(){
        var self = this;
        return this.client.flushdbAsync();
    }
    addJob(functionName,data){
        var self = this;
        console.log("Handling addJob command");
        return self._addJobToQueue(functionName,data)
            .then(function(){
                return self._runJob(functionName).catch(function(err){
                    console.log("Error: ",err);
                });
            });

    }

    jobFinished(functionName){
        var self = this;

        return self._getJob(functionName).then(function (data) {
            if (data) {
                return self.executeLambda(functionName, data);
            } else {
                return self.client.rpoplpushAsync('Running-' + functionName, "Workers-" + functionName).then(function (worker) {
                    if (worker) {
                        console.log("Restoring worker:", worker);
                    } else {
                        console.log("No worker to restore");
                    }
                });
            }
        });
    }
    quit(){
        return this.client.quitAsync();
    }
    _runJob(functionName){
        console.log("Checking for availble worker",functionName);
        var self = this;
        return self.client.rpoplpushAsync('Workers-'+functionName,"Running-"+functionName).then(function(worker){
            if(worker){
                console.log("Found worker:",worker);
                return self._getJob(functionName).then(function(data){
                    if(data){
                        return self.executeLambda(functionName,data);
                    }
                });
            }else{
                console.log("No worker is available");
            }
        });
    }
    _getJob(functionName){
        var self = this;
        console.log("Receiving a job");
        return self.client.rpopAsync("Jobs-"+functionName).then(function(data){
            if(data){
                console.log("Received a job",data);
                return JSON.parse(data);
            }else{
                console.log("======== NO JOB ON QUEUE =======");
                return null;
            }
        })
    }
    _addJobToQueue(functionName,data){
        var self = this;
        console.log("Adding a job to queue: ",functionName,data);
        return self.client.lpushAsync("Jobs-"+functionName,JSON.stringify(data));
    }

    executeLambda(functionName,data){
        var params = {
            FunctionName: functionName, // the lambda function we are going to invoke
            InvocationType: 'Event',
            LogType: 'Tail',
            Payload:JSON.stringify(data)
        };
        console.log("Invoking lambda: ",params);
        return this.lambda.invoke(params).promise();

    }
}

