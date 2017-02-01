var Promise = require('bluebird');
var AWS = require("aws-sdk");
AWS.config.update({region:'us-east-1'});
AWS.config.setPromisesDependency(Promise);


class Agent{
    constructor(lambdaManagerName){
        this.lambda = new AWS.Lambda();
        this.managerFunctionName = lambdaManagerName;
    }

    addJob(functionName,payload){
        var payload =
        {
            command:'addJob',
            params: {
                functionName: functionName,
                data:payload
            }
        };
        return this._executeLambda(this.managerFunctionName,payload)
    }
    jobFinished(functionName){
        var payload =
        {
            command:'jobFinished',
            params: {
                functionName: functionName
            }
        };
        return this._executeLambda(this.managerFunctionName,payload)
    }

    _executeLambda(functionName,data){
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

export default Agent
