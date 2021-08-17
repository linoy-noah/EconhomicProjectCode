
let AWS= require('aws-sdk');
AWS.config.update({region: 'eu-west-1'});
const lambda = new AWS.Lambda();
const NotifyUserTableName = 'NotifyUser'
let ddbDocumentClient = new AWS.DynamoDB.DocumentClient();
exports.handler = async (event) => {
    console.log(event.Records[0])
    
    if(event.Records[0].eventName === 'INSERT'){
        await sendUserActionNotification(event.Records[0].dynamodb)
    }
    // TODO implement
    const response = {
        statusCode: 200,
        body: JSON.stringify('Hello from Lambda!'),
    };
    return response;
};

const sendUserActionNotification = async (values) => {
    console.log(values)
      let params = {
      FunctionName: 'SendNotificationLambda', 
      Payload: JSON.stringify({
          title:"New Action Notification", 
          msg: values.NewImage.msgToUser["S"], 
          UserID: values.NewImage.UserID["S"], 
          notificationType:"action", 
          extraData:values.NewImage.NotifyUserID["S"]
          
      })
  }
  try{
      let result = await lambda.invoke(params).promise();
      result = JSON.parse(result.Payload) 
      result = JSON.parse(result.body)
      await updateNotifyUserNootificationID(values.NewImage.NotifyUserID["S"], result.result)
  }catch(err){
      console.log("err", err, err.stack)
  }
}

const updateNotifyUserNootificationID = async (NotifyUserID, notificationID) => {
        let params = {
        TableName: NotifyUserTableName,
        Key: {
            "NotifyUserID":NotifyUserID,
        },
        UpdateExpression: "set notificationID = :notificationID",
        ExpressionAttributeValues: {
            ":notificationID":notificationID,
        },
    };
    try {
        const result = await ddbDocumentClient.update(params).promise()
        return result;
    } catch (err) {
        console.log("setUpdateSettings err", err)
        return err
    }
}