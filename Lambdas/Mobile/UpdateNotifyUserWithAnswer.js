

let AWS= require('aws-sdk');
AWS.config.update({region: 'eu-west-1'});
const NotifyUserTableName = 'NotifyUser'
let ddbDocumentClient = new AWS.DynamoDB.DocumentClient();
exports.handler = async (event) => {
       const requestBody = JSON.parse(event.body);
    console.log(requestBody)
    let response;
    try{
        const notifyUserId = await getNotifyUserByNotificationID(requestBody.notificationID)
        const updateNotifyUserResult = await updateNotifyUser(notifyUserId, requestBody.userAnswer)
         response = {
            statusCode: 200,
            body: JSON.stringify({updateNotifyUserResult}),
        };
    }catch(err){
        response = { 
            statusCode: 400,
            body: JSON.stringify({"err":err, "reqBody":requestBody}),
        }
    }
    
    return response;
};

const getNotifyUserByNotificationID = async (notificationID) => {
     try {
        let params = {
            TableName: NotifyUserTableName,
            FilterExpression: "notificationID =:notificationID",
            ExpressionAttributeValues: {
                ":notificationID": notificationID
            }
        }
        let result = [];
        const items = await ddbDocumentClient.scan(params).promise();
        items.Items.forEach((item) => result.push(item));
        return result[0].NotifyUserID;
    } catch (err) {
        return err;
    }
}

const updateNotifyUser = async (notifyUserId, userAnswer) => {
     let params = {
        TableName: NotifyUserTableName,
        Key: {
            "NotifyUserID":notifyUserId,
        },
        UpdateExpression: "set userAnswer = :userAnswer",
        ExpressionAttributeValues: {
            ":userAnswer":userAnswer,
        },
    };
    try {
        const result = await ddbDocumentClient.update(params).promise()
        return result;
    } catch (err) {
        return err
    }
}