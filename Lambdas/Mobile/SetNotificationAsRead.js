var AWS = require("aws-sdk");

const NotificationTableName = 'NotificationsTest'

AWS.config.update({
    region: 'eu-west-1'
});
// Create the Service interface for DynamoDB
var dynamodb = new AWS.DynamoDB();
var ddbDocumentClient = new AWS.DynamoDB.DocumentClient();
exports.handler = async (event) => {
    
    // TODO implement
       const requestBody = JSON.parse(event.body);
    let response;
    try{
        const updateResult = await setNotificationAsRead(requestBody.notificationID)
         response = {
            statusCode: 200,
            body: JSON.stringify({"updateResult":updateResult, "reqBody":requestBody}),
        };
    }catch(err){
        response = { 
              statusCode: 400,
            body: JSON.stringify({"err":err, "reqBody":requestBody}),
        }
    }
    
    return response;
};


async function setNotificationAsRead(nid){
    let now = new Date()
    let params = {
        TableName: NotificationTableName,
        Key: {
            "NotificationsID":nid,
        },
        UpdateExpression: "set readAt = :readAt",
        ExpressionAttributeValues: {
            ":readAt":now.toLocaleString()
        },
    };
    try {
        const result = await ddbDocumentClient.update(params).promise()
        return result;
    } catch (err) {
        console.log("setNotificationAsRead err", err)
        return err
    }
}
