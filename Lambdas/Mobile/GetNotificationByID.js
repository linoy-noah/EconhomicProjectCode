var AWS = require("aws-sdk");
const NotificationTableName = 'NotificationsTest'

AWS.config.update({region: 'eu-west-1'});

// Create the Document Client interface for DynamoDB
var ddbDocumentClient = new AWS.DynamoDB.DocumentClient();

exports.handler = async (event) => {
   const requestBody = JSON.parse(event.body);
    try{
        const notification = await getNotificationByID(requestBody.notificationID)
        
        const response = {
            statusCode: 200,
            body: JSON.stringify({notification}),
        };
         return response;

    }catch(err){
        
        const response = {
            statusCode: 400,
            body: JSON.stringify(err),
        };
         return response;
    }
    
};
async function getNotificationByID(notificationID){
    try{
        var params = { 
            TableName: NotificationTableName,
            Key:{
                "NotificationsID":notificationID
            }
        }
        var result = await ddbDocumentClient.get(params).promise()
        return result;
    }catch(err){
        return err;
    }
}
