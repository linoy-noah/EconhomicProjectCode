
var AWS = require("aws-sdk");
var uuid = require("uuid");
const PLATFORM_APPLICATION_ARN = "arn:aws:sns:eu-west-1:047268288390:app/GCM/EconhomeicTesting"
const MobileDeviceTableName = 'MobileDeviceTest'
const NotificationTableName = "NotificationsTest"

AWS.config.update({
    region: 'eu-west-1'
});
// Create the Service interface for DynamoDB
var dynamodb = new AWS.DynamoDB();

// Create the Document Client interface for DynamoDB
var ddbDocumentClient = new AWS.DynamoDB.DocumentClient();

var sns = new AWS.SNS();
exports.handler = async (event) => {
    // TODO implement
    console.log(event)
    let response;
    try{
        const deviceItem = await getDeviceByUserID(event.UserID)
        const addNotificationResult = await addNotoficationToTable(event.UserID, event.title, event.msg, event.notificationType)
        let extraData = null;
        if(event.hasOwnProperty('extraData')){
            extraData = event.extraData
        }
        const publishResult = await publishMessage(deviceItem,  event.msg, event.title, addNotificationResult, event.notificationType, extraData)
        response = {
            statusCode: 200,
            body: JSON.stringify({
                "result":addNotificationResult,
            }),
        };
    }catch(err){
         response = {
            statusCode: 400,
            body: JSON.stringify({"err":err}),
        };
    }
    
    return response;
};

async function getDeviceByUserID(userID){
    
    try {
        let params = {
            TableName: MobileDeviceTableName,
            FilterExpression: "UserID =:userID",
            ExpressionAttributeValues: {
                ":userID": userID
            }
        }
        const result = await ddbDocumentClient.scan(params).promise();
        console.log("inside function: getDeviceByUserID result", result)
        return result;
    } catch (err) {
        console.log("getDeviceByID err", err)
        return err;
    }
}


async function publishMessage(device,msg,title, notificationUUID, notificationType, extraData){
    
    let payload = { 
        default: 'default', 
        GCM:{
            notification:{
                body:msg, 
                title:title
            }, 
            data:{
                notificationID: notificationUUID, 
                notificationType: notificationType, 
                extraData
            }
        }
    }
    
    payload.GCM = JSON.stringify(payload.GCM);
    payload = JSON.stringify(payload);
    
    let params = {
        Message: payload,
        MessageStructure: 'json',
          TargetArn: device.Items[0].TokenARN
        };
        try{
            const result = await sns.publish(params).promise()
            console.log("inside function: publishMessage result ", result)
            return result;
        }catch(err){
            console.log(err)
            return err
        }
    
}


async function addNotoficationToTable(userID, title, msg, notificationType){
    let now = new Date();
    let notificationUUID = uuid.v1()
    try{
        var params = { 
            TableName: NotificationTableName,
            Item:{
                NotificationsID: notificationUUID,
                UserID: userID,
                title: title, 
                msg: msg, 
                sentAt: now.toLocaleString(), 
                readAt: "unread", 
                notificationType: notificationType
            }
        }
        
        var result = await ddbDocumentClient.put(params).promise();
        console.log(result)
        return notificationUUID;
    }catch(err){
        return err;
    }
}
