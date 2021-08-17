var AWS = require("aws-sdk");

const PLATFORM_APPLICATION_ARN = "arn:aws:sns:eu-west-1:047268288390:app/GCM/EconhomeicTesting"
const MobileDeviceTableName = 'MobileDeviceTest'
const UserTableName = 'UserTest'

AWS.config.update({
    region: 'eu-west-1'
});
// Create the Service interface for DynamoDB
var dynamodb = new AWS.DynamoDB();

// Create the Document Client interface for DynamoDB
var ddbDocumentClient = new AWS.DynamoDB.DocumentClient();

var sns = new AWS.SNS();

exports.handler = async (event) => {
    const requestBody = JSON.parse(event.body);
    console.log(requestBody)
    let respons;
    let deviceItem;
    let userID;
    try{
        userID = await getUserByEmail(requestBody.userEmail);
        console.log("getUserByEmail result", userID)
        if(userID.Count != 0){
            console.log("userID count is not 0")
            userID = userID.Items[0].UserID
            console.log("userID",userID)
        }
        deviceItem = await getDeviceByID(requestBody.deviceID, userID);
        console.log("getDeviceByID result ", deviceItem)
        if(deviceItem.Count != 0){
            console.log("deviceItem count is not 0")
            deviceItem = deviceItem.Items[0]
            console.log("deviceItem result ", deviceItem)
        }
        
        
        switch (requestBody.case) {
        case 'generate':
            console.log("in generate case")
            const ARNResult = await createEndpointForFCMToken(requestBody.FCMToken)
            console.log("createEndpointForFCMToken result", ARNResult )
            const createDeviceResult = await createDeviceItem(requestBody,userID, ARNResult)
            console.log("createDeviceItem result", createDeviceResult )
            respons = {
                statusCode: 200,
                body: JSON.stringify({
                    "responseMsg": "generated",
                    "createDeviceResult":createDeviceResult

                })
            }
            break;
        case 'check':
            console.log("in check case")
            console.log("deviceItem", deviceItem)
            if(deviceItem.FCMToken != requestBody.FCMToken){
                console.log("FCM tokens are diffrent")
                console.log("deviceItem.FCMToken",deviceItem.FCMToken)
                console.log("requestBody.FCMToken",requestBody.FCMToken)
                const ARNResult = await createEndpointForFCMToken(requestBody.FCMToken)
                console.log("createEndpointForFCMToken result", ARNResult )
                const updateDeviceResult = await updateDeviceFCMAndARN(requestBody, ARNResult)
                console.log("updateDeviceFCMAndARN result", updateDeviceResult )
                  respons = {
                    statusCode: 200,
                    body: JSON.stringify({
                        "responseMsg": "check & update",
                        "updateDeviceResult":updateDeviceResult
                    })
                }
            }else{
                console.log("FCM tokens are the same")
                respons = {
                    statusCode: 200,
                    body: JSON.stringify({
                        "responseMsg": "check",
                    })
                }
            }
            
            break;
        case 'deactivate':
            console.log("in deactivate case")
            const deleteDeviceEnpointResult = await deleteDeviceEnpoint(deviceItem.TokenARN)
            console.log("deleteDeviceEnpoint result", deleteDeviceEnpointResult )
            const deleteDeviceItemResult = await deleteDeviceItem(requestBody.deviceID);
            console.log("deleteDeviceItem result", deleteDeviceItemResult )

             respons = {
                statusCode: 200,
                body: JSON.stringify({
                    "responseMsg": "deactivated",
                    "deleteDeviceItemResult":deleteDeviceItemResult

                })
            }
            break;
        
        default:
        
             respons = {
                statusCode: 200,
                body: JSON.stringify({
                    "responseMsg": "default",
                    "userID": userID, 
                    "deviceItem":deviceItem

                })
            }
    }
    
        
    }catch(err){
        console.log(err)
       
    }
    
    
    return respons
}


async function getUserByEmail(email) {
  
    try {
         var params = { 
            TableName: "UserTest",
            FilterExpression: "userDetails.email = :email",
             ExpressionAttributeValues: { 
                ":email":email
              }
        }
        const result = await ddbDocumentClient.scan(params).promise();
        return result;
    } catch (err) {
        console.log("getUserByEmail err", err)
        return err;
    }
}

async function getDeviceByID(deviceId, userID) {
    try {
        let params = {
            TableName: MobileDeviceTableName,
            FilterExpression: "DeviceID = :deviceID and UserID =:userID",
            ExpressionAttributeValues: {
                ":deviceID": deviceId,
                ":userID": userID
            }
        }
        const result = await ddbDocumentClient.scan(params).promise();
        return result;
    } catch (err) {
        console.log("getDeviceByID err", err)
        return err;
    }
}


async function createEndpointForFCMToken(FCMToken) {
    let params = {
        PlatformApplicationArn: PLATFORM_APPLICATION_ARN,
        Token: FCMToken
    };
    try {
        const result = await sns.createPlatformEndpoint(params).promise()
        return result;
    } catch (err) {
        console.log("createEndpointForFCMToken err", err)
        return err
    }
}

async function updateDeviceFCMAndARN(userInfo, tokenArn) {
    let params = {
        TableName: MobileDeviceTableName,
        Key: {
            "DeviceID": userInfo.deviceID,
        },
        UpdateExpression: "set FCMToken=:fcmToken, TokenARN=:tokenArn",
        ExpressionAttributeValues: {
            ":fcmToken": userInfo.FCMToken,
            ":tokenArn": tokenArn.EndpointArn

        },
    };
    try {
        const result = await ddbDocumentClient.update(params).promise()
        return result;
    } catch (err) {
        console.log("updateDeviceFCMAndARN err", err)
        return err
    }
}


async function createDeviceItem(userInfo,userID, tokenARN) {
    let params = {
        TableName: MobileDeviceTableName,
        Item: {
            DeviceID: userInfo.deviceID,
            UserID: userID,
            FCMToken: userInfo.FCMToken,
            TokenARN: tokenARN.EndpointArn,
            active: true
        }
    }
    try {
        const result = await ddbDocumentClient.put(params).promise()
        return result
    } catch (err) {
        console.log("createDeviceItem err", err)
        return err
    }
}


async function deleteDeviceEnpoint(ARNtoken) {
    let params = {
        EndpointArn: ARNtoken
    };
    try {
        const result = await sns.deleteEndpoint(params).promise();
        return result;
    } catch (err) {
        console.log("err from deleteDeviceEnpoint", err)
        return err
    }
}

async function deleteDeviceItem(deviceID) {
    let params = {
        TableName: MobileDeviceTableName,
        Key: {
            "DeviceID": deviceID,
        }
    }
    try {
        const result = await ddbDocumentClient.delete(params).promise()
        return result
    } catch (err) {
        console.log("err on deleteDeviceItem", err)
        return err
    }
}