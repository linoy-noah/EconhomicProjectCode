
var AWS = require("aws-sdk");


const NotificationTableName = 'NotificationsTest'
const UserTableName = 'UserTest'


AWS.config.update({
    region: 'eu-west-1'
});
// Create the Service interface for DynamoDB
var dynamodb = new AWS.DynamoDB();

// Create the Document Client interface for DynamoDB
var ddbDocumentClient = new AWS.DynamoDB.DocumentClient();
exports.handler = async (event) => {
    // TODO implement
    const requestBody = JSON.parse(event.body);
    let response;
    try{
        const user = await getUserByEmail(requestBody.userEmail)
        const userNotifications = await getUserNotifications(user.Items[0].UserID)
         response = {
            statusCode: 200,
            body: JSON.stringify(userNotifications),
        };
    }catch(err){
        response = { 
              statusCode: 400,
            body: JSON.stringify({"err":err}),
        }
    }
    
    return response;
};


async function getUserNotifications(userID) {
    try {
         var params = { 
            TableName: NotificationTableName,
            FilterExpression: "UserID = :uid",
             ExpressionAttributeValues: { 
                ":uid":userID
              }
        }
        let result = []
        const items = await ddbDocumentClient.scan(params).promise();
        items.Items.forEach((item) => {
                result.push(item)
        })
        return result;
    } catch (err) {
        console.log("getUserNotifications err", err)
        return err;
    }
}

async function getUserByEmail(email) {
  
    try {
         var params = { 
            TableName: UserTableName,
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