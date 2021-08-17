var AWS = require("aws-sdk");
var uuid = require("uuid");

const HouseTableName = 'HouseTest'
const UserTableName ='UserTest'
AWS.config.update({region: 'eu-west-1'});
// Create the Service interface for DynamoDB
var dynamodb = new AWS.DynamoDB();

// Create the Document Client interface for DynamoDB
var ddbDocumentClient = new AWS.DynamoDB.DocumentClient();
exports.handler = async (event) => {
    const requestBody = JSON.parse(event.body);
    try{
        const result = await createNewUserItem(requestBody)
        const response = {
            statusCode: 200,
            body: JSON.stringify(result),
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


async function createNewUserItem(userObject){
    try{
        var params = { 
            TableName: "UserTest",
            Item:{
                UserID:  uuid.v1(),
                HouseID:userObject.houseID,
                userDetails:{
                    firstName:userObject.firstName, 
                    lastNsme:userObject.lastName,
                    email: userObject.email,
                    PhoneNumber: userObject.PhoneNumber,
                },
                Address:{
                    state:userObject.AddressState,
                    city:userObject.AddressCity,
                    street:userObject.AddressStreet,
                    streetNumber:userObject.AddressStreetNumber,
                    entrance:userObject.AddressEntrance,
                    apartment:userObject.AddressApartment
                },
                Constaints:{
                    numberOfHouseMembers:userObject.numberOfHouseMembers,
                    electricityBudget:userObject.electricityBudget,
                    waterBudget:userObject.waterBudget
                }    
             }
        }
        var result = await ddbDocumentClient.put(params).promise();
        return result;
    }catch(err){
        return err;
    }
}
