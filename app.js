var sql = require('mssql');
var AWS = require('aws-sdk');


var config = {
    user: 'sa',
    password: 'yourStrong(!)Password',
    server: 'localhost', // You can use 'localhost\\instance' to connect to named instance
    database: ''
}

var bucket = ''
var sqsURL = ''

AWS.config.update({region: 'us-east-1'});

// Create an SQS service object
var sqs = new AWS.SQS({apiVersion: '2012-11-05'});

var params = {
    AttributeNames: [
        "SentTimestamp"
    ],
    MaxNumberOfMessages: 1,
    MessageAttributeNames: [
        "All"
    ],
    QueueUrl: sqsURL,
    VisibilityTimeout: 20,
    WaitTimeSeconds: 0
};

sqs.receiveMessage(params, function(err, data_sqs) {
  if (err) {
    console.log("Receive Error", err);
  } else if (data_sqs.Messages) {

    for (let index = 0; index < data_sqs.Messages.length; index++) {

        const element = JSON.parse(data_sqs.Messages[index].Body);
        var key = element.keyArquivo
        var dataIn = element.dataInclusao

        new AWS.S3().getObject({ Bucket: bucket, Key: key }, function(err, data_s3)
        {
            if (!err)                
                console.log(data_s3.Body.toString());
                InsertProc(data_s3.Body, data_sqs.Messages[index].ReceiptHandle);
        });        
        
    }
  }

});

 function  InsertProc(arquivo, ReceiptHandle){
    
    var fsKeyID = 0;

    sql.connect(config, function (err) {
        if (err) console.log(err);
    
        var request = new sql.Request();
        request.input('RxDate', sql.VarChar(14), '1234');
        request.input('PoolID', sql.Int(), 72);
        request.input('uProcesso', sql.VarChar(20), '2020');
        request.output('FSKeyID',sql.Int)
    
        request.execute('spInsertTiff').then(function(result) {
    
          console.log(result);
          fsKeyID = result.output.FSKeyID; 

          request = new sql.Request();
          request.input('RXFile', sql.VarBinary(sql.MAX), arquivo);

          let query  ='update FileStore..RXFiles set RXFile = @RXFile where  KeyID =' + fsKeyID;
          request.query(query, (err, result) => {

            console.log(result)
            DeleteMessage(ReceiptHandle);  

            });          

    
        }).catch(function(err) {
          console.log(err);
        });
    });

}


function DeleteMessage(ReceiptHandle){
    var deleteParams = {
      QueueUrl: sqsURL,
      ReceiptHandle: ReceiptHandle
    };


    sqs.deleteMessage(deleteParams, function(err, data) {
      if (err) {
        console.log("Delete Error", err);
      } else {
        console.log("Message Deleted", data);
      }
    });
}

//  Exemplo de Json da fila
exemplo_objeto_fila = {
     "nomeArquivo": "2001098989999-1-1-1.pdf",
     "keyArquivo": "Documento/Estbelecimento/2001098989999-1-1-1.pdf",
     "chargeBackId": "2001098989999-1-1-1",
     "dataInclusao": "20201202103155999"       
 }
