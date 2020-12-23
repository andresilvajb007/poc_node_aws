var sql = require('mssql');
var AWS = require('aws-sdk'),
    region = "us-east-1",
    secretName = "sql-secret",
    secret,
    decodedBinarySecret;


var bucket = ''
var sqsURL = '' 

AWS.config.update({region: 'us-east-1'});

// Create an SQS service object
var sqs = new AWS.SQS({apiVersion: '2012-11-05'});

async function buscaQuantidadeMensagensNaFila(){
  try {
    var queParams = {
      QueueUrl: sqsURL,
      AttributeNames : ['ApproximateNumberOfMessages'],
     };
     
     let fo =  await sqs.getQueueAttributes(queParams).promise();

     return Number(fo.Attributes.ApproximateNumberOfMessages);
      
  } catch (error) {
    console.log(error);
  }
};

async function buscaMensagensNaFila(){
  try {

    var params = {
      AttributeNames: [
          "All"
      ],
      MaxNumberOfMessages: 2,
      MessageAttributeNames: [
          "All"
      ],
      QueueUrl: sqsURL,
      VisibilityTimeout: 30,
      WaitTimeSeconds: 20
    };
     
     let reponse =  await sqs.receiveMessage(params).promise();

     return reponse.Messages;
      
  } catch (error) {
    console.log(error);
  }
};

async function RemoveMensagemDaFila(ReceiptHandle){

  try {

    var deleteParams = {
      QueueUrl: sqsURL,
      ReceiptHandle: ReceiptHandle
    };

    let response =  await sqs.deleteMessage(deleteParams).promise();

    return response;

  } catch (error) {
    console.log(error);
  }   
}

async function buscaArquivoS3(key){

  try {
     let s3 = new AWS.S3();
     let reponse =  await s3.getObject({ Bucket: bucket, Key: key }).promise();

     return reponse;
      
  } catch (error) {
    console.log(error);
  }
};

async function InsertTiff(RxDate, uProcesso, config){
  
  try{
  
    await sql.connect(config);
    
    var request = new sql.Request();
    request.input('xxx', sql.VarChar(14), RxDate);
    request.input('xxx', sql.Int(), 72);
    request.input('xxx', sql.VarChar(20), uProcesso);
    request.output('xxx',sql.Int);

    var result = await request.execute('spInsertTiff');

    return result.output.FSKeyID;
  
  }
  catch(error){
    console.log(error);
  }
};

async function AtualizaBaseComArquivo(FSKeyID, bytes, config){
  
  try{

      await sql.connect(config);
    
      var request = new sql.Request();
      request.input('RXFile', sql.VarBinary(sql.MAX), bytes);
    
      let query  ='update database..table set xxx = @xxx where  xxx =' + xxx;
      var response = await request.query(query);
    
      return true;
  }
  catch(error){
    console.log(error);

    return false;
  }
};

async function GetDbConfig(){
  
  try{

    var client = new AWS.SecretsManager({
      region: region
    });
  
    const params = {SecretId: secretName};
    const rc = await client.getSecretValue(params).promise();
    let secret_json = JSON.parse(rc.SecretString);
  
  
    var dbConfig = {
      user: secret_json.username,
      password: secret_json.password,
      server: secret_json.host, // You can use 'localhost\\instance' to connect to named instance
      database: 'XXX'
    };

    return dbConfig;

  }
  catch(error){
    console.log(error);
    throw error;
  }
};

async function handler(){


  var dbConfig = await GetDbConfig();  
  
  var quantidadeMensagens =  await buscaQuantidadeMensagensNaFila();
  quantidadeMensagens = 23;
  var quantidadeChamadasFila = quantidadeMensagens / 10;

  console.log("Quantidade de mensagens na fila: ", quantidadeMensagens);

  if(quantidadeMensagens){

    for (let chamada = 1; chamada <= quantidadeChamadasFila; chamada++) {
      
      var mensagens =  await buscaMensagensNaFila();

      if(mensagens){
        for (let index = 0; index < mensagens.length; index++) {
    
          var mensagem = JSON.parse(mensagens[index].Body);
          var numeroProcesso = mensagem.chargeBackId.split("-")[0];
      
          var arquivo =  await buscaArquivoS3(mensagem.keyArquivo);
          var id =  await InsertTiff(mensagem.dataInclusao, numeroProcesso,dbConfig);
      
          if(id){
  
            var atualizada =  await AtualizaBaseComArquivo(id, arquivo.Body,dbConfig);
  
            if(atualizada){
              await RemoveMensagemDaFila(mensagens[index].ReceiptHandle);
            }              
          }
    
        }
      }      
    }

  }
  else{
    console.log("Sem itens na fila para serem processados.");
  }

}

handler().then(v=> console.log("Fim de processamento."));


