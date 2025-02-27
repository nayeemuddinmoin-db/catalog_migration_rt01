import msal
import logging
import requests
import json
import base64
import mimetypes
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

EMAIL_TEMPLATES = {
    "RESTORE_REPORT": {
        "from": "Shavali.Pinjari.Contractor@pepsico.com",
        "to": ["nayeemuddin.moinuddin@databricks.com"],
        "subject": "Restore Report - Job ID - {jobid}",
        "message": ""
    }
}

logger = logging.getLogger(__name__)


def sendEmailGeneric(credential_scope,emailfrom,subject,message,receiver,attachments=None,client_id=None,scope=None,authority=None) :

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    secret = dbutils.secrets.get(scope=credential_scope, key="email-secret")

    if client_id==None:
        client_id="b4d11fce-40b6-4674-b2bc-0dff8ebe4338"
    if authority==None:
        authority="https://login.microsoftonline.com/42cc3295-cd0e-449c-b98e-5ce5b560c1d3"
    if scope==None:
        scope=["https://graph.microsoft.com/.default"]

    endpoint="https://graph.microsoft.com/v1.0/users/{}/sendMail".format(emailfrom)


    # Create a preferably long-lived app instance which maintains a token cache.
    app = msal.ConfidentialClientApplication(client_id, authority=authority,client_credential=secret,)

    # The pattern to acquire a token looks like this.
    result = None
    
    # Firstly, looks up a token from cache
    # Since we are looking for token for the current app, NOT for an end user,
    # notice we give account parameter as None.
    result = app.acquire_token_silent(scope, account=None)

    if not result :
        logger.info("No suitable token exists in cache. Let's get a new one from AAD.")
        result = app.acquire_token_for_client(scopes=scope)
    attached_files = []
    if attachments:
        for filename in attachments.split(","):
            #print("Working on File Name :"+filename)
            # b64_content = base64.b64encode(open(filename,'rb').read())
            data = ''
            for i in spark.read.text(filename).collect():
                data = data + i[0] + '\n'

            b64_content = base64.b64encode(data.encode('utf-8'))
            mime_type =  mimetypes.guess_type(filename)[0]
            mime_type = mime_type if mime_type else ''
            attached_files.append( \
                {
                    '@odata.type' : '#microsoft.graph.fileAttachment',
                    'ContentBytes' : b64_content.decode('utf-8'),
                    'ContentType' : mime_type,
                    'Name' : filename.split("/")[-1]+".csv"  })
		
    if "access_token" in result :
        # Calling graph using the access token
        cbody = {
            "message": {
            "subject": subject,
            "body": {
            "contentType": "HTML",
            "content": message
            },
            "toRecipients": [],
			"attachments" : attached_files
            },
            "saveToSentItems": "false"
        }
        #print(result['access_token'])
        [cbody["message"]["toRecipients"].append({"emailAddress":{"address":i}}) for i in receiver.split(";")]
        graph_data = requests.post(endpoint,data=json.dumps(cbody),headers={'Authorization': 'Bearer ' + result['access_token'],'Content-Type': 'application/json'},)
        #print("Graph API call result: %s" % json.dumps(graph_data, indent=2))
        logger.info("Graph API call result: %s" % graph_data)
    
    else :
        logger.error(result.get("error"))
        logger.error(result.get("error_description"))
        logger.error(result.get("correlation_id"))  # You may need this when reporting a bug