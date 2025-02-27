import msal
import logging
import requests
import json
import base64
import mimetypes
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

EMAIL_TEMPLATES = {
    "EXISTING_PE_NO_SA_CONN": {
        "from": "svcedapucdr@pepsico.com",
        "to": "wagner.silveira@databricks.com",
        "subject": "Private Endpoint without Storage Account Connectivity",
        "message": """Please check the private endpoint for the storage account `{sa_name}` to the VNet `{vnet_name}` and SubNet `{subnet_name}`. 
        The private endpoint is not able to connect to the storage account. 
        No Service Now request has been created.
        
        Originated from Workspace: {workspace_url}"""
    },
    "SA_WITHOUT_CONN": {
        "subject": "Storage Account Monitoring - Found SA(s) without connectivity",
        "message": """There was(were) Storage Account(s) detected without connectivity to the Restore Workspace ({workspace_url}).
        <br>
        <br>
        To verify the connectivity, execute the following command from the Restore Workspace ({workspace_url}): 
        <br>
        <br>
        "%sh nc -zv &lt;Replace by Storage Account Host&gt; 443"
        <br>
        <br>
        {html_table}

        """
    },
    "NEW_SNOW_ORDER": {
        "from": "svcedapucdr@pepsico.com",
        "to": "wagner.silveira@databricks.com",
        "subject": "New ServiceNow Order {snow_order_id}",
        "message": "A new ServiceNow order was created to provision a Private Endpoint for the storage account `{sa_name}` to the VNet `{vnet_name}` and SubNet `{subnet_name}`. The order ID is `{snow_order_id}`. Originated from Workspace: {workspace_url}"
    },
    "BACKUP_FAILURE": {
        "from": "svcedapucdr@pepsico.com",
        "to": "wagner.silveira@databricks.com",
        "subject": "Backup Failure - Object Detected without DDL and API Payload values for Recovery",
        "message": """There was(were) object(s) detected without DDL and API Payload values for recovery.
        Run this query against the Backup Table to identify the object(s):
        ```
            SELECT * FROM delta.`backup_table_location` WHERE restore_ddl IS NULL AND api_payload IS NULL;
        ```
        
        Originated from Workspace: {workspace_url}"""
    },
    "BACKUP_STATS": {
        "from": "svcedapucdr@pepsico.com",
        "to": "naeem.akhtar.contractor@pepsico.com;rakesh.kundarapu.contractor@pepsico.com;josephraymond.tena.contractor@pepsico.com;ravi.munaganuri@pepsico.com",
        "subject": "DR Backup Stats from {workspace_url}",
        "message": """Backed Up Objects stats:
        <br>
        <br>
        Backup Table Version: {backup_table_version}; Backup Table Timestamp: {backup_table_timestamp}.
        <br>
        <br>
        {html_table}
        <br>
        <br>
        Originated from Workspace: {workspace_url}"""
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
            b64_content = base64.b64encode(open(filename,'rb').read())
            mime_type =  mimetypes.guess_type(filename)[0]
            mime_type = mime_type if mime_type else ''
            attached_files.append( \
                {
                    '@odata.type' : '#microsoft.graph.fileAttachment',
                    'ContentBytes' : b64_content.decode('utf-8'),
                    'ContentType' : mime_type,
                    'Name' : filename.split("/")[-1]  })
		
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