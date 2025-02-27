from azure.identity import DefaultAzureCredential
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.subscription import SubscriptionClient
from azure.mgmt.network import NetworkManagementClient
from typing import Dict

import logging

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_az_storage_account_details(azure_creds: str, sa_name: str) -> Dict[str, str]:
    """
    Get details of a storage account in Azure

    Args:
        azure_creds (str): Name of the Azure credentials
        sa_name (str): Name of the storage account

    Returns:
        Dict[str, str]: Details of the storage account
    """

    # Create a credential object
    credential = DefaultAzureCredential()

    # Initialize Subscription Client
    subscription_client = SubscriptionClient(credential)

    # Get all subscriptions
    subscriptions = list(subscription_client.subscriptions.list())
    logger.info(f"{len(subscriptions)} subscription(s) fetched successfully")

    sa_details = {}
    # Iterate over subscriptions
    for subscription in subscriptions:

        # Initialize Storage Management Client for each subscription
        storage_client = StorageManagementClient(credential, subscription.subscription_id)

        # List storage accounts
        storage_accounts = storage_client.storage_accounts.list()

        # Print storage account details
        for account in storage_accounts:
            if account.name == sa_name:
                logger.info(f"Storage Account {sa_name} found in subscription {subscription.subscription_id}")

                sa_details["name"] = account.name
                sa_details["subscription_id"] = subscription.subscription_id
                sa_details["subscription_name"] = subscription.display_name
                sa_details["resource_group"] = account.id.split("/")[4]
                sa_details["location"] = account.location

                return sa_details

    logger.info(f"Storage Account {sa_name} not found in any subscription")

    return sa_details

def check_existing_pe(azure_creds: str,
                      sa_name: str,
                      pe_subs_id: str,
                      pe_rg_name: str,
                      pe_vnet_name: str,
                      pe_subnet_name: str) -> Dict[str, str]:
    """
    Check if a private endpoint exists for a storage account

    Args:
        azure_creds (str): Name of the Azure credentials
        sa_name (str): Name of the storage account
        pe_subs_id (str): Subscription ID of the private endpoint
        pe_rg_name (str): Resource Group name of the private endpoint
        pe_vnet_name (str): Virtual Network name of the private endpoint
        pe_subnet_name (str): Subnet name of the private endpoint

    Returns:
        Dict[str, str]: Details of the private endpoint
    """

    logger.info(f"Checking if Private Endpoint exists for Storage Account '{sa_name}' in Virtual Network '{pe_vnet_name}' and Subnet '{pe_subnet_name}'")
    
    # Initialize credentials
    credential = DefaultAzureCredential()

    # Initialize Network Management Client
    network_client = NetworkManagementClient(credential, pe_subs_id)

    # List all private endpoints in the resource group
    private_endpoints = list(network_client.private_endpoints.list(pe_rg_name))
    logger.info(f"{len(private_endpoints)} private endpoint(s) fetched successfully")

    # logger.info(f"Checking if Private Endpoint exists for Storage Account '{sa_name}'")

    pe_details = {}

    for pe in private_endpoints:

        net = pe.subnet.id.split("/")
        vnet_service = net[-4]
        vnet_name = net[-3]
        subnet_service = net[-2]
        subnet_name = net[-1]

        logger.info(f"Checking Private Endpoint '{pe.name}' in Virtual Network '{vnet_name}' and Subnet '{subnet_name}'")

        if (     (vnet_service == "virtualNetworks" and subnet_service == "subnets")
             and (vnet_name == pe_vnet_name and subnet_name == pe_subnet_name)):

            for private_link_service_connection in pe.private_link_service_connections:
                pe_service = private_link_service_connection.private_link_service_id.split("/")
                if pe_service[-2] == "storageAccounts" and pe_service[-1] == sa_name:

                    logger.info(f"Private Endpoint found for Storage Account '{sa_name}', Virtual Network '{pe_vnet_name}', Subnet '{pe_subnet_name}'")

                    pe_details["name"] = pe.name
                    pe_details["id"] = pe.id
                    pe_details["type"] = pe.type
                    pe_details["service"] = ("storageAccounts", sa_name)
                    pe_details["subnet"] = (pe_vnet_name, pe_subnet_name)

                    return pe_details

    logger.info(f"Private Endpoint not found for Storage Account '{sa_name}', Virtual Network '{pe_vnet_name}', Subnet '{pe_subnet_name}'")

    return pe_details