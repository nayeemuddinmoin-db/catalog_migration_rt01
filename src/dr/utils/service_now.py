import requests
import logging
from typing import Dict

class ServiceNow:

    def __init__(self, instance, username, password):
        self._instance = instance
        self._username = username
        self._password = password

        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(level=logging.DEBUG)  # Prevents changing other log levels
    

    def create_order(self, order_details: Dict[str, str]):
        """
        Create a new order in ServiceNow
        """

        item_sys_id = "sys_id_of_the_item_to_order"
        quantity = 1  # Quantity of the item to add to the cart

        # Step 1: Add an item to the cart
        add_to_cart_url = f"https://{self._instance}.service-now.com/api/sn_sc/servicecatalog/items/{item_sys_id}/add_to_cart"
        add_to_cart_payload = {
            "sysparm_quantity": quantity,
            "variables": {
                        # "g_requested_for":"7ce91ebb83cb3110d5cb1920ceaad3a3",
                        # "g_location":"8e5d2c0bdb733b0478dba57305961909",
                        # "g_requestor_manager":"352e4ef087d079505af262873cbb3575",
                        # "g_email":"navy@test",
                        # "g_desc":"test",
                        # "g_req_type:":"Other", 	
                        # "g_business_service":"e9939bd51b7a68105f7bcbf2604bcbf7",
                        # "g_assignment_group":"21517c73db4a48d078dba5730596190b",
                        # "u_gr_req_cat":"df774ce50f511700fa1b1b1e51050e98",
                        # "u_gr_req_subcat":"4378c4e90f511700fa1b1b1e51050eeb"
                }
        }
        add_to_cart_response = requests.post(add_to_cart_url, auth=(self._username, self._password), json=add_to_cart_payload)

        if add_to_cart_response.status_code == 200:
            self._logger.info("Item added to cart successfully.")
        else:
            self._logger.error("Failed to add item to cart.")
            return

        # Step 2: Submit the cart to create an order
        submit_order_url = f"https://{self._instance}.service-now.com/api/sn_sc/servicecatalog/cart/submit_order"
        submit_order_response = requests.post(submit_order_url, auth=(self._username, self._password))

        if submit_order_response.status_code == 200:
            self._logger.info("Order created successfully.")
            order_details = submit_order_response.json()
            self._logger.info(f"Order Request Number: {order_details.get('result').get('request_number')}")
        else:
            self._logger.error("Failed to create order.")

        return