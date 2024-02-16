import grpc
import market_services_pb2
import market_services_pb2_grpc
from uuid import uuid1
import socket

from concurrent import futures

class NotificationServicer(market_services_pb2_grpc.NotifyClientServicer):
    def NotifySeller(self, request, context):
        print(f"Notification received: {request.message}")
        return market_services_pb2.Notification(message="Received")

class SellerClient:
    def __init__(self, address):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        market_services_pb2_grpc.add_NotifyClientServicer_to_server(NotificationServicer(), self.server)
        port = self.server.add_insecure_port('0.0.0.0:0')
        self.server.start()
        print('Notification server started...')

        self.channel = grpc.insecure_channel(address)
        self.stub = market_services_pb2_grpc.MarketStub(self.channel)
        self.uuid = str(uuid1())
        self.address = f'{self.get_ip()}:{port}'

    def get_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(('10.255.255.255', 1))
            IP = s.getsockname()[0]
        finally:
            s.close()
        return IP

    def register_seller(self):
        request = market_services_pb2.SellerRegistrationRequest(address=self.address, uuid=self.uuid)
        response = self.stub.RegisterSeller(request)
        print("Seller registration response:", response.message)

    def sell_item(self, product_name, category, quantity, description, price):
        request = market_services_pb2.SellItemRequest(
            seller_uuid=self.uuid,
            product_name=product_name,
            category=category,
            quantity=quantity,
            description=description,
            price=price,
            seller_address=self.address
        )
        response = self.stub.SellItem(request)
        print("Sell item response:", response.message)
        return response.item_id

    def update_item(self, item_id, new_price, new_quantity):
        request = market_services_pb2.UpdateItemRequest(
            seller_uuid=self.uuid,
            item_id=item_id,
            new_price=new_price,
            new_quantity=new_quantity,
            seller_address=self.address,  
        )
        response = self.stub.UpdateItem(request)
        print("Update item response:", response.message)

    def delete_item(self, item_id):
        request = market_services_pb2.DeleteItemRequest(
            seller_uuid=self.uuid,
            item_id=item_id,
            seller_address=self.address,  
        )
        response = self.stub.DeleteItem(request)
        print("Delete item response:", response.message)

    def display_seller_items(self):
        request = market_services_pb2.DisplaySellerItemsRequest(seller_uuid=self.uuid)
        for item in self.stub.DisplaySellerItems(request):
            print(item)

def main():
    seller_client = SellerClient('34.134.236.156:50051')
    while True:
        print("\nMenu:")
        print("1. Register Seller")
        print("2. Sell Item")
        print("3. Update Item")
        print("4. Delete Item")
        print("5. Display Seller Items")
        print("6. Exit")
        choice = input("Enter your choice: ")

        if choice == '1':
            # seller_address = input("Enter seller address: ")
            seller_client.register_seller()
        elif choice == '2':
            product_name = input("Enter product name: ")
            category = int(input("Enter category (1 for Electronics, 2 for Fashion, etc.): "))
            quantity = int(input("Enter quantity: "))
            description = input("Enter description: ")
            price = float(input("Enter price: "))
            # seller_address = input("Enter seller address: ")
            seller_client.sell_item(product_name, category, quantity, description, price)
        elif choice == '3':
            item_id = input("Enter item ID: ")
            new_price = float(input("Enter new price: "))
            new_quantity = int(input("Enter new quantity: "))
            seller_client.update_item(item_id, new_price, new_quantity)
        elif choice == '4':
            item_id = input("Enter item ID to delete: ")
            seller_client.delete_item(item_id)
        elif choice == '5':
            seller_client.display_seller_items()
        elif choice == '6':
            print("Exiting program.")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == '__main__':
    main()

