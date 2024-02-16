import grpc
import market_services_pb2
import market_services_pb2_grpc

import socket
from concurrent import futures


class NotificationServicer(market_services_pb2_grpc.NotifyClientServicer):
    def NotifyBuyer(self, request, context):
        print(f"Notification received: {request.message}")
        return market_services_pb2.Notification(message="Received")


class BuyerClient:
    def __init__(self, address):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        market_services_pb2_grpc.add_NotifyClientServicer_to_server(NotificationServicer(), self.server)
        port = self.server.add_insecure_port('0.0.0.0:0')
        self.server.start()
        print('Notification server started...')

        self.channel = grpc.insecure_channel(address)
        self.stub = market_services_pb2_grpc.MarketStub(self.channel)
        self.address = f'{self.get_ip()}:{port}'

    def get_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(('10.255.255.255', 1))
            IP = s.getsockname()[0]
        finally:
            s.close()
        return IP

    def search_item(self, name, category):
        request = market_services_pb2.SearchItemRequest(product_name=name, category=category)
        print("Search item response:")
        for item in self.stub.SearchItem(request):
            print(item)

    def buy_item(self, item_id, quantity):
        request = market_services_pb2.BuyItemRequest(item_id=item_id, quantity=quantity)
        response = self.stub.BuyItem(request)
        print("Buy item response:", response.message)

    def add_to_wishlist(self, item_id):
        buyer_address = self.address
        request = market_services_pb2.AddToWishListRequest(item_id=item_id, buyer_address=buyer_address)
        response = self.stub.AddToWishList(request)
        print("Add to wishlist response:", response.message)

    def rate_item(self, item_id, rating):
        buyer_address = self.address
        request = market_services_pb2.RateItemRequest(item_id=item_id, buyer_address=buyer_address, rating=rating)
        response = self.stub.RateItem(request)
        print("Rate item response:", response.message)

def main():
    buyer_client = BuyerClient('34.134.236.156:50051')
    while True:
        print("\nMenu:")
        print("1. Search Item")
        print("2. Buy Item")
        print("3. Add Item to Wishlist")
        print("4. Rate Item")
        print("5. Exit")
        choice = input("Enter your choice: ")

        if choice == '1':
            name = input("Enter item name to search (leave blank for no preference): ")
            print("Categories: 0 - Any, 1 - Electronics, 2 - Fashion, 3 - Others")
            category = int(input("Enter category: "))
            buyer_client.search_item(name, category)
        elif choice == '2':
            item_id = input("Enter item ID: ")
            quantity = int(input("Enter quantity: "))
            buyer_client.buy_item(item_id, quantity)
        elif choice == '3':
            item_id = input("Enter item ID to add to wishlist: ")
            buyer_client.add_to_wishlist(item_id)
        elif choice == '4':
            item_id = input("Enter item ID to rate: ")
            rating = int(input("Enter rating (1-5): "))
            buyer_client.rate_item(item_id, rating)
        elif choice == '5':
            print("Exiting program.")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == '__main__':
    main()

