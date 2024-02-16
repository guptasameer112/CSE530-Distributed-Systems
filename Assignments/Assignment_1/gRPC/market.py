import grpc
from concurrent import futures
import time

import market_services_pb2
import market_services_pb2_grpc

from uuid import uuid4

class MarketServicer(market_services_pb2_grpc.MarketServicer):

    def __init__(self):
        self.sellerDict = {}  
        self.items = {}  
        self.wishlists = {}  
        self.ratings = {}

    def notify_seller(self, address, message):
        channel = grpc.insecure_channel(address)
        stub = market_services_pb2_grpc.NotifyClientStub(channel)
        stub.NotifySeller(market_services_pb2.Notification(message=message))
    
    def notify_buyer(self, address, message):
        channel = grpc.insecure_channel(address)
        stub = market_services_pb2_grpc.NotifyClientStub(channel)
        stub.NotifyBuyer(market_services_pb2.Notification(message=message))
    
    def RegisterSeller(self, request, context):
        print(f'Seller join request from {context.peer()}, uuid = {request.uuid}')
        if request.uuid in self.sellerDict:
            return market_services_pb2.Notification(message="FAILED")
        self.sellerDict[request.uuid] = {"address" : request.address, "items" : []}
        #notif
        return market_services_pb2.Notification(message="SUCCESS")
        
        

    def SellItem(self, request, context):
        print(f'Sell Item request from {context.peer()}')
        # print(request.seller_uuid)
        if (self.sellerDict.get(request.seller_uuid) == None):
            return market_services_pb2.SellItemResponse(message="FAILED")
        item = {
            "product_name" : request.product_name, 
            "category" : request.category,
            "quantity" : request.quantity,
            "description" : request.description,
            "price" : request.price,
            "seller_address" : request.seller_address,
            "rating" : 0.0
        }
        item_id = str(uuid4())
        self.items[item_id] = item
        self.sellerDict[request.seller_uuid]["items"].append(item_id)

        return market_services_pb2.SellItemResponse(message="SUCCESS", item_id = item_id)

    def UpdateItem(self, request, context):
        print(f'Update {request.item_id} request from {context.peer()}')

        if (self.sellerDict.get(request.seller_uuid) == None) or request.item_id not in self.items:
            return market_services_pb2.Notification(message="FAILED")
        if self.items[request.item_id]["seller_address"] != request.seller_address:
            return market_services_pb2.Notification(message="FAILED")
        
        self.items[request.item_id]["price"] = request.new_price
        self.items[request.item_id]["quantity"] = request.new_quantity

        # notification trigger
        for buyer_address in self.wishlists[request.item_id]:
            self.notify_buyer(buyer_address, f'Update made to item {request.item_id}')

        return market_services_pb2.Notification(message="SUCCESS")


    def DeleteItem(self, request, context):
        print(f'Delete {request.item_id} request from {context.peer()}')

        if (self.sellerDict.get(request.seller_uuid) == None) or request.item_id not in self.items:
            return market_services_pb2.Notification(message="FAILED")
        if self.items[request.item_id]["seller_address"] != request.seller_address:
            return market_services_pb2.Notification(message="FAILED")
        
        self.items.pop(request.item_id)
        self.sellerDict[request.seller_uuid]["items"].remove(request.item_id)

        return market_services_pb2.Notification(message="SUCCESS")




    def DisplaySellerItems(self, request, context):
        print(f'Display Items request from {context.peer()}')
        if (self.sellerDict.get(request.seller_uuid) == None):
            return market_services_pb2.DisplaySellerItemsResponse(message="FAILED")
        
        items = []

        for item_id in self.sellerDict[request.seller_uuid]["items"]:
            item = self.items[item_id]
            response_item = market_services_pb2.Item(
                    item_id = item_id,
                    price = item["price"],
                    product_name = item["product_name"],
                    category = item["category"],
                    description = item["description"],
                    quantity = item["quantity"],
                    rating = item["rating"],
                    seller_address = item["seller_address"],
                 )
            yield response_item
        

    def SearchItem(self, request, context):
        print(f'Search Request for item {request.product_name} and category {request.category}')

        filtered_items = [item_id for item_id in self.items if (self.items[item_id]["category"] == request.category or request.category == market_services_pb2.Category.ANY) and (request.product_name.lower() in self.items[item_id]["product_name"].lower() or not request.product_name)]

        for item_id in filtered_items:
            item = self.items[item_id]
            response_item = market_services_pb2.Item(
                    item_id = item_id,
                    price = item["price"],
                    product_name = item["product_name"],
                    category = item["category"],
                    description = item["description"],
                    quantity = item["quantity"],
                    rating = item["rating"],
                    seller_address = item["seller_address"],
                 )
            yield response_item
    
    def BuyItem(self, request, context):
        print(f'Buy request {request.quantity} of item {request.item_id}, from {context.peer()}')
        if request.item_id not in self.items or self.items[request.item_id]["quantity"] < request.quantity:
            return market_services_pb2.Notification(message="FAILED")
        self.items[request.item_id]["quantity"] -= request.quantity

        self.notify_seller(self.items[request.item_id]["seller_address"], f'Order for item {request.item_id}, Quantity = {request.quantity}')
        # notification trigger

        return market_services_pb2.Notification(message="SUCCESS")
    
    def AddToWishList(self, request, context):
        print(f'Wishlist request of {request.item_id}, from {context.peer()}')
        if request.item_id not in self.items:
            return market_services_pb2.Notification(message="FAILED")
        if request.item_id not in self.wishlists:
            self.wishlists[request.item_id] = []
        self.wishlists[request.item_id].append(request.buyer_address)
        return market_services_pb2.Notification(message="SUCCESS")

        

    def RateItem(self, request, context):
        if request.item_id not in self.items:
            return market_services_pb2.Notification(message="FAILED")
        
        if request.buyer_address in [rating[0] for rating in self.ratings.get(request.item_id, [])]:
            return market_services_pb2.Notification(message="FAILED")
        
        # Update the rating for the item
        if request.rating >= 1 and request.rating <= 5:
            if request.item_id not in self.ratings:
                self.ratings[request.item_id] = []
            self.ratings[request.item_id].append((request.buyer_address, request.rating))
            average_rating = sum([x[1] for x in self.ratings[request.item_id]]) / len(self.ratings[request.item_id])
            self.items[request.item_id]["rating"] = average_rating

            print(f'{context.peer()} rated item {request.item_id} with {request.rating} stars.')
            return market_services_pb2.Notification(message="SUCCESS")
        else:
            return market_services_pb2.Notification(message="FAILED")
        


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    market_services_pb2_grpc.add_MarketServicer_to_server(MarketServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    try:
        while True:
            time.sleep(10000)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
