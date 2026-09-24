class User:
    def __init__(self, username, password, email):
        self.username = username
        self.password = password
        self.email = email

class OrderStats:
    def __init__(self, valid=0, invalid=0):
        self.valid = valid
        self.invalid = invalid
class OrderCategoryStats:
    def __init__(self):
        self.categories = {}

    def add_category(self, category, Y=0, N=0, total_quantity=0):
        if category not in self.categories:
            self.categories[category] = {'Y': 0, 'N': 0, 'total_quantity': 0}
        self.categories[category]['Y'] += Y
        self.categories[category]['N'] += N
        self.categories[category]['total_quantity'] += total_quantity

class OrderNumberStats:
    def __init__(self):
        self.orders = {}

    def add_order(self, order_name, Y=0, N=0):
        if order_name not in self.orders:
            self.orders[order_name] = {'Y': 0, 'N': 0}
        self.orders[order_name]['Y'] += Y
        self.orders[order_name]['N'] += N