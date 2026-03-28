"""
generate_sample_data.py
-----------------------
Generates small but realistic sample CSVs so you can test the app
without downloading the full 3.4M row Kaggle dataset.

Run: python generate_sample_data.py
Creates: ./data/ folder with all 6 CSV files
"""
import pandas as pd
import numpy as np
import os, random
from pathlib import Path

random.seed(42)
np.random.seed(42)

Path("data").mkdir(exist_ok=True)

# ── departments ────────────────────────────────────────────────────────────────
departments = pd.DataFrame({
    "department_id": range(1, 22),
    "department": [
        "frozen", "other", "bakery", "produce", "alcohol",
        "international", "beverages", "pets", "dry goods pasta", "bulk",
        "personal care", "meat seafood", "pantry", "breakfast", "canned goods",
        "dairy eggs", "household", "babies", "snacks", "deli", "missing"
    ]
})
departments.to_csv("data/departments.csv", index=False)
print(f"✅ departments.csv — {len(departments)} rows")

# ── aisles ────────────────────────────────────────────────────────────────────
aisle_names = [
    "prepared soups salads", "specialty cheeses", "energy granola bars",
    "instant foods", "marinades meat preparation", "other", "packaged produce",
    "dry pasta", "fresh vegetables", "fresh fruits", "packaged cheese",
    "crackers", "breakfast bakery", "cereals", "yogurt", "milk",
    "water seltzer sparkling water", "soy lactosefree", "juice nectars",
    "sparkling water", "organic produce", "nuts seeds dried fruit",
    "chips pretzels", "frozen meals", "ice cream ice", "frozen produce",
    "poultry counter", "fresh dips tapenades", "eggs", "butter",
    "lunch meat", "bread", "candy chocolate", "coffee", "tea",
    "baby food formula", "diapers wipes", "cleaning products", "dish detergents",
    "laundry", "paper goods", "trash bags liners", "personal care",
    "deodorants", "shampoo conditioner", "soap", "vitamins supplements",
    "cat food care", "dog food care", "beauty", "hair care",
    "oral hygiene", "feminine care", "frozen pizza", "frozen breakfast",
    "frozen appetizers sides", "frozen meat seafood", "condiments",
    "oils vinegars", "spices seasonings", "baking ingredients",
    "canned meals beans", "canned jarred vegetables", "soup broth bouillon",
    "canned fruit applesauce", "tomato sauce pasta sauce", "specialty wines champagnes",
    "beers coolers", "spirits", "wine", "packaged seafood",
    "seafood counter", "hot dogs bacon sausage", "kosher foods",
    "asian foods", "hispanic foods", "indian foods", "middle eastern foods",
    "european cheeses", "more household", "kitchen supplies", "office supplies",
    "baby bath body care", "baby accessories", "trail mix snack mix",
    "protein meal replacements", "juice", "sports drinks", "refrigerated",
    "tofu meat alternatives", "prepared meals", "salad dressing toppings",
    "pickled goods olives", "meat counter", "deli meats", "specialty deli",
    "bulk dried fruits vegetables", "bulk grains rice dried goods",
    "bulk bins", "fresh pasta", "specialty cheeses", "frozen vegan vegetarian",
    "frozen breads doughs", "ice cream toppings", "food storage",
    "first aid", "cold flu allergy", "digestion", "muscles joints pain relief",
    "frozen juice", "frozen dessert", "chips pretzels", "popcorn jerky",
    "tortillas flat bread", "hot cereals oatmeal", "granola",
    "pancake mixes", "waffles", "coffee pods", "k-cups pods",
    "tea", "soft drinks", "energy drinks", "vegetable juice",
    "tomato vegetable juice", "protein shakes meal replacement", "flowers",
    "mint gum", "gift wrap", "greeting cards", "magazines",
    "seasonal", "other creams cheeses", "cream cheese spreads",
    "lunch snacks", "buns rolls", "bagels muffins breakfast breads",
    "alternative milks", "refrigerated pudding desserts"
][:134]
while len(aisle_names) < 134:
    aisle_names.append(f"aisle_{len(aisle_names)+1}")

aisles = pd.DataFrame({
    "aisle_id": range(1, 135),
    "aisle": aisle_names
})
aisles.to_csv("data/aisles.csv", index=False)
print(f"✅ aisles.csv — {len(aisles)} rows")

# ── products ───────────────────────────────────────────────────────────────────
product_templates = [
    ("Organic", ["Bananas", "Strawberries", "Baby Spinach", "Whole Milk", "Eggs", "Avocado", "Blueberries"]),
    ("", ["Bag of Organic Bananas", "Sparkling Water", "Unsweetened Almond Milk", "Chicken Breast", "Greek Yogurt",
          "Cheddar Cheese", "Sourdough Bread", "Orange Juice", "Butter", "Pasta"]),
    ("", ["Apple Juice", "Beef Steak", "Salmon Fillet", "Frozen Pizza", "Ice Cream",
          "Potato Chips", "Salsa", "Tortilla Chips", "Beer 6-Pack", "Red Wine"]),
]

# Assign realistic aisle/dept combos
product_aisle_dept = {
    range(1, 51): (9, 4),    # produce-ish → produce dept
    range(51, 101): (16, 16), # dairy → dairy dept
    range(101, 151): (6, 3),  # bakery → bakery dept
    range(151, 201): (13, 13),# pantry → pantry dept
    range(201, 251): (24, 1), # frozen → frozen dept
    range(251, 301): (21, 7), # beverages → beverages dept
}

products_list = []
names_pool = (
    [f"Organic {item}" for item in ["Apple", "Banana", "Carrot", "Spinach", "Kale", "Strawberry",
                                     "Blueberry", "Avocado", "Tomato", "Broccoli"]] +
    [f"Fresh {item}" for item in ["Salmon", "Chicken Breast", "Ground Beef", "Tilapia", "Shrimp"]] +
    [f"Whole {item}" for item in ["Milk", "Grain Bread", "Wheat Pasta", "Bean Coffee"]] +
    ["Greek Yogurt", "Cheddar Cheese", "Butter", "Orange Juice", "Almond Milk",
     "Sparkling Water", "Potato Chips", "Salsa", "Frozen Pizza", "Ice Cream Vanilla",
     "Sourdough Bread", "Brown Rice", "Black Beans", "Olive Oil", "Sea Salt",
     "Baby Wipes", "Dish Soap", "Paper Towels", "Laundry Detergent", "Shampoo",
     "Dog Food Chicken", "Cat Food Tuna", "Baby Formula", "Diapers Size 3",
     "Red Wine Cabernet", "IPA Beer 6-Pack", "Sparkling Wine", "Vodka",
     "Granola Bar", "Trail Mix", "Protein Powder", "Energy Drink", "Coffee Pods",
     "Green Tea", "Chamomile Tea", "Tomato Sauce", "Chicken Broth", "Canned Tuna",
     "Peanut Butter", "Strawberry Jam", "Honey", "Maple Syrup", "Vanilla Extract"]
)

for i in range(1, 1001):
    base_name = names_pool[(i-1) % len(names_pool)]
    if i > len(names_pool):
        base_name = f"{base_name} #{(i // len(names_pool)) + 1}"
    
    dept_id = ((i-1) % 21) + 1
    aisle_id = ((i-1) % 134) + 1
    products_list.append({
        "product_id": i,
        "product_name": base_name,
        "aisle_id": aisle_id,
        "department_id": dept_id,
    })

products = pd.DataFrame(products_list)
products.to_csv("data/products.csv", index=False)
print(f"✅ products.csv — {len(products)} rows")

# ── orders ─────────────────────────────────────────────────────────────────────
N_USERS = 2000
N_ORDERS = 15000

orders_list = []
order_id = 1

for user_id in range(1, N_USERS + 1):
    n_orders = random.randint(2, 15)
    for order_num in range(1, n_orders + 1):
        eval_set = "prior" if order_num < n_orders else random.choice(["train", "test"])
        orders_list.append({
            "order_id": order_id,
            "user_id": user_id,
            "eval_set": eval_set,
            "order_number": order_num,
            "order_dow": random.randint(0, 6),
            "order_hour_of_day": int(np.random.normal(12, 3.5).clip(0, 23)),
            "days_since_prior_order": None if order_num == 1 else random.choice(
                [1, 2, 3, 4, 5, 6, 7, 7, 7, 14, 14, 21, 30]
            ),
        })
        order_id += 1
        if order_id > N_ORDERS + 1:
            break
    if order_id > N_ORDERS + 1:
        break

orders = pd.DataFrame(orders_list[:N_ORDERS])
orders.to_csv("data/orders.csv", index=False)
print(f"✅ orders.csv — {len(orders)} rows")

# ── order_products (prior + train) ─────────────────────────────────────────────
# Popular products (simulating power law distribution)
popular_products = list(range(1, 51))  # top 50 most popular
all_products = list(range(1, 1001))

def gen_order_products(order_ids, max_items=8):
    rows = []
    for oid in order_ids:
        n_items = random.randint(1, max_items)
        # 70% chance to pick from popular products
        chosen = random.choices(
            [random.choice(popular_products), random.choice(all_products)],
            weights=[0.7, 0.3],
            k=n_items
        )
        chosen = list(set(chosen))  # deduplicate
        for pos, pid in enumerate(chosen, 1):
            rows.append({
                "order_id": oid,
                "product_id": pid,
                "add_to_cart_order": pos,
                "reordered": 1 if random.random() < 0.6 else 0,
            })
    return pd.DataFrame(rows)

prior_order_ids = orders[orders["eval_set"] == "prior"]["order_id"].tolist()
train_order_ids = orders[orders["eval_set"] == "train"]["order_id"].tolist()

op_prior = gen_order_products(prior_order_ids)
op_prior.to_csv("data/order_products__prior.csv", index=False)
print(f"✅ order_products__prior.csv — {len(op_prior)} rows")

op_train = gen_order_products(train_order_ids)
op_train.to_csv("data/order_products__train.csv", index=False)
print(f"✅ order_products__train.csv — {len(op_train)} rows")

print("\n✅ All sample data generated in ./data/")
print("   Note: This is synthetic sample data (~15K orders).")
print("   For the real 3.4M row dataset, download from Kaggle.")
