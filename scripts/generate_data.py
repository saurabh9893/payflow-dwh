import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker("en_IN")

random.seed(42)
Faker.seed(42)

RAW_PATH = "data/raw"
os.makedirs(RAW_PATH, exist_ok=True)


# ── Master Data ───

KYC_TIERS = ["BASIC", "FULL", "PREMIUM"]

CUSTOMER_SEGMENTS = ["NEW", "REGULAR", "VIP"]

PAYMENT_METHODS = ["UPI", "WALLET", "CARD", "NETBANKING"]

MERCHANT_CATEGORIES = [
    "FOOD", "RETAIL", "TRAVEL", 
    "UTILITY", "ENTERTAINMENT", "HEALTHCARE"
]

TRANSACTION_STATUSES = ["SUCCESS", "FAILED", "PENDING"]

FAILURE_REASONS = [
    "INSUFFICIENT_BALANCE",
    "BANK_DECLINED",
    "TIMEOUT",
    "INVALID_UPI_ID",
    None  
]

INDIAN_STATES = [
    "MH", "DL", "KA", "TN", "GJ", 
    "RJ", "UP", "WB", "TS", "KL"
]

#Customer Data
def generate_customers(n=10000):
    print(f"Generating {n} customers...")
    customers = []

    for i in range(n):
        customer_id = f"CUST-{str(i+1).zfill(6)}"  
        state = random.choice(INDIAN_STATES)
        kyc_tier = random.choice(KYC_TIERS)

        customer = {
            "customer_id":       customer_id,
            "full_name":         fake.name(),
            "email":             fake.email(),
            "phone":             fake.phone_number(),
            "city":              fake.city(),
            "state":             state,
            "kyc_tier":          kyc_tier,
            "wallet_limit":      random.choice([5000, 10000, 25000, 50000, 100000]),
            "customer_segment":  random.choice(CUSTOMER_SEGMENTS),
            "created_date":      fake.date_between(
                                     start_date="-5y",
                                     end_date="-6m"
                                 ),
        }
        customers.append(customer)

    df = pd.DataFrame(customers)
    df.to_csv(f"{RAW_PATH}/customers.csv", index=False)
    print(f"✅ customers.csv → {len(df)} rows")
    return df

#Merchants data
def generate_merchants(n=1000):
    print(f"Generating {n} merchants...")
    merchants = []

    for i in range(n):
        merchant_id = f"MERCH-{str(i+1).zfill(4)}"
        category = random.choice(MERCHANT_CATEGORIES)

        merchant = {
            "merchant_id":    merchant_id,
            "merchant_name":  fake.company(),
            "category":       category,
            "sub_category":   f"{category}_SUB_{random.randint(1,3)}",
            "city":           fake.city(),
            "state":          random.choice(INDIAN_STATES),
            "is_active":      random.choices([1, 0], weights=[95, 5])[0],
            "onboarded_date": fake.date_between(
                                  start_date="-4y",
                                  end_date="-3m"
                              ),
        }
        merchants.append(merchant)

    df = pd.DataFrame(merchants)
    df.to_csv(f"{RAW_PATH}/merchants.csv", index=False)
    print(f"✅ merchants.csv → {len(df)} rows")
    return df

#Payment Methods (fixed values)
def generate_payment_methods():
    print("Generating payment methods...")
    
    payment_methods = [
        {"method_id": "PM-001", "method_code": "UPI",        "method_name": "UPI Payment",       "method_type": "INSTANT",  "is_active": 1},
        {"method_id": "PM-002", "method_code": "WALLET",     "method_name": "Wallet Payment",     "method_type": "INSTANT",  "is_active": 1},
        {"method_id": "PM-003", "method_code": "CARD_DEBIT", "method_name": "Debit Card",         "method_type": "INSTANT",  "is_active": 1},
        {"method_id": "PM-004", "method_code": "CARD_CREDIT","method_name": "Credit Card",        "method_type": "DEFERRED", "is_active": 1},
        {"method_id": "PM-005", "method_code": "NETBANKING", "method_name": "Net Banking",        "method_type": "INSTANT",  "is_active": 1},
        {"method_id": "PM-006", "method_code": "UPI_LITE",   "method_name": "UPI Lite",           "method_type": "INSTANT",  "is_active": 1},
        {"method_id": "PM-007", "method_code": "BNPL",       "method_name": "Buy Now Pay Later",  "method_type": "DEFERRED", "is_active": 1},
        {"method_id": "PM-008", "method_code": "EMI",        "method_name": "EMI Payment",        "method_type": "DEFERRED", "is_active": 1},
        {"method_id": "PM-009", "method_code": "PREPAID",    "method_name": "Prepaid Card",       "method_type": "INSTANT",  "is_active": 1},
        {"method_id": "PM-010", "method_code": "CASH",       "method_name": "Cash on Delivery",   "method_type": "INSTANT",  "is_active": 0},
    ]

    df = pd.DataFrame(payment_methods)
    df.to_csv(f"{RAW_PATH}/payment_methods.csv", index=False)
    print(f"✅ payment_methods.csv → {len(df)} rows")
    return df

#Transactions Data
def generate_transactions(customers_df, merchants_df, n=500000):
    print(f"Generating {n} transactions...")
    transactions = []

    # Extract IDs from dataframes for random selection
    customer_ids = customers_df["customer_id"].tolist()
    merchant_ids = merchants_df["merchant_id"].tolist()
    method_codes = ["UPI", "WALLET", "CARD_DEBIT", 
                    "CARD_CREDIT", "NETBANKING", "UPI_LITE",
                    "BNPL", "EMI", "PREPAID"]

    for i in range(n):
        txn_id = f"TXN-{str(i+1).zfill(8)}"
        status = random.choices(
            TRANSACTION_STATUSES,
            weights=[85, 10, 5]  
        )[0]

        if status == "FAILED":
            failure_reason = random.choice(FAILURE_REASONS[:-1])
        else:
            failure_reason = None

        txn_amount = round(random.uniform(10, 50000), 2)
        
        if status == "SUCCESS":
            platform_fee    = round(txn_amount * random.uniform(0.001, 0.02), 2)
            cashback_amount = round(txn_amount * random.uniform(0, 0.05), 2)
        else:
            platform_fee    = 0.0
            cashback_amount = 0.0

        txn_date = fake.date_between(
            start_date="-2y",
            end_date="today"
        )

        transaction = {
            "transaction_id":   txn_id,
            "customer_id":      random.choice(customer_ids),
            "merchant_id":      random.choice(merchant_ids),
            "method_code":      random.choice(method_codes),
            "txn_date":         txn_date,
            "txn_amount":       txn_amount,
            "platform_fee":     platform_fee,
            "cashback_amount":  cashback_amount,
            "txn_status":       status,
            "failure_reason":   failure_reason,
            "is_fraud_flag":    random.choices([0, 1], weights=[98, 2])[0],
        }
        transactions.append(transaction)

        if (i + 1) % 100000 == 0:
            print(f"  {i+1:,} transactions generated...")

    df = pd.DataFrame(transactions)
    df.to_csv(f"{RAW_PATH}/transactions.csv", index=False)
    print(f"✅ transactions.csv → {len(df)} rows")
    return df

#Changed Customers Details to apply scd2
def generate_customer_updates(customers_df, update_ratio=0.15):
    print("Generating customer updates for SCD2...")
    
    n_updates = int(len(customers_df) * update_ratio)
    updated_customers = customers_df.sample(n=n_updates, random_state=42).copy()

    updated_customers["kyc_tier"] = updated_customers["kyc_tier"].apply(
        lambda current: random.choice(
            [t for t in KYC_TIERS if t != current]
        )
    )

    updated_customers["wallet_limit"] = updated_customers["wallet_limit"].apply(
        lambda current: random.choice(
            [w for w in [5000, 10000, 25000, 50000, 100000] if w != current]
        )
    )

    updated_customers["customer_segment"] = updated_customers["customer_segment"].apply(
        lambda current: random.choice(
            [s for s in CUSTOMER_SEGMENTS if s != current]
        )
    )

    updated_customers["updated_date"] = updated_customers["created_date"].apply(
        lambda created: fake.date_between(
            start_date=created,
            end_date="today"
        )
    )

    updated_customers.to_csv(f"{RAW_PATH}/customer_updates.csv", index=False)
    print(f"✅ customer_updates.csv → {len(updated_customers)} rows")
    return updated_customers

#Main function to call
def main():
    print("=" * 50)
    print("PayFlow DWH — Synthetic Data Generation")
    print("=" * 50)


    customers_df        = generate_customers(n=10000)
    merchants_df        = generate_merchants(n=1000)
    payment_methods_df  = generate_payment_methods()
    transactions_df     = generate_transactions(customers_df, merchants_df, n=500000)
    customer_updates_df = generate_customer_updates(customers_df)

    print("=" * 50)
    print("All files generated successfully!")
    print(f"Location: {RAW_PATH}")
    print("=" * 50)

if __name__ == "__main__":
    main()