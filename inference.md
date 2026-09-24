Now you are at the most useful part: **inference/scoring new transactions**.

Your production-style flow becomes:

```text
New transaction data
        ↓
Create the same ML features
        ↓
Load registered model
        ↓
Predict suspicious / normal
        ↓
Generate suspicious probability
        ↓
Store predictions
        ↓
Analyst / downstream system
```

The critical rule is: **new data must have the same feature columns the model was trained with**.

For your model, those were:

```text
txn_amount
is_cross_border
txn_hour
user_txn_count
merchant_txn_count
merchant_avg_amount
transactions_last_10_min
```

### Step 1: create some new transactions

For the demo, create a small new-data table:

```sql
%sql

CREATE OR REPLACE TABLE main.demo.new_transactions AS
SELECT * FROM VALUES

('NEW001','U2001','M001','Starbucks',
 TIMESTAMP('2026-09-24 10:00:00'),
 25.50,'USD','COMPLETED','PAYMENT',
 'US','US',0,'CARD','POS'),

('NEW002','U2002','M004','Tokyo Camera',
 TIMESTAMP('2026-09-24 10:01:00'),
 3200.00,'USD','COMPLETED','PAYMENT',
 'US','JP',1,'WALLET','MOBILE'),

('NEW003','U2002','M004','Tokyo Camera',
 TIMESTAMP('2026-09-24 10:02:00'),
 4100.00,'USD','COMPLETED','PAYMENT',
 'US','TW',1,'QR','MOBILE'),

('NEW004','U2002','M004','Tokyo Camera',
 TIMESTAMP('2026-09-24 10:03:00'),
 3600.00,'USD','COMPLETED','PAYMENT',
 'US','KR',1,'WALLET','MOBILE')

AS t(
    txn_id,
    user_id,
    merchant_id,
    merchant_name,
    txn_timestamp,
    txn_amount,
    txn_currency,
    txn_status,
    txn_type,
    from_country,
    to_country,
    is_cross_border,
    payment_method,
    device_type
);
```

You intentionally have:

```text
NEW001 = normal-looking transaction

NEW002-NEW004 =
high value
cross border
same user
same merchant
rapid sequence
```

### Step 2: create features for the new data

This is very important. The model cannot work directly from raw transactions. Build the same features:

```sql
%sql

CREATE OR REPLACE TABLE main.demo.new_transaction_features AS

SELECT
    n.txn_id,
    n.user_id,
    n.merchant_id,
    n.merchant_name,
    n.txn_timestamp,
    n.txn_amount,
    n.is_cross_border,

    HOUR(n.txn_timestamp) AS txn_hour,

    COUNT(*) OVER (
        PARTITION BY n.user_id
    ) AS user_txn_count,

    COUNT(*) OVER (
        PARTITION BY n.merchant_id
    ) AS merchant_txn_count,

    AVG(n.txn_amount) OVER (
        PARTITION BY n.merchant_id
    ) AS merchant_avg_amount,

    COUNT(*) OVER (
        PARTITION BY n.user_id
        ORDER BY CAST(n.txn_timestamp AS LONG)
        RANGE BETWEEN 600 PRECEDING AND CURRENT ROW
    ) AS transactions_last_10_min

FROM main.demo.new_transactions n;
```

Then validate:

```sql
%sql

SELECT *
FROM main.demo.new_transaction_features
ORDER BY txn_timestamp;
```

You should see the rapid sequence for `U2002` gradually become:

```text
1
2
3
```

for `transactions_last_10_min`.

### Step 3: load the registered model

Now use a Python cell.

Assuming your registered model is:

```text
main.demo.suspicious_transaction_model
```

use:

```python
import mlflow

model_uri = "models:/main.demo.suspicious_transaction_model/1"

loaded_model = mlflow.pyfunc.load_model(
    model_uri
)

print("Model loaded successfully.")
```

If you have more than one version, replace `/1` with the correct version.

A cleaner Unity Catalog pattern is often to use an alias such as:

```text
@Champion
```

For example:

```python
model_uri = "models:/main.demo.suspicious_transaction_model@Champion"
```

But for your first demo, version `1` is easiest.

### Step 4: prepare new feature rows

```python
feature_columns = [
    "txn_amount",
    "is_cross_border",
    "txn_hour",
    "user_txn_count",
    "merchant_txn_count",
    "merchant_avg_amount",
    "transactions_last_10_min"
]

new_df = spark.table(
    "main.demo.new_transaction_features"
)

new_pdf = new_df.toPandas()
```

### Step 5: predict

Because you registered the model using MLflow, run:

```python
new_pdf["predicted_label"] = loaded_model.predict(
    new_pdf[feature_columns]
)
```

Then:

```python
display(
    new_pdf[
        [
            "txn_id",
            "user_id",
            "merchant_name",
            "txn_amount",
            "is_cross_border",
            "transactions_last_10_min",
            "predicted_label"
        ]
    ]
)
```

You may see something conceptually like:

```text
NEW001   Starbucks      25.50     0   1   0

NEW002   Tokyo Camera   3200.00   1   1   1
NEW003   Tokyo Camera   4100.00   1   2   1
NEW004   Tokyo Camera   3600.00   1   3   1
```

So:

```text
0 = normal
1 = suspicious
```

### Step 6: probability

There is one detail here: if you load the model as a generic `pyfunc`, you'll normally get the model's configured prediction output, but not necessarily `predict_proba()` directly.

For your demo, load the sklearn model flavor instead:

```python
import mlflow.sklearn

sk_model = mlflow.sklearn.load_model(
    model_uri
)
```

Then:

```python
new_pdf["predicted_label"] = sk_model.predict(
    new_pdf[feature_columns]
)

new_pdf["suspicious_probability"] = sk_model.predict_proba(
    new_pdf[feature_columns]
)[:, 1]
```

Now:

```python
display(
    new_pdf[
        [
            "txn_id",
            "user_id",
            "merchant_name",
            "txn_amount",
            "transactions_last_10_min",
            "predicted_label",
            "suspicious_probability"
        ]
    ].sort_values(
        "suspicious_probability",
        ascending=False
    )
)
```

This is probably the best screen for your next demo.

### Step 7: save the results

```python
prediction_df = spark.createDataFrame(
    new_pdf
)

prediction_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "main.demo.new_transaction_predictions"
    )
```

Then query it:

```sql
%sql

SELECT
    txn_id,
    user_id,
    merchant_name,
    txn_timestamp,
    txn_amount,
    predicted_label,
    suspicious_probability

FROM main.demo.new_transaction_predictions

ORDER BY suspicious_probability DESC;
```

Now your complete architecture is:

```text
Historical transaction data
        ↓
Feature engineering
        ↓
Train Random Forest
        ↓
MLflow
        ↓
Unity Catalog model
        ↓
                NEW DATA
                   ↓
        new_transactions
                   ↓
         feature engineering
                   ↓
       registered ML model
                   ↓
        prediction + probability
                   ↓
 new_transaction_predictions
                   ↓
        analyst / alerting
```

For a production system, I would change one major thing from this demo: **do not calculate features using only the new transaction batch**. Features like `user_txn_count`, merchant average, and 10-minute velocity should use historical transaction data plus the arriving transaction. That is the next important step if you want this demo to look closer to a real fraud/suspicious-transaction architecture.
