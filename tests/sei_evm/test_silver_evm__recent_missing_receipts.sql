-- depends_on: {{ ref('test_silver_evm__transactions_recent') }}
{{ recent_missing_txs(ref("test_silver_evm__receipts_recent")) }}
