TABLE_CONFIG = """
[
    {
        "table": "mfs_customer_d",
        "control_table": "load_data_ts_local",
        "schema": [
          { "mode": "NULLABLE",
            "name": "load_data_ts_local",
            "type": "TIMESTAMP"
          },
          { "mode": "NULLABLE",
            "name": "customer_id",
            "type": "INTEGER"
          },
          { "mode": "NULLABLE",
            "name": "customer_type",
            "type": "INTEGER"
          },
          { "mode": "NULLABLE",
            "name": "user_name",
            "type": "STRING"
          },
          { "mode": "NULLABLE",
            "name": "trust_level",
            "type": "INTEGER"
          },
          { "mode": "NULLABLE",
            "name": "notification_type",
            "type": "STRING"
          },
          { "mode": "NULLABLE",
            "name": "language_code",
            "type": "STRING"
          },
          { "mode": "NULLABLE",
            "name": "charge_profile_id",
            "type": "INTEGER"
          },
          { "mode": "NULLABLE",
            "name": "rule_profile_id",
            "type": "INTEGER"
          },
          { "mode": "NULLABLE",
            "name": "sp_id",
            "type": "INTEGER"
          },
          { "mode": "NULLABLE",
            "name": "owner_identity_type",
            "type": "STRING"
          },
          { "mode": "NULLABLE",
            "name": "owner_identity_id",
            "type": "INTEGER"
          },
          { "mode": "NULLABLE",
            "name": "active_time",
            "type": "INTEGER"
          },
          { "mode": "NULLABLE",
            "name": "status_change_time",
            "type": "INTEGER"
          },
          { "mode": "NULLABLE",
            "name": "create_oper_id",
            "type": "INTEGER"
          },
          { "mode": "NULLABLE",
            "name": "create_time",
            "type": "TIMESTAMP"
          },
          { "mode": "NULLABLE",
            "name": "modify_oper_id",
            "type": "INTEGER"
          },
          { "mode": "NULLABLE",
            "name": "modify_time",
            "type": "TIMESTAMP"
          },
          { "mode": "NULLABLE",
            "name": "status",
            "type": "STRING"
          },
          { "mode": "NULLABLE",
            "name": "status_change_reason",
            "type": "STRING"
          },
          { "mode": "NULLABLE",
            "name": "load_data_ts",
            "type": "TIMESTAMP"
          },
          { "mode": "NULLABLE",
            "name": "person_id",
            "type": "INTEGER"
          },
          { "mode": "NULLABLE",
            "name": "public_name",
            "type": "STRING"
          },
          { "mode": "NULLABLE",
            "name": "first_link_bank_account_time",
            "type": "TIMESTAMP"
          },
          { "mode": "NULLABLE",
            "name": "inviter_identity_id",
            "type": "INTEGER"
          },
          { "mode": "NULLABLE",
            "name": "inviter_identity_type",
            "type": "STRING"
          }
        ]
    }
]
"""