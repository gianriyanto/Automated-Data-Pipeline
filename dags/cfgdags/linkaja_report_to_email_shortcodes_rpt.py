from cfgdags.linkaja_report_to_email_shortcodes_sc import list_of_short_codes


def add_comma(this_index):
    if this_index != len(list_of_short_codes)-1:
        return ""","""
    else:
        return """"""


short_code_queries = []
index = 0
for itr in list_of_short_codes:
    short_code = list_of_short_codes[index]

    # Input Query Here
    short_code_query = """
                    {{
                    "query_name": "{}",
                    "query": "SELECT TRIM(order_id) ||'\\",\\"'||TRIM(INVOICE_ID) ||'\\",\\"'||TRIM(BUYER) ||'\\",\\"'||TRIM(TERMINAL_ID_MERCHANT) ||'\\",\\"'||TRIM(MERCHANT_NAME) ||'\\",\\"'||TRIM(TRANSACTION_TIME) ||'\\",\\"'||TRIM(TRANSACTION_STATUS) ||'\\",\\"'||TRIM(BISNIS_SEGMENT) ||'\\",\\"'||TRIM(BISNIS_SUBSEGMENT) ||'\\",\\"'||TRIM(TRANSACTION_AMOUNT) ||'\\",\\"'||TRIM(MDR) ||'\\",\\"'||TRIM(NET_AMOUNT) ||'\\",\\"'||TRIM(MERCHANTTRXID) FROM (select '\\"'||A.orderid as order_id, wco_refnum as invoice_id, case when instr(debit_party_mnemonic,'-') = 0 then debit_party_mnemonic else substr(debit_party_mnemonic,0, instr(debit_party_mnemonic,'-') -2) end  as buyer, short_code as terminal_id_merchant, biz_org_name as merchant_name, to_char(last_updated_time, 'YYYY-MM-DD hh24:mi:ss') as transaction_time, trans_status as transaction_status, bisnis_segment, bisnis_subsegment, request_amount/100 as transaction_amount, fee/100 as mdr, request_amount/100 - fee/100 as net_amount, merchant_trx_id||'\\"' as merchanttrxid from t_o_trans_record A, (select reference_value as merchant_trx_id, orderid from t_o_order_refdata where reference_key = 'transaction reference number') B, (select reference_value as wco_refnum, orderid from t_o_order_refdata where reference_key = 'WCORefNum') C, t_o_biz_org D, (select identityid, field_70 as bisnis_segment, field_28 as bisnis_subsegment from t_o_org_kyc) F where A.orderid = B.orderid and A.orderid =C.orderid and A.credit_party_id = D.biz_org_id and D.biz_org_id = F.identityid and short_code = '{}' and trans_initate_time BETWEEN TO_DATE('{{{{ yesterday_ds_nodash }}}} 22:00:00','YYYYMMDD hh24:mi:ss') AND TO_DATE('{{{{ ds_nodash }}}} 22:00:00','YYYYMMDD hh24:mi:ss')) xx "}}{}""".format(short_code, short_code, add_comma(index))

    short_code_queries.append(short_code_query)
    index += 1

REPORT_CONFIG = """
[
    {{
        "report_name": "dummy_shortcodes",
        "pic": [
            "gian_riyanto@linkaja.id"
        ],
        "recipient": [
            "gian_riyanto@linkaja.id"
        ],
        "queries": [
            {}
        ]
    }}
]
""".format(("".join(map(str, short_code_queries))))

print(REPORT_CONFIG)

