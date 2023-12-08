import query_runner as qr
import datetime as DT

comparison_start_date = '2023-06-01'
comparison_end_date = DT.date.today()

start_date =  DT.date.today() - DT.timedelta(days=1)
end_date = DT.date.today()

fields = "email,order_id,order_status,retry_process_response,completed_at, is_enrolled_nv, is_enrolled_vendor, is_device_eligible, denied_reason, created_at"

qr.run_comparison(fields,start_date,end_date,comparison_start_date,comparison_end_date)