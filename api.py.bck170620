import falcon, sys
from falcon_multipart.middleware import MultipartMiddleware
from falcon_cors import CORS
from falcon_prometheus import PrometheusMiddleware

cors = CORS(allow_all_origins=True) #CORS(allow_origins_list=['http://test.com:8080'])
prometheus = PrometheusMiddleware()

sys.path.append("/home/centos/mint/apis/")

from ml_login import LoginResource as MlLoginResource
from ml_login import ForceLoginResource as MlForceLoginResource
from ml_logout import LogoutResource as MlLogoutResource
from ml_search_user import SearchUserResource as MlSearchUserResource
from ml_client_registration import ClientRegisterResource as MlClientRegisterResource
from ml_client_registration_v2 import ClientRegisterResource as MlClientRegisterResourceV2
from ml_dashboard import DashboardResource as MlDashboardResource
from ml_dashboard_verification_team import DashboardVerificationTeamResource as MlDashboardVerificationTeamResource
from ml_dashboard_backoffice import DashboardBackofficeResource as MlDashboardBackofficeResource
from ml_dashboard_outcall_team import DashboardOutcallResource as MlDashboardOutcallResource
from ml_dashboard_outcall_team_v2 import DashboardOutcallResource as MlDashboardOutcallResourceV2
from ml_dashboard_investment import DashboardInvestmentTeamResource as MlDashboardInvestmentTeamResource
#from ml_loan_application_request import LoanApplicationRequestResource as MlLoanApplicationRequestResource
from ml_loan_application_request_v2 import LoanApplicationRequestResource as MlLoanApplicationRequestResourceV2
from ml_get_loan_repayment_schedule import GetLoanRepaymentScheduleResource as MlGetLoanRepaymentScheduleResource
from ml_customer_details import CustDetailsResource as MlCustDetailsResource
from ml_get_cust_stages import GetCustStagesResource as MlGetCustStagesResource
from ml_get_available_cities import GetAvailableCitiesResource as MlGetAvailableCitiesResource
from ml_get_document_types import GetDocumentTypesResource as MlGetDocumentTypesResource
from ml_get_customer_income import CustIncomeDetailsResource as MlGetCustomerIncomeDetailsResource
from ml_get_customer_details import CustDetailsResource as MlGetCustomerDetailsResource
from ml_get_customer_extended_details import CustExtendedDetailsResource as MlCustExtendedDetailsResource
from ml_get_customer_documents import GetCustDocumentsResource as MlGetCustomerDocumentsResource
from ml_get_cust_bank_details import CustBankDetailsResource as MlGetCustBankDetailsResource
from ml_get_credit_eval_info import CreditEvalInfoResource as MlGetCreditEvalInfoResource
from ml_get_resolution_list import GetInteractionResolutionsResource as MlGetInteractionResolutionsResource
from ml_get_unidentified_uber_data import GetUnidentifiedUberDataResource as MlGetUnidentifiedUberDataResource
from ml_get_agreement_list import GetAgreementListResource as MlGetAgreementListResource
from ml_s3_image_dump import S3ReadResource as MlS3ReadResource
from ml_s3_image_upload import S3UploadResource as MlS3UploadResource
from ml_client_update import ClientUpdateResource as MlClientUpdateResource
from ml_app_activity_logs import AppActivityLogsResource as MlAppActivityLogsResource
from ml_list_loans import ListLoansResource as MlListLoansResource
from ml_list_repayments import ListRepaymentsResource as MlListRepaymentsResource
from ml_generate_payment_disbursal_report import PaymentDisbursalReportResource as MlPaymentDisbursalReportResource
from ml_generate_cust_csv import CustReportResource as MlCustReportResource
from ml_generate_mandate_payment_order import MandatePaymentOrderResource as MlMandatePaymentOrderResource
from ml_generate_uber_payment_order import UberPaymentOrderResource as MlUberPaymentOrderResource
from ml_aadhar_kyc_fill import AadharFillResource as MlAadharFillResource
from ml_create_task import CreateTaskResource as MlCreateTaskResource
from ml_create_call_info import CreateCallInfoResource as MlCreateCallInfoResource
from ml_show_task import ShowTaskResource as MlShowTaskResource
from ml_create_repayment_info import CreateRepaymentInfoResource as MlCreateRepaymentInfoResource
from ml_show_repay_info import ShowRepayInfoResource as MlShowRepayInfoResource
##from ml_show_sms_extract import ShowSMSExtractResource as MlShowSMSExtractResource
##from ml_sms_read import SmsReadResource as MlSmsReadResource
##from ml_contact_read import ContactReadResource as MlContactReadResource
from ml_uber_income_data_upload import UberIncomeUploadResource as MlUberIncomeUploadResource
from ml_mandate_data_upload import MandateDataUploadResource as MlMandateDataUploadResource
from ml_show_mandate_data import ShowMandateDataResource as MlShowMandateDataResource
from ml_update_repay_info import UpdateRepayInfoResource as MlUpdateRepayInfoResource
from ml_update_loan_status import UpdateLoanStatusResource as MlUpdateLoanStatusResource
from ml_store_customer_data import StoreCustomerDataResource as MlStoreCustomerDataResource
from ml_send_sms import SendSmsResource as MlSendSmsResource
from ml_show_sms_templates import ShowSmsTemplateResource as MlShowSmsTemplateResource
from ml_payment_disbursal_report_upload import PaymentDisbursalUploadResource as MlPaymentDisbursalUploadResource
from ml_insert_mandate_payment_info import MandatePaymentsUploadResource as MlMandatePaymentsUploadResource
from ml_insert_uber_repay_info import UberPaymentsUploadResource as MlUberPaymentsUploadResource
from ml_reject_loan import RejectLoanResource as MlRejectLoanResource
##from ml_upgrade_loan_limit_list import UpgradeLoanLimitListResource as MlUpgradeLoanLimitListResource
from ml_get_first_loan_limit import GetUpfrontLoanLimitResource as MlGetUpfrontLoanLimitResource
from ml_approve_loan import ApproveLoanResource as MlApproveLoanResource
from ml_disburse_loan import DisburseLoanResource as MlDisburseLoanResource
from ml_insert_finflux_repayment import RepaymentsBulkUploadResource as MlRepaymentsBulkUploadResource
from ml_get_available_products import GetAvailableProductsResource as MlGetAvailableProductsResource
from ml_get_available_products_v2 import GetAvailableProductsResource as MlGetAvailableProductsResourceV2
from ml_show_finflux_insert_log import ShowFinfluxInsertLogResource as MlShowFinfluxInsertLogResource
from ml_process_webhook import ProcessWebhookResource as MlProcessWebhookResource
from ml_get_customer_investments import GetCustInvestmentsResource as MlGetCustInvestmentsResource
from ml_get_standard_query_list import GetStandardQueryListResource as MlGetStandardQueryListResource
from ml_run_standard_query import RunStandardQueryResource as MlRunStandardQueryResource
from ml_split_repayments import SplitRepaymentsResource as MlSplitRepaymentsResource
from ml_insert_trans_id import InsertTransIDResource as MlInsertTransIDResource
from ml_get_company_city_product_details import GetCompanyCityProductDetailsResource as MlGetCompanyCityProductDetailsResource
from ml_get_bank_ifsc_details import GetBankIfscDetailsResource as MlGetBankIfscDetailsResource
from ml_insert_bank_ifsc_details import InsertBankIfscDetailsResource as MlInsertBankIfscDetailsResource
from ml_get_cust_audit_trail import GetCustAuditTrailResource as MlGetCustAuditTrailResource
from ml_get_uber_auth import GetUberAuthResource as MlGetUberAuthResource
from ml_reliance_offline_purchase import RelianceOfflinePurchaseResource as MlRelianceOfflinePurchaseResource
from ml_generate_refund_report import RefundReportResource as MlGenerateRefundReportResource
from ml_stage_sync import StageSyncResource as MlStageSyncResource
from ml_finflux_auth_update import FinfluxOauthUpdateResource as MlFinfluxOauthUpdateResource
#from ml_get_poonawalla_customer_kyc_documents import GetCustDocumentsResource as MlGetCustDocumentsResource
from ml_bulk_loan_disbursement import BulkLoanDisbursementUploadResource as MlBulkLoanDisbursementUploadResource
from ml_send_bulk_sms import SendBulkSmsUploadResource as MlSendBulkSmsUploadResource
from ml_send_obd_calls import SendBulkObdCallsUploadResource as MlSendBulkObdCallsUploadResource
from ml_swiggy_weekly_deduction_upload import SwiggyWeeklyDeductionUploadResource as MlSwiggyWeeklyDeductionUploadResource
from ml_swiggy_reliance_transaction_feed_upload import SwiggyRelianceTransactionFeedUploadResource as MlSwiggyRelianceTransactionFeedUploadResource
from ml_poonawalla_disbursal_report_upload import PoonawallaDisbursalUploadResource as MlPoonawallaDisbursalUploadResource
from ml_reject_uber_login import RejectUberLoginResource as MlRejectUberLoginResource
from ml_dashboard_poonawalla import DashboardPoonawallaTeamResource as MlDashboardPoonawallaTeamResource
from ml_push_loans_to_lender import PushLoanToLenderResource as MlPushLoanToLenderResource
from ml_set_pushtoken import SetPushtokenResource as MlSetPushtokenResource
from ml_loans_upload_for_update import UpdateLoansResource as MlUpdateLoansResource
from ml_map_uber_auth import MapUberAuthResource as MlMapUberAuthResource
from ml_swiggy_income_data_upload import SwiggyIncomeUploadResource as MlSwiggyIncomeUploadResource
from ml_bulk_customer_create import BulkCustomerCreateResource as MlBulkCustomerCreateResource
#from ml_donation_disbursal_report_upload import DonationDisbursalUploadResource as MlDonationDisbursalUploadResource
#from ml_create_customer_donation_tag import CreateCustomerDonationTagResource as MlCreateCustomerDonationTagResource
#from ml_view_donation_data import ViewDonationDataResource as MlViewDonationDataResource
from ml_bulk_reschedule import BulkRescheduleResource as MlBulkRescheduleResource


app = falcon.API(middleware=[MultipartMiddleware(), cors.middleware, prometheus])

##reco = RR() #RecoResource()
##reco_taral = RR()#_taral()
##reco_debug = RRD()
##devicePrice = devpr()

##pincode = PincodeResource()

##capture_event = IER() #InsertEventResource()
##capture_event_taral = IER()#_taral()

##ping = PR()

##riskProfiling = RiskProfilingResource()
##mlSmsRead = MlSmsReadResource()
##callRead = CallReadResource()
##mlContactRead = MlContactReadResource()


mlLogin = MlLoginResource()
mlForceLogin = MlForceLoginResource()
mlLogout = MlLogoutResource()
mlSearchUser = MlSearchUserResource()
mlClientRegister = MlClientRegisterResource()
mlClientRegisterV2 = MlClientRegisterResourceV2()
mlDashboard = MlDashboardResource()
mlDashboardForVerification = MlDashboardVerificationTeamResource()
mlDashboardBackoffice = MlDashboardBackofficeResource()
mlDashboardOutcall = MlDashboardOutcallResource()
mlDashboardOutcallV2 = MlDashboardOutcallResourceV2()
mlDashboardInvestment = MlDashboardInvestmentTeamResource()
#mlLoanApplicationRequest = MlLoanApplicationRequestResource()
mlLoanApplicationRequestV2 = MlLoanApplicationRequestResourceV2()
mlGetLoanRepaymentSchedule = MlGetLoanRepaymentScheduleResource()
mlCustomerDetails = MlCustDetailsResource()
mlGetCustStages = MlGetCustStagesResource()
mlGetAvailableCities = MlGetAvailableCitiesResource()
mlGetDocumentTypes = MlGetDocumentTypesResource()
mlGetCustomerDetails = MlGetCustomerDetailsResource()
mlGetCustomerExtendedDetails = MlCustExtendedDetailsResource()
mlGetCustomerDocuments = MlGetCustomerDocumentsResource()
mlGetCustomerIncomeDetails = MlGetCustomerIncomeDetailsResource()
mlGetCustBankDetails = MlGetCustBankDetailsResource()
mlGetCreditEvalInfo = MlGetCreditEvalInfoResource()
mlGetInteractionResolutions = MlGetInteractionResolutionsResource()
mlGetUnidentifiedUberData = MlGetUnidentifiedUberDataResource()
mlGetAgreementList = MlGetAgreementListResource()
mls3read = MlS3ReadResource()
mls3upload = MlS3UploadResource()
mlClientUpdate = MlClientUpdateResource()
mlAppActivityLogs = MlAppActivityLogsResource()
mlListLoans = MlListLoansResource()
mlListRepayments = MlListRepaymentsResource()
mlPaymentDisbursalReport = MlPaymentDisbursalReportResource()
mlMandatePaymentOrder = MlMandatePaymentOrderResource()
mlUberPaymentOrder = MlUberPaymentOrderResource()
mlCustReport = MlCustReportResource()
mlAadharFill = MlAadharFillResource()
mlCreateTask = MlCreateTaskResource()
mlShowTask = MlShowTaskResource()
mlCreateCallInfo = MlCreateCallInfoResource()
mlCreateRepaymentInfo = MlCreateRepaymentInfoResource()
mlShowRepaymentInfo = MlShowRepayInfoResource()
#mlShowSms = MlShowSMSExtractResource()
mlUberIncomeUpload = MlUberIncomeUploadResource()
mlMandateDataUpload = MlMandateDataUploadResource()
mlShowMandateData = MlShowMandateDataResource()
mlUpdateRepayInfo = MlUpdateRepayInfoResource()
mlUpdateLoanStatus = MlUpdateLoanStatusResource()
mlStoreCustomerData = MlStoreCustomerDataResource()
mlSendSms = MlSendSmsResource()
mlShowSmsTemplate = MlShowSmsTemplateResource()
mlPaymentDisbursalUpload = MlPaymentDisbursalUploadResource()
mlMandatePaymentsUpload = MlMandatePaymentsUploadResource()
mlUberPaymentsUpload = MlUberPaymentsUploadResource()
mlRejectLoan = MlRejectLoanResource()
#mlUpgradeLoanLimitList = MlUpgradeLoanLimitListResource()
mlGetUpfrontLoanLimit = MlGetUpfrontLoanLimitResource()
mlApproveLoan = MlApproveLoanResource()
mlDisburseLoan = MlDisburseLoanResource()
mlInsertFinfluxRepayment = MlRepaymentsBulkUploadResource()
mlGetAvailableProducts = MlGetAvailableProductsResource()
mlGetAvailableProductsV2 = MlGetAvailableProductsResourceV2()
mlShowFinfluxInsertLog = MlShowFinfluxInsertLogResource()
mlProcessWebhook = MlProcessWebhookResource()
mlGetCustInvestments = MlGetCustInvestmentsResource()
mlGetStandardQueryList = MlGetStandardQueryListResource()
mlRunStandardQuery = MlRunStandardQueryResource()
mlSplitRepayments = MlSplitRepaymentsResource()
mlInsertTransID = MlInsertTransIDResource()
mlGetCompanyCityProductDetails = MlGetCompanyCityProductDetailsResource()
mlInsertBankIfscDetails = MlInsertBankIfscDetailsResource()
mlGetBankIfscDetails = MlGetBankIfscDetailsResource()
mlGetCustAuditTrail = MlGetCustAuditTrailResource()
mlGetUberAuth = MlGetUberAuthResource()
mlRelianceOfflinePurchase = MlRelianceOfflinePurchaseResource()
mlGenerateRefundReport = MlGenerateRefundReportResource()
mlStageSync = MlStageSyncResource()
mlFinfluxOauthUpdate = MlFinfluxOauthUpdateResource()
#mlGetCustKycDocuments = MlGetCustDocumentsResource()
mlBulkLoanDisbursementUpload = MlBulkLoanDisbursementUploadResource()
mlSendBulkSmsUpload = MlSendBulkSmsUploadResource()
mlSendBulkObdCallsUpload = MlSendBulkObdCallsUploadResource()
mlSwiggyWeeklyDeductionUpload = MlSwiggyWeeklyDeductionUploadResource()
mlSwiggyRelianceTransactionFeedUpload = MlSwiggyRelianceTransactionFeedUploadResource()
mlPoonawallaDisbursalUpload = MlPoonawallaDisbursalUploadResource()
mlRejectUberLogin = MlRejectUberLoginResource()
mlDashboardPoonawallaTeam = MlDashboardPoonawallaTeamResource()
mlPushLoanToLender = MlPushLoanToLenderResource()
mlSetPushtoken = MlSetPushtokenResource()
mlUpdateLoans = MlUpdateLoansResource()
mlMapUberAuth = MlMapUberAuthResource()
mlSwiggyIncomeUpload = MlSwiggyIncomeUploadResource()
mlBulkCustomerCreate = MlBulkCustomerCreateResource()
##mlDonationDisbursalUpload = MlDonationDisbursalUploadResource()
##mlCreateCustomerDonationTag = MlCreateCustomerDonationTagResource()
##mlViewDonationData = MlViewDonationDataResource()
mlBulkReschedule = MlBulkRescheduleResource()

app.add_route('/metrics', prometheus)

##app.add_route('/capture_event', capture_event_taral)
##app.add_route('/capture_event2', capture_event)

##app.add_route('/recoengine_ping', ping)
##app.add_route('/getreco', reco_taral)
##app.add_route('/getreco2', reco)
##app.add_route('/getreco_debug',reco_debug)
##app.add_route('/getDevicePrice', devicePrice)

##app.add_route('/getpincode',pincode)

##app.add_route('/riskProfiling', riskProfiling)
##app.add_route('/smsRead', mlSmsRead)
##app.add_route('/callRead', callRead)
##app.add_route('/contactRead', mlContactRead)


app.add_route('/mlLogin', mlLogin)
app.add_route('/mlForceLogin', mlForceLogin)
app.add_route('/mlLogout', mlLogout)
app.add_route('/mlSearchUser', mlSearchUser)
app.add_route('/mlClientRegister', mlClientRegister)
app.add_route('/mlClientRegisterV2', mlClientRegisterV2)
#app.add_route('/mlLoanApplicationRequest', mlLoanApplicationRequest)
app.add_route('/mlLoanApplicationRequestV2', mlLoanApplicationRequestV2)
app.add_route('/mlGetLoanRepaymentSchedule', mlGetLoanRepaymentSchedule)
app.add_route('/mlDashboard', mlDashboard)
app.add_route('/mlDashboardForVerification', mlDashboardForVerification)
app.add_route('/mlDashboardBackoffice', mlDashboardBackoffice)
app.add_route('/mlDashboardOutcall', mlDashboardOutcall)
app.add_route('/mlDashboardOutcallV2', mlDashboardOutcallV2)
app.add_route('/mlDashboardInvestment', mlDashboardInvestment)
app.add_route('/mlCustomerDetails', mlCustomerDetails)
app.add_route('/mlGetCustomerExtendedDetails', mlGetCustomerExtendedDetails)
app.add_route('/mlGetCustStages', mlGetCustStages)
app.add_route('/mlGetAvailableCities', mlGetAvailableCities)
app.add_route('/mlGetDocumentTypes', mlGetDocumentTypes)
app.add_route('/mlGetCustomerDetails', mlGetCustomerDetails)
app.add_route('/mlGetCustomerDocuments', mlGetCustomerDocuments)
app.add_route('/mlGetCustIncomeDetails', mlGetCustomerIncomeDetails)
app.add_route('/mlGetCustBankDetails', mlGetCustBankDetails)
app.add_route('/mlGetCreditEvalInfo', mlGetCreditEvalInfo)
app.add_route('/mlGetInteractionResolutions', mlGetInteractionResolutions)
app.add_route('/mlGetUnidentifiedUberData', mlGetUnidentifiedUberData)
app.add_route('/mlGetAgreementList', mlGetAgreementList)
app.add_route('/mlS3Read', mls3read)
app.add_route('/mlUpload', mls3upload)
app.add_route('/mlClientUpdate', mlClientUpdate)
app.add_route('/mlAppActivityLogs', mlAppActivityLogs)
app.add_route('/mlListLoans', mlListLoans)
app.add_route('/mlListRepayments', mlListRepayments)
app.add_route('/mlDisbursalReport', mlPaymentDisbursalReport)
app.add_route('/mlMandateOrder', mlMandatePaymentOrder)
app.add_route('/mlUberOrder', mlUberPaymentOrder)
app.add_route('/mlCustReport', mlCustReport)
app.add_route('/mlAadharFill', mlAadharFill)
app.add_route('/mlCreateTask', mlCreateTask)
app.add_route('/mlShowTask', mlShowTask)
app.add_route('/mlCreateCallInfo', mlCreateCallInfo)
app.add_route('/mlCreateRepaymentInfo', mlCreateRepaymentInfo)
app.add_route('/mlShowRepaymentInfo', mlShowRepaymentInfo)
#app.add_route('/mlShowSms', mlShowSms)
app.add_route('/mlUberIncomeUpload', mlUberIncomeUpload)
app.add_route('/mlMandateDataUpload', mlMandateDataUpload)
app.add_route('/mlShowMandateData', mlShowMandateData)
app.add_route('/mlUpdateRepayInfo', mlUpdateRepayInfo)
app.add_route('/mlUpdateLoanStatus', mlUpdateLoanStatus)
app.add_route('/mlStoreCustomerData', mlStoreCustomerData)
app.add_route('/mlSendSms', mlSendSms)
app.add_route('/mlShowSmsTemplate', mlShowSmsTemplate)
app.add_route('/mlPaymentDisbursalUpload', mlPaymentDisbursalUpload)
app.add_route('/mlMandatePaymentsUpload', mlMandatePaymentsUpload)
app.add_route('/mlUberPaymentsUpload', mlUberPaymentsUpload)
app.add_route('/mlRejectLoan', mlRejectLoan)
#app.add_route('/mlUpgradeLoanLimitList', mlUpgradeLoanLimitList)
app.add_route('/mlGetUpfrontLoanLimit', mlGetUpfrontLoanLimit)
app.add_route('/mlApproveLoan', mlApproveLoan)
app.add_route('/mlDisburseLoan', mlDisburseLoan)
app.add_route('/mlInsertFinfluxRepayment', mlInsertFinfluxRepayment)
app.add_route('/mlGetAvailableProducts', mlGetAvailableProducts)
app.add_route('/mlGetAvailableProductsV2', mlGetAvailableProductsV2)
app.add_route('/mlShowFinfluxInsertLog', mlShowFinfluxInsertLog)
app.add_route('/mlProcessWebhook', mlProcessWebhook)
#app.add_route('/mlProcessWebhook/', mlProcessWebhook)
app.add_route('/mlGetCustInvestments', mlGetCustInvestments)
app.add_route('/mlGetStandardQueryList', mlGetStandardQueryList)
app.add_route('/mlRunStandardQuery', mlRunStandardQuery)
app.add_route('/mlSplitRepayments', mlSplitRepayments)
app.add_route('/mlInsertTransID', mlInsertTransID)
app.add_route('/mlGetCompanyCityProductDetails', mlGetCompanyCityProductDetails)
app.add_route('/mlInsertBankIfscDetails', mlInsertBankIfscDetails)
app.add_route('/mlGetBankIfscDetails', mlGetBankIfscDetails)
app.add_route('/mlGetCustAuditTrail', mlGetCustAuditTrail)
app.add_route('/mlGetUberAuth', mlGetUberAuth)
app.add_route('/mlRelianceOfflinePurchase', mlRelianceOfflinePurchase)
app.add_route('/mlGenerateRefundReport', mlGenerateRefundReport)
app.add_route('/mlStageSync', mlStageSync)
app.add_route('/mlFinfluxOauthUpdate', mlFinfluxOauthUpdate)
#app.add_route('/mlGetCustKycDocuments', mlGetCustKycDocuments)
app.add_route('/mlBulkLoanDisbursementUpload', mlBulkLoanDisbursementUpload)
app.add_route('/mlPoonawallaDisbursementUpload', mlPoonawallaDisbursalUpload)
app.add_route('/mlSendBulkSmsUpload', mlSendBulkSmsUpload)
app.add_route('/mlSendBulkObdCallsUpload', mlSendBulkObdCallsUpload)
app.add_route('/mlSwiggyWeeklyDeductionUpload', mlSwiggyWeeklyDeductionUpload)
app.add_route('/mlSwiggyRelianceTransactionFeedUpload', mlSwiggyRelianceTransactionFeedUpload)
app.add_route('/mlRejectUberLogin', mlRejectUberLogin)
app.add_route('/mlDashboardPoonawallaTeam', mlDashboardPoonawallaTeam)
app.add_route('/mlPushLoanToLender', mlPushLoanToLender)
app.add_route('/mlSetPushtoken', mlSetPushtoken)
app.add_route('/mlUpdateLoans', mlUpdateLoans)
app.add_route('/mlMapUberAuth', mlMapUberAuth)
app.add_route('/mlSwiggyIncomeUpload', mlSwiggyIncomeUpload)
app.add_route('/mlBulkCustomerCreate', mlBulkCustomerCreate)
##app.add_route('/mlDonationDisbursementUpload', mlDonationDisbursalUpload)
##app.add_route('/mlCreateCustomerDonationTag', mlCreateCustomerDonationTag)
##app.add_route('/mlViewDonationData', mlViewDonationData)
app.add_route('/mlBulkReschedule', mlBulkReschedule)
app.req_options.strip_url_path_trailing_slash = True
