import datetime
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

usr = 'DDTAutomations'
pwd = 'TA2XVMLQ6^xDSm4cYbVTA$Qjx9CdVxZj'
driver = 'SQL Server Native Client 11.0'

server = 'OITORLSQL1A.r03.med.va.gov'
database = 'PowerHCM'
pwra = create_engine('mssql+pyodbc://{u}:{p}@{s}/{db}?driver={driver}&Trusted_Connection=no'.format(u=usr, p=pwd, s=server, db=database, driver=driver))


stub = 'all_positions_2'
today = datetime.date.today()
#today = '2024-05-12'


library = "Received"
extract_date = str(today)


#site = "oitautomations/customers"
# https://dvagov.sharepoint.com/sites/oitautomations/customers/Received/


file = f'c:\\data\\downloads\\{today}__{stub}.xlsx'
print(file)

df = pd.read_excel(file, engine='openpyxl', skiprows=12)

rename_dict = {
    'Business Unit': 'business_unit',
    'Admin/Staff Office': 'admin_staff_office',
    'Sub-Agency': 'sub-agency',
    'Sub-Agency Desc': 'sub-agency_desc',
    'Region': 'region',
    'POID': 'poid',
    'Station': 'station',
    'Dept ID': 'dept_id',
    'Dept Desc': 'dept_desc',
    'Cost Center': 'cost_center',
    'Cost Center Desc': 'cost_center_desc',
    'Org Code': 'org_code',
    'Org Code Desc': 'org_code_desc',
    'Position Number': 'position_number',
    'Date Position Established': 'date_position_established',
    #'Resource Board Approval': 'resource_board_approval',
    #'Budget Approval': 'budget_approval',
    #'HR Approval': 'hr_approval',
    #'Classification Approval': 'classification_approval',
    'Effective Date': 'effective_date',
    'Last Update UserID': 'last_update_userid',
    'Last Update Date/Time': 'last_update_date_time',
    'Location': 'location',
    'Location Desc': 'location_desc',
    'Status as of Effective Date': 'status_as_of_effective_date',
    'Action': 'action',
    'Reason': 'reason',
    'Position Status': 'position_status',
    'Status Date': 'status_date',
    'Job Code': 'job_code',
    'Pay Basis': 'pay_basis',
    'Pay Plan': 'pay_plan',
    'Pay Plan (OPM)': 'pay_plan_opm',
    'Grade': 'grade',
    'Target Grade': 'target_grade',
    'Occ Series': 'occ_series',
    'Official Position Title': 'official_position_title',
    'Title Code': 'title_code',
    'Assignment Code': 'assignment_code',
    'Assignment Code Desc': 'assignment_code_desc',
    #'Parenthetical Title': 'parenthetical_title',
    'Title Code Prefix': 'title_code_prefix',
    'Title Code Suffix': 'title_code_suffix',
    'Appointing Authority': 'appointing_authority',
    'Full/Part Time': 'full_part_time',
    'Standard Hours': 'standard_hours',
    'FTE': 'fte',
    'FTE (Calc=Standard Hours/40)': 'fte_std_hours_40',
    'Work Schedule': 'work_schedule',
    'Regular/Temporary': 'regular_temporary',
    'NTE Date': 'nte_date',
    'Budgeted Position': 'budgeted_position',
    'Job Sharing Permitted': 'job_sharing_permitted',
    'Position Eligible for Telework': 'position_eligible_for_telework',
    'FLSA': 'flsa',
    # 'Comp Level': 'comp_level',
    # 'Functional Class': 'functional_class',
    'Drug Test (Applicable)': 'drug_test_applicable',
    # 'Salary Admin Plan': 'salary_admin_plan',
    # 'Security Clearance Type': 'security_clearance_type',
    'Supervisory Level': 'supervisory_level',
    'Supervisory Level Desc': 'supervisory_level_desc',
    'PD/Functional Stmt Number': 'pd_functional_statement_number',
    'Detailed Position Description': 'detailed_position_description',
    'Fund Source': 'fund_source',
    # 'LEO/Fire Position': 'leo_fire_position',
    'BUS Code': 'bus_code',
    'Position Location': 'position_location',
    'Position Occupied': 'position_occupied',
    # 'Special Population Code': 'special_population_code',
    'Sensitivity Code': 'sensitivity_code',
    # 'Confidential Position': 'confidential_position',
    # 'Job Family': 'job_family',
    # 'PATCOB': 'patcob',
    # 'COVID-19': 'covid-19',
    # 'Obligated To ID': 'obligated_to_id',
    # 'Obligation Expiration': 'obligation_expiration',
    # 'Special Program': 'special_program',
    # 'Special Program Begin Date': 'special_program_start_date',
    # 'Special Program End Date': 'special_program_end_date',
    'Sub Account Code': 'sub_account_code',
    'Fund Control Point': 'fund_control_point',
    'Personnel Action Request Nbr': 'personnel_action_num',
    'Reports To Position Number': 'reports_to_position_number',
    'Reports To Empl ID': 'reports_to_empl_id',
    'Reports To Name': 'reports_to_name',
    'Max Head Count': 'max_head_count',
    'Head Count': 'head_count',
    'Head Count Status': 'head_count_status',
    'Head Count Status Info': 'head_count_status1',
    'Current Incumbent Empl ID': 'current_incumbent_empl_id',
    'Current Incumbent Empl Record': 'current_incumbent_empl_record',
    'Current Incumbent Name': 'current_incumbent_name',
    # 'Current Incumbent Pay Status': 'current_incumbent_pay_status',
    'Last Encumbered Date': 'last_encumbered_date',
    'Last Incumbent Empl ID': 'last_incumbent_empl_id',
    'Last Incumbent Name': 'last_incumbent_name',
    'OrgStructure Cd' : 'org_structure_code',
    'Descr' : 'org_structure_desc',
    'County' : 'county',
    'Current Incumbent Empl Status': 'current_incumbent_empl_status',
}

df.rename(columns=rename_dict, inplace=True)

df['extract_date'] = datetime.datetime.now()
#df['extract_date'] = datetime.datetime(2024,5,12,9,0)

df['record_hash'] = np.nan

df.to_sql('RPT_OIT_AllPositionReport_RAW', con=pwra, index=False, if_exists='replace')

## CHAVA    --------------------------------  run the sp to get the changes
## http://oitorlsql1a.r03.med.va.gov/Reports/report/Manpower/HCM%20DDT/hr_daily_changes

connection = pwra.raw_connection()

try:
    cursor = connection.cursor()
    restult = cursor.execute('exec proc_daily_changes_v2')
    cursor.close()
    connection.commit()
    print('')
    print("proc_daily_changes: ok")
    print('')
finally:
    connection.close() 

print("")
print(restult)