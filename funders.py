import pyodbc
from datetime import datetime

# =========================
# CONFIG
# =========================
SERVER = "20.68.160.100"
DATABASE = "SahelihubCRM"
USERNAME = "saheli_app"
PASSWORD = "309183"

TABLE_NAME = "dbo.FundingProjects"

# ODBC Driver examples:
# "ODBC Driver 17 for SQL Server"
# "ODBC Driver 18 for SQL Server"
DRIVER = "ODBC Driver 17 for SQL Server"

# Set to True if you want to delete existing rows with the same FunderProject before insert
DELETE_EXISTING_BY_FUNDERPROJECT = False

# =========================
# DATA
# =========================
rows = [
    {
        "FunderProject": "Birmingham City Council – Coalition For Impact",
        "FundingManagementLead": "Naseem",
        "ResponsibleForReport": "Rob and Aesha",
        "StrategicObjectives": "7. Influence agenda for ethnically diverse; 8. Showcase work to partners; 9. Workforce development; 10. Evidence and impact",
        "ValueGBP": 16000,
        "StartDate": "2024-10-01",
        "EndDate": "2025-03-31",
        "SiteArea": "ARCC/East",
        "Targets": "Develop a Community Economic Plan for the “Sports Quarter” in parts of Alum Rock, Washwood Heath & Bordesley Green.",
        "ReportingEvaluation": "Evidence from events, e.g. comments on plans, photos and videos; Complete an evaluation with Loconomy; Submit final Community Economic Plan.",
        "Deadlines": "Interim report – December 2024; End of project report – March 2025",
        "Status": "Amber",
        "Commentary": "Underway",
        "Link": "Coalition For Impact Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Birmingham City Council - UK Shared Prosperity Fund",
        "FundingManagementLead": "Naseem",
        "ResponsibleForReport": "Rob",
        "StrategicObjectives": "1. Increase activity; 5. Socialise and have fun",
        "ValueGBP": 21075,
        "StartDate": "2024-11-01",
        "EndDate": "2025-03-31",
        "SiteArea": "All",
        "Targets": "Purchase new minibus; Number of people reached – 2,500 people per year (journeys taken/footfall); Number of local events or activities supported – 23 per week/920 per year; Improved engagement numbers – 1,100 people per year (journeys taken/footfall).",
        "ReportingEvaluation": "Report with numbers of people/activities supported.",
        "Deadlines": "Apr-25",
        "Status": "Green",
        "Commentary": "Completed",
        "Link": "UKSPF Minibus Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Bsol ICB - WorkWell",
        "FundingManagementLead": "Aesha",
        "ResponsibleForReport": "Aesha",
        "StrategicObjectives": "4. Advice one stop shop; 3. Targeted health interventions; 6. Tackle inequalities in health system; 7. Influence agenda for ethnically diverse",
        "ValueGBP": 271000,
        "StartDate": "2024-10-01",
        "EndDate": "2026-03-31",
        "SiteArea": "East - Restricted to certain areas",
        "Targets": "300 citizens are supported with employment and health advice to overcome health-related barriers to employment; Create a WorkWell action plan with each participant and support them to achieve their agreed objectives; Citizens are supported to gain or stay in employment; Improved Employability; Improved Health and Wellbeing.",
        "ReportingEvaluation": "Participant demographic information; Initial assessment showing current circumstances and barriers to employment; Progress towards agreed outcomes and evidence of outcome(s) achieved; Follow up with each participant 3 months post-exit; Stories and feedback to showcase difference made.",
        "Deadlines": "Monthly data submission through online portal; Quarterly monitoring reports and meetings.",
        "Status": "Amber",
        "Commentary": "Mobilizing – On track",
        "Link": "WorkWell Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Fairer Futures Fund – Innerva Fitness",
        "FundingManagementLead": "Rakhyia",
        "ResponsibleForReport": "Rakhyia and Fozia",
        "StrategicObjectives": "1. Increase activity; 3. Targeted health interventions; 4. cohort needs to complete min 12 weeks; 5. only for East service users",
        "ValueGBP": 45000,
        "StartDate": "2025-01-01",
        "EndDate": "2027-12-30",
        "SiteArea": "Arcc/East",
        "Targets": "120 people per year (360 total) attend Innerva sessions; 120 sessions delivered per year (360 total); Increased Physical Activity; Improved Health - Reduced weight, reduced blood pressure and improved cardiovascular function; Better Managing Long Term Health – Improved knowledge and awareness of their health and how to manage it.",
        "ReportingEvaluation": "Attendance and demographic information; Saheli Hub Health Assessment tracking health & wellbeing improvements to demonstrate outcomes; Stories, feedback, photos and videos to showcase difference made.",
        "Deadlines": "Monitoring meeting – Every 3 months: Apr 25, Jul 25, Oct 25, Jan 26, Apr 26, Jul 26, Oct 26, Jan 27, Apr 27, Jul 27, Oct 27, Dec 27; Interim report – Every 6 months: Jul 25, Dec 25, Jul 26, Dec 26, Jan 27, Jul 27; End of project report – Oct 2027; Health data needs to be reported through their system RedCap.-(NEED UPDATE)",
        "Status": "Amber",
        "Commentary": "Mobilising",
        "Link": "Fairer Futures Fund 1 Innerva Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Fairer Futures Fund – Men’s Know Your Health Numbers",
        "FundingManagementLead": "Rakhyia and Rabia",
        "ResponsibleForReport": "Usman",
        "StrategicObjectives": "3. Targeted health interventions",
        "ValueGBP": 45000,
        "StartDate": "2025-01-01",
        "EndDate": "2027-12-30",
        "SiteArea": "All",
        "Targets": "Aims to reduce hypertension and cardiovascular disease; 80 men per year (240 total) attend Know Your Health Numbers checks; 360 hours of volunteering will support session delivery; Improved Health – Reduced blood pressure and improved cardiovascular function; Increased Physical Activity.",
        "ReportingEvaluation": "Attendance and demographic information; Saheli Hub Health Assessment tracking health & wellbeing improvements to demonstrate outcomes; Number of volunteer hours; Stories, feedback, photos and videos to showcase difference made.",
        "Deadlines": "Monitoring meeting – Every 3 months: Apr 25, Jul 25, Oct 25, Jan 26, Apr 26, Jul 26, Oct 26, Dec 26; Interim report – Every 6 months: Jul 25, Jan 26, Jul 26; End of project report; Health data needs to be reported through their system RedCap.",
        "Status": "Amber",
        "Commentary": "Mobilizing",
        "Link": "Fairer Futures Fund 2 Men's Know Your Health Numbers Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Hodge Hill NNS – Innerva Fitness",
        "FundingManagementLead": "Rakhyia",
        "ResponsibleForReport": "Rakhyia and Fozia",
        "StrategicObjectives": "1. Increase activity; 3. Targeted health interventions",
        "ValueGBP": 18400,
        "StartDate": "2024-09-01",
        "EndDate": "2025-08-30",
        "SiteArea": "ARCC/East",
        "Targets": "120 older citizens aged 50+ attend Innerva sessions (80 women and 40 men); 60 citizens aged 18-50 with long-term disabilities attend Innerva sessions; 120 sessions delivered per year (10 per month); Increased Physical Activity; Improved Health - Reduced weight, reduced blood pressure, increased mobility and improved cardiovascular function.",
        "ReportingEvaluation": "Attendance and demographic information; Saheli Hub Health Assessment tracking health & wellbeing improvements to demonstrate outcomes; Stories, feedback, photos and videos to showcase difference made.",
        "Deadlines": "End of project report; data collection June 25; start report July 25",
        "Status": "Green",
        "Commentary": "On track",
        "Link": "Hodge Hill NNS 1 & 2 Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "HS2 – Community & Environment Fund",
        "FundingManagementLead": "Naseem, Rakhyia and Usman",
        "ResponsibleForReport": "Usman and Rakhyia",
        "StrategicObjectives": "1. Increase activity",
        "ValueGBP": 74995,
        "StartDate": "2024-04-01",
        "EndDate": "2027-03-31",
        "SiteArea": "ARCC/East - Restricted to certain areas",
        "Targets": "630 men participate in holistic health interventions; % increase of men feeling they have better physical health and mental health; % increase of men feeling they are better informed and confident approaching mainstream providers; % increase of men feeling they are better motivated to sustain their health in the longer term; Reduced isolation; Reduced GP & hospital admissions; Increased active travel and use of green spaces.",
        "ReportingEvaluation": "Surveys capturing outcomes; Stories, feedback, photos and videos to showcase difference made.",
        "Deadlines": "Every 6 months",
        "Status": "Green",
        "Commentary": "On track",
        "Link": "HS2 Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Lawn Tennis Association – Tennis Foundation",
        "FundingManagementLead": "Naseem and Aesha",
        "ResponsibleForReport": "Rabia and Lead",
        "StrategicObjectives": "1. Increase activity; 2. New opportunities with NGBs (Squash, Football, Cricket); 7. Influence agenda for ethnically diverse; 9. Workforce development",
        "ValueGBP": 126000,
        "StartDate": "2024-04-01",
        "EndDate": "2027-03-31",
        "SiteArea": "All",
        "Targets": "Deliver women’s tennis sessions across 4 community venues; events 26 Jul and 2 Aug at Calthorpe; Upskill volunteer tennis coaches from the local community via the Training Academy; Organize 2 promotional events.",
        "ReportingEvaluation": "Jun 25, Dec 25, Jun 26, Dec 26, Feb 27",
        "Deadlines": "Every 6 months",
        "Status": "RED",
        "Commentary": "Need to urgently recruit lead tennis coach",
        "Link": "LTA Tennis Foundation Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Northfield NNS – Men’s Health & Wellbeing Club",
        "FundingManagementLead": "Usman",
        "ResponsibleForReport": "Usman & Rakhyia",
        "StrategicObjectives": "1. Increase activity; 3. Targeted health interventions",
        "ValueGBP": 5000,
        "StartDate": "2024-06-01",
        "EndDate": "2025-03-31",
        "SiteArea": "Weoley Castle",
        "Targets": "75 men over 50 will attend activities at Weoley Castle library; 55 hours of volunteering will support session delivery; Increased Physical and Mental Health; Reduced Isolation.",
        "ReportingEvaluation": "Attendance and demographic information; Saheli Hub Health Assessment tracking health & wellbeing improvements to demonstrate outcomes; Stories, feedback, photos and videos to showcase difference made.",
        "Deadlines": "End of project report.",
        "Status": "Green",
        "Commentary": "On track",
        "Link": "Northfield NNS Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Perry Barr NNS – Cycling",
        "FundingManagementLead": "Shaafia",
        "ResponsibleForReport": "Shaafia",
        "StrategicObjectives": "1. Increase activity",
        "ValueGBP": 5000,
        "StartDate": "2024-08-01",
        "EndDate": "2025-07-31",
        "SiteArea": "West",
        "Targets": "50 older citizens attend cycling sessions in Handsworth Park; Deliver 3 x 6 week cycling courses; Increased Physical Activity; Reduced Isolation. (WEMWBS and health checks)",
        "ReportingEvaluation": "Attendance and demographic information; Saheli Hub Health Assessment tracking health & wellbeing improvements to demonstrate outcomes; Stories, feedback, photos and videos to showcase difference made.",
        "Deadlines": "End of project report. May start prep, June start writing",
        "Status": "Green",
        "Commentary": "On track",
        "Link": "Perry Barr NNS 2 Cycling Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Perry Barr NNS – Indoor Activities",
        "FundingManagementLead": "Rakhyia",
        "ResponsibleForReport": "Rakhyia",
        "StrategicObjectives": "1. Increase activity; 5. Socialise and have fun",
        "ValueGBP": None,
        "StartDate": "2024-08-01",
        "EndDate": "2025-07-31",
        "SiteArea": "West",
        "Targets": "50 people attend indoor activities in Handsworth including Social Knit, Talking Art & Chair-Based Exercise; Reduced Isolation; Improved Mental Health; Improved Mobility/Physical Activity.",
        "ReportingEvaluation": "Attendance and demographic information; Saheli Hub Health Assessment tracking health & wellbeing improvements to demonstrate outcomes; Stories, feedback, photos and videos to showcase difference made.",
        "Deadlines": "End of project report.",
        "Status": "Green",
        "Commentary": "On track",
        "Link": "Perry Barr NNS 1 Indoor Activities Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Police & Crime Commissioner - Safe & Confident On The Canals",
        "FundingManagementLead": "Shaafia",
        "ResponsibleForReport": "Shaafia",
        "StrategicObjectives": "1. Increase activity",
        "ValueGBP": 4550,
        "StartDate": "2025-01-01",
        "EndDate": "2025-06-30",
        "SiteArea": "West",
        "Targets": "Deliver 3 sets of 6 week self-defense classes for women; New cycling and bellboating equipment; 100 women participate in activities; Women feel safer and more confident using canals and other local spaces in their communities; Improved social connections and reduced isolation.",
        "ReportingEvaluation": "Brief written update & evidence of expenditure.",
        "Deadlines": "End report c. June 2025",
        "Status": "Amber",
        "Commentary": None,
        "Link": "Police & Crime Commissioner Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Prevention & Communities – I Am Birmingham",
        "FundingManagementLead": "Rakhyia/Rabia and Naseem",
        "ResponsibleForReport": "Rakhyia/Rabia and Rob",
        "StrategicObjectives": "4. Advice one stop shop; 5. Socialize and have fun social clubs",
        "ValueGBP": 75000,
        "StartDate": "2023-10-01",
        "EndDate": "2026-09-30",
        "SiteArea": "All",
        "Targets": "55 vulnerable older adults supported, reducing reliance on public services; 120 fun mixed ability activities; 60% will feel less isolated; 70% will connect with others from different cultures, ages and abilities and build relationships leading to friendship; 70% will feel happier; 60% will become active citizens, building sense of community and belonging; 70% will be better informed and connect with additional services; 70% will develop their hobbies and passions; 50% will feel physically/mentally fitter.",
        "ReportingEvaluation": "Written update covering outputs and outcomes; Spend update; Workbook – Attendance and demographic information; 1 story; Photos and videos",
        "Deadlines": "Quarterly report April 25, July 25, Oct25, Jan 26, May 26",
        "Status": "Green",
        "Commentary": "Ongoing delivery – achieved all targets and positive feedback from funder",
        "Link": "Prevention & Communities Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Sport England – Capacity Building",
        "FundingManagementLead": "Naseem",
        "ResponsibleForReport": "Naseem and Rob",
        "StrategicObjectives": "6. Tackle inequalities in health system; 7. Influence agenda for ethnically diverse; 8. Showcase work to partners; 9. Workforce development; 10. Evidence and impact",
        "ValueGBP": 262996,
        "StartDate": "2024-04-01",
        "EndDate": "2027-03-31",
        "SiteArea": "All",
        "Targets": "Create capacity to manage the organisation effectively and free up the capacity of its founder to develop new contracts; Strengthen its business skills and planning through the support of expert advice and the use of appropriate business diagnostic tools; Support the organisation’s leaders with additional leadership skills and mentoring support.",
        "ReportingEvaluation": "TCB",
        "Deadlines": "TBC",
        "Status": "Amber",
        "Commentary": "On track",
        "Link": "Sport England Capacity Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Yardley NNS - Men's Health Club",
        "FundingManagementLead": "Usman",
        "ResponsibleForReport": "Usman & Rakhyia",
        "StrategicObjectives": "1. Increase activity; 3. Targeted health interventions; 5. Socialise and have fun",
        "ValueGBP": 10665,
        "StartDate": "2024-07-01",
        "EndDate": "2025-05-31",
        "SiteArea": "East - Yardley",
        "Targets": "Weekly programme of men's activities in Yardley; 50% over 50s and 50% 18-49 with disabilities; People feel more connected to their community; People being and staying more physically and mentally active; People with conditions listed engaging in physical and/or social activities.",
        "ReportingEvaluation": "Quarterly",
        "Deadlines": "May-25",
        "Status": "Green",
        "Commentary": "Ongoing delivery",
        "Link": "Yardley NNS 1 & 2 Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Clarion Futures Wellbeing Spaces",
        "FundingManagementLead": "Rakyia",
        "ResponsibleForReport": "Rakhyia & Rob",
        "StrategicObjectives": "1. Increase activity; 4. Advice one stop shop; 5. Socialise and have fun",
        "ValueGBP": 15000,
        "StartDate": "2025-04-01",
        "EndDate": "2026-03-31",
        "SiteArea": "ARCC",
        "Targets": "Develop and enhance health & wellbeing services at ARCC; Support community living on the estate and around ARCC; Offer a safe wellbeing space including healthy cooking, physical and social activities; Added advice and guidance support; Provision of 50 Wellbeing Care Packages and 20 Air Fryers for community members.",
        "ReportingEvaluation": "Attendance/ Demographics, case studies. Increased wellness and health statistics.",
        "Deadlines": "End of grant report April 2026",
        "Status": "Green",
        "Commentary": "Mobilising",
        "Link": "Clarion Futures Wellbeing Space Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Warm Hub Fund",
        "FundingManagementLead": "Rabia",
        "ResponsibleForReport": "Rabia",
        "StrategicObjectives": None,
        "ValueGBP": 6000,
        "StartDate": None,
        "EndDate": None,
        "SiteArea": "ALL",
        "Targets": "To create warm space for service user",
        "ReportingEvaluation": "na",
        "Deadlines": "na",
        "Status": None,
        "Commentary": "Received £6000 from Thrive (Birmingham City Council)",
        "Link": None,
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Fairer Futures Fund - Healthy Neighbourhoods (Central Locality Partnership)",
        "FundingManagementLead": "Aesha & Rabia",
        "ResponsibleForReport": "Aesha, Rabia & Pulkit",
        "StrategicObjectives": None,
        "ValueGBP": 346135,
        "StartDate": "2025-07-01",
        "EndDate": "2028-03-31",
        "SiteArea": "Calthorpe/Central",
        "Targets": "Saheli Hub to lead a partnership with The Springfield Project and CLDP; Deliver programme of activities including physical activity, nutrition, health literacy, health checks, etc.; 150 participants per year (450 total) who are inactive and/or obese, split 60/40 for first 1.5 years and then 50/50 for rest with The Springfield Project; Increased Physical Activity; Reduced weight; Improved dietary habits and eating choices; Increased health literacy, empowering citizens to manage their health and lifestyle.",
        "ReportingEvaluation": "Attendance and demographic information; Saheli Hub Health Assessment tracking health & wellbeing improvements to demonstrate outcomes; Number of volunteer hours; Stories, feedback, photos and videos to showcase difference made.",
        "Deadlines": "1st October 2025; 1st January 2026; 1st April 2026; 1st July 2026; 1st October 2026; 1st January 2027; 1st April 2027; 1st July 2027; 1st October 2027; 1st January 2028",
        "Status": "Amber",
        "Commentary": "Mobilising",
        "Link": "Fairer Futures Fund 3 Central Locality (Partnership) Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Fairer Futures Fund - East Locality Partnership",
        "FundingManagementLead": "Rakhyia",
        "ResponsibleForReport": "Rakhyia, Pulkit & Sultana",
        "StrategicObjectives": "1. Increase activity; 3. Targetted health intervention; 6. Tackle inequalities in health system; 7. Influence agenda for ethnically diverse; 8. Showcase work to partners; 10. Evidence and impact",
        "ValueGBP": 138000,
        "StartDate": "2025-07-01",
        "EndDate": "2028-03-31",
        "SiteArea": "ARCC/East",
        "Targets": "Deliver programme of activities including physical activity, nutrition, health literacy, health checks, etc.; 100 participants per year (300 total) who are inactive and/or obese; Increased Physical Activity; Reduced weight; Improved dietary habits and eating choices; Increased health literacy, empowering citizens to manage their health and lifestyle.",
        "ReportingEvaluation": "Attendance and demographic information; Saheli Hub Health Assessment tracking health & wellbeing improvements to demonstrate outcomes; Number of volunteer hours; Stories, feedback, photos and videos to showcase difference made.",
        "Deadlines": "Quarterly TBC",
        "Status": "Amber",
        "Commentary": "Mobilising",
        "Link": "Fairer Futures Fund 4 East Locality Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Fairer Futures Fund - City-Wid Partnership(Game of Stones) BCAN",
        "FundingManagementLead": "Naseem & Rabia",
        "ResponsibleForReport": "Rabia & Pulkit",
        "StrategicObjectives": "1. Increase activity; 3. Targetted health intervention; 6. Tackle inequalities in health system; 7. Influence agenda for ethnically diverse; 8. Showcase work to partners; 10. Evidence and impact; Active diagnosis of type 2 diabetes; BMI of 27.5 or above (obese); If on diabetes medication, HbA1c 43 to 75 mmol/mol; If not on diabetes medication, HbA1c 48 to 75 mmol/mol",
        "ValueGBP": 100000,
        "StartDate": "2025-07-01",
        "EndDate": "2027-06-30",
        "SiteArea": "All",
        "Targets": "Support citizens with type 2 diabetes to manage their condition, lose weight and to enter remission; 50 participants per year (100 total) who have type 2 diabetes (see summary sheet for criteria); Reduced weight; Reduced HbA1c levels.",
        "ReportingEvaluation": "Attendance and demographic information; Saheli Hub Health Assessment tracking health & wellbeing improvements to demonstrate outcomes; Number of volunteer hours; Stories, feedback, photos and videos to showcase difference made.",
        "Deadlines": "Quarterly TBC",
        "Status": "Amber",
        "Commentary": "Mobilising",
        "Link": "Fairer Futures Fund 5 City-Wide Partnership Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "East Birmingham 24/7 Neighbourhood Mental Health Centre (NMHC)",
        "FundingManagementLead": "Aesha",
        "ResponsibleForReport": "Pulkit & Rakhiya",
        "StrategicObjectives": "1. Increase activity; 2. Targeted health interventions; 3. Tackle inequalities in health system; 4. Influence agenda for ethnically diverse; 5. Evidence and impact",
        "ValueGBP": 29014,
        "StartDate": "2025-08-18",
        "EndDate": "2026-03-31",
        "SiteArea": "ARCC/Small Heath",
        "Targets": "Sports; Monthly wellbeing surgeries; Entry health checks (BP, BMI, AF, weight); 1:1 drop-in support; Peer champions and volunteers; 12 ppl for 12 weeks, 45 min session followed by min 20min MH session complete work book; Monthly recap sessions and invitation to other partners and end of 12 weeks group celebration",
        "ReportingEvaluation": "Mid-point report: March 2026; Final report: August 2026; Attendance and demographic data; Health indicators; Participant feedback and case studies; Reflective artwork",
        "Deadlines": "March 2026, August 2026",
        "Status": "Amber",
        "Commentary": "Mobilising",
        "Link": "East Birmingham 24/7 Neighbourhood Mental Health Centre (NMHC)",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Innerva Fitness Programme – Specialist Health Coach(Peter Harrison Foundation)",
        "FundingManagementLead": "Naseem",
        "ResponsibleForReport": "Pulkit & Rakhiya",
        "StrategicObjectives": "1. Increase activity; 2. Targeted health interventions; 3. Tackle inequalities in health system; 4.Influence agenda for ethnically diverse; 5. Evidence and impact",
        "ValueGBP": 30000,
        "StartDate": "2025-08-01",
        "EndDate": "2027-08-01",
        "SiteArea": "All",
        "Targets": "Specialist health coach to deliver sessions to support the Innerva Fitness programme",
        "ReportingEvaluation": "Progress report at 14 months; Final report at 26 months; Attendance and demographic information; Saheli Hub Health Assessment tracking health & wellbeing improvements; Stories, feedback, photos",
        "Deadlines": "Progress Report: October 2026; Final Report: October 2027",
        "Status": "Amber",
        "Commentary": "Mobilising",
        "Link": "Innerva Fitness Programme – Specialist Health Coach",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Five Ways to Wellbeing – Thrive Together",
        "FundingManagementLead": "Aesha",
        "ResponsibleForReport": "Pulkit & Rakhiya",
        "StrategicObjectives": "Increase activity; Targeted health interventions; Tackle inequalities in health system; Influence agenda for ethnically diverse; Evidence and impact",
        "ValueGBP": 10000,
        "StartDate": "2025-09-01",
        "EndDate": "2026-08-31",
        "SiteArea": "ARCC & Calthorpe",
        "Targets": "Support 120 neurodiverse, long-term unemployed individuals and carers; Weekly power-assisted fitness sessions; Chair-based and stretching exercises; Community walks, dance, and cycling groups; Peer support networks; 30% increase in participants meeting national activity guidelines; Dropout rate below 15%",
        "ReportingEvaluation": "Monthly monitoring meetings; Quarterly reports; Final evaluation report; WEMWBS scores; Attendance and engagement data; Case studies and feedback",
        "Deadlines": "Quarterly reports: Dec 2025, Mar 2026, Jun 2026; Final report: August 2026",
        "Status": "Amber",
        "Commentary": "mobilising",
        "Link": "Thrive Together – Five Ways to Wellbeing Summary Sheet.docx",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Veolia Sustainability Fund – Make It with Saheli",
        "FundingManagementLead": "Rabia Khatun",
        "ResponsibleForReport": "Rabia Khatun",
        "StrategicObjectives": None,
        "ValueGBP": 920,
        "StartDate": "2025-11-11",
        "EndDate": "2026-06-30",
        "SiteArea": "Calthorpe",
        "Targets": "Deliver weekly upcycling workshops using recyclable materials; Engage 20+ local residents; Divert household waste from landfill; Promote sustainability and creative reuse; Strengthen community bonds",
        "ReportingEvaluation": "“Before” quote at project start; Images of sessions and completed work; “After” quote/testimonial; Final summary of outcomes and engagement; Open to Veolia visits and social media feature",
        "Deadlines": "Final report – June 2026",
        "Status": "Amber",
        "Commentary": None,
        "Link": "Make It with Saheli",
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
    {
        "FunderProject": "Fairer Futures Fund - East Locality Partnership",
        "FundingManagementLead": "Rakhyia",
        "ResponsibleForReport": "Rakhyia, Pulkit & Sultana",
        "StrategicObjectives": "1. Increase activity; 3. Targetted health intervention; 6. Tackle inequalities in health system; 7. Influence agenda for ethnically diverse; 8. Showcase work to partners; 10. Evidence and impact",
        "ValueGBP": 117500,
        "StartDate": "2026-04-01",
        "EndDate": "2028-03-31",
        "SiteArea": "ARCC/East",
        "Targets": "Deliver a culturally appropriate health programme focused on health literacy, healthy weight and healthy behaviours; work with 300 inactive and/or obese citizens across 2 years (150 per year average); physical activity, diet & nutrition, health checks and community support; earlier detection, prevention and treatment of obesity, cardiovascular disease and diabetes/pre-diabetes; connect participants to clinical support where needed",
        "ReportingEvaluation": "Quarterly progress reports including performance targets, outcomes, demographic and equalities data; quarterly income & expenditure analysis; REDCap updates at least quarterly; attendance and demographic information; health assessment / public health outcome measures; participant feedback, case studies and evaluation evidence; participation in programme evaluation activity",
        "Deadlines": "Quarterly returns due on the 15th of the month following each quarter; mid-year programme reviews: Oct 2026 and Oct 2027; quarterly project review meetings with VCFSE lead / BCC",
        "Status": "Amber",
        "Commentary": "COGA received for East contract review; mobilisation and monitoring requirements set out.",
        "Link": None,
        "Comments": None,
        "ProjectTracker": None,
        "DeadlineTracker": None,
    },
]

# =========================
# SQL
# =========================
insert_sql = f"""
INSERT INTO {TABLE_NAME}
(
    FunderProject,
    FundingManagementLead,
    ResponsibleForReport,
    StrategicObjectives,
    ValueGBP,
    StartDate,
    EndDate,
    SiteArea,
    Targets,
    ReportingEvaluation,
    Deadlines,
    Status,
    Commentary,
    Link,
    Comments,
    ProjectTracker,
    DeadlineTracker,
    CreatedAtUtc,
    UpdatedAtUtc
)
VALUES
(
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETUTCDATE(), GETUTCDATE()
)
"""

delete_sql = f"DELETE FROM {TABLE_NAME} WHERE FunderProject = ?"

# =========================
# RUN
# =========================
def make_connection():
    conn_str = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

def normalize_date(value):
    if value in (None, "", "NULL"):
        return None
    return value  # yyyy-mm-dd strings are fine for SQL Server via pyodbc

def main():
    conn = make_connection()
    cursor = conn.cursor()

    inserted = 0
    deleted = 0

    try:
        for row in rows:
            if DELETE_EXISTING_BY_FUNDERPROJECT:
                cursor.execute(delete_sql, row["FunderProject"])
                deleted += cursor.rowcount if cursor.rowcount != -1 else 0

            params = (
                row["FunderProject"],
                row["FundingManagementLead"],
                row["ResponsibleForReport"],
                row["StrategicObjectives"],
                row["ValueGBP"],
                normalize_date(row["StartDate"]),
                normalize_date(row["EndDate"]),
                row["SiteArea"],
                row["Targets"],
                row["ReportingEvaluation"],
                row["Deadlines"],
                row["Status"],
                row["Commentary"],
                row["Link"],
                row["Comments"],
                row["ProjectTracker"],
                row["DeadlineTracker"],
            )

            cursor.execute(insert_sql, params)
            inserted += 1

        conn.commit()
        print(f"Done. Inserted {inserted} row(s).")
        if DELETE_EXISTING_BY_FUNDERPROJECT:
            print(f"Deleted existing rows by FunderProject before insert: {deleted}")
    except Exception as e:
        conn.rollback()
        print("Failed. Transaction rolled back.")
        print(str(e))
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()