import win32com.client

def create_workshops():
    outlook = win32com.client.Dispatch("Outlook.Application")
    
    workshops = [
        ("Workshop: Getting started with Project monitoring", "2026-05-12 10:00", 180),
        ("Workshop: Project Management in A Day", "2026-05-18 10:00", 330),
        ("Workshop: Project monitoring Streamlined", "2026-05-27 10:00", 180)
    ]
    
    for subject, start_time, duration in workshops:
        appt = outlook.CreateItem(1) # 1 = olAppointmentItem
        appt.Subject = subject
        appt.Start = start_time
        appt.Duration = duration
        appt.Location = "Ackers Adventure"
        appt.Body = "Training Workshop Booking - Saheli Hub"
        appt.ReminderSet = True
        appt.ReminderMinutesBeforeStart = 30
        appt.Save()
        print(f"Added: {subject}")

if __name__ == "__main__":
    create_workshops()