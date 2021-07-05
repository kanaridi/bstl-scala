from datetime import timedelta, date
import re


def workday(datevalue, offset):
    # -------------------------------------------------------------------------
    # Workday Function
    # -------------------------------------------------------------------------
    datevalue = str(datevalue)
    # use regex to to seperate datevalue into "Year, Month, and Day"
    datelist = re.findall(r"[\w']+", datevalue)
    if len(datelist) > 1:
        y = int(datelist[0])
        m = int(datelist[1])
        d = int(datelist[2])
    elif len(datelist) == 1:
        y = int(datevalue[0:4])
        m = int(datevalue[4:6])
        d = int(datevalue[6:8])
    # pass the datelist values to the "date" fuction
    startdate = date(y, m, d)
    i = 0
    workdays = startdate
    # iterate through offset to return working days
    while i < offset:
        # weekday
        # print(newdate.isoweekday())
        if workdays.isoweekday() < 5 or workdays.isoweekday() == 7:
            workdays += timedelta(days=1)
            # weekend
        else:
            # print("weekends don't count for offset, so add 1")
            workdays += timedelta(days=1)
            # weekends don't count for offset, so add 1
            offset += 1
        i += 1
    return workdays.strftime("%Y%m%d")


def workdayStart(datevalue, offset):
    # -------------------------------------------------------------------------
    # WorkdayStart Function
    # -------------------------------------------------------------------------
    datevalue = str(datevalue)
    # use regex to to seperate datevalue into "Year, Month, and Day"
    datelist = re.findall(r"[\w']+", datevalue)
    if len(datelist) > 1:
        y = int(datelist[0])
        m = int(datelist[1])
        d = int(datelist[2])
    elif len(datelist) == 1:
        y = int(datevalue[0:4])
        m = int(datevalue[4:6])
        d = int(datevalue[6:8])
    # pass the datelist values to the "date" fuction
    startdate = date(y, m, d)
    i = 0
    workdaystart = startdate
    # iterate through offset to return working days
    while i < offset:
        # weekday
        # print(newdate.isoweekday())
        if 1 < workdaystart.isoweekday() < 7:
            workdaystart -= timedelta(days=1)
        # weekend
        else:
            # print("weekends don't count for offset, so add 1")
            workdaystart -= timedelta(days=1)
            # weekends don't count for offset, so add 1
            offset += 1
        i += 1
    return workdaystart


def compareWorkingdays(datevalue, comparedate):
    # -------------------------------------------------------------------------
    # Compare Workday Function
    # -------------------------------------------------------------------------
    datevalue = str(datevalue)
    # use regex to to seperate datevalue into "Year, Month, and Day"
    datelist = re.findall(r"[\w']+", datevalue)
    if len(datelist) > 1:
        y = int(datelist[0])
        m = int(datelist[1])
        d = int(datelist[2])
    elif len(datelist) == 1:
        y = int(datevalue[0:4])
        m = int(datevalue[4:6])
        d = int(datevalue[6:8])
    # pass the datelist values to the "date" fuction
    startdate = date(y, m, d)
    comparedate = str(comparedate)
    # use regex to to seperate datevalue into "Year, Month, and Day"
    datelist2 = re.findall(r"[\w']+", comparedate)
    if len(datelist2) > 1:
        y2 = int(datelist2[0])
        m2 = int(datelist2[1])
        d2 = int(datelist2[2])
    elif len(datelist) == 1:
        y2 = int(datelist2[0:4])
        m2 = int(datelist2[4:6])
        d2 = int(datelist2[6:8])
    # pass the datelist values to the "date" fuction
    comparedate = date(y2, m2, d2)
    i = 0
    # iterate through offset to return working days
    while startdate < comparedate:
        if startdate.isoweekday() <= 5:
            startdate += timedelta(days=1)
            i += 1
        # weekend
        else:
            # weekends don't count for offset, so skip updating "i"
            startdate += timedelta(days=1)
    networkdays = i
    return networkdays


def lastWorkdayofMonth(datevalue):
    # -------------------------------------------------------------------------
    # Last Workday of Month Function
    # -------------------------------------------------------------------------
    datevalue = str(datevalue)
    # use regex to to seperate datevalue into "Year, Month, and Day"
    datelist = re.findall(r"[\w']+", datevalue)
    if len(datelist) > 1:
        y = int(datelist[0])
        m = int(datelist[1])
        d = int(datelist[2])
    elif len(datelist) == 1:
        y = int(datevalue[0:4])
        m = int(datevalue[4:6])
        d = int(datevalue[6:8])
    # pass the datelist values to the "date" fuction
    startdate = date(y, m, d)
    nextmonth = startdate.replace(day=28) + \
        timedelta(days=4)  # this will never fail
    lastworkday = nextmonth - timedelta(days=nextmonth.day)
    # If lastworkday is a weekend then move to Friday
    # Saturday (subtract 1 day)
    if lastworkday.isoweekday() == 6:
        lastworkday -= timedelta(days=1)
    # Sunday (subtract 2 days)
    elif lastworkday.isoweekday() == 7:
        lastworkday -= timedelta(days=2)
    return lastworkday


def lastDayofMonth(datevalue):
    # -------------------------------------------------------------------------
    # Last day of Month Function
    # -------------------------------------------------------------------------
    datevalue = str(datevalue)
    # use regex to to seperate datevalue into "Year, Month, and Day"
    datelist = re.findall(r"[\w']+", datevalue)
    if len(datelist) > 1:
        y = int(datelist[0])
        m = int(datelist[1])
        d = int(datelist[2])
    elif len(datelist) == 1:
        y = int(datevalue[0:4])
        m = int(datevalue[4:6])
        d = int(datevalue[6:8])
    # pass the datelist values to the "date" fuction
    startdate = date(y, m, d)
    nextmonth = startdate.replace(day=28) + \
        timedelta(days=4)  # this will never fail
    return nextmonth - timedelta(days=nextmonth.day)


def lastWorkdayofQtr(datevalue):
    # -------------------------------------------------------------------------
    # Last Workday of Qtr Function
    # -------------------------------------------------------------------------
    datevalue = str(datevalue)
    # use regex to to seperate datevalue into "Year, Month, and Day"
    datelist = re.findall(r"[\w']+", datevalue)
    if len(datelist) > 1:
        y = int(datelist[0])
        m = int(datelist[1])
        d = int(datelist[2])
    elif len(datelist) == 1:
        y = int(datevalue[0:4])
        m = int(datevalue[4:6])
        d = int(datevalue[6:8])
    # pass the datelist values to the "date" fuction
    startdate = date(y, m, d)
    startdate = startdate.replace(day=28)
    # -------------------------------------------------------------------------
    # Qtr list
    # -------------------------------------------------------------------------
    Q1 = ['Nov', 'Dec', 'Jan']
    Q2 = ['Feb', 'Mar', 'Apr']
    Q3 = ['May', 'Jun', 'Jul']
    Q4 = ['Aug', 'Sep', 'Oct']
    # -------------------------------------------------------------------------
    # Find End of Qtr Month
    # -------------------------------------------------------------------------
    # Q1 cutoff month is Jan
    if startdate.strftime("%b") in Q1:
        # If month is in Nov or Dec, update year by 1
        if startdate.strftime("%b") in ('Nov', 'Dec'):
            startdate = startdate.replace(startdate.year + 1)
        startdate = startdate.replace(month=1)
    # Q2 cutoff month is Apr
    elif startdate.strftime("%b") in Q2:
        startdate = startdate.replace(month=4)
    # Q3 cutoff month is Jul
    elif startdate.strftime("%b") in Q3:
        startdate = startdate.replace(month=7)
    # Q4 cutoff month is Oct
    elif startdate.strftime("%b") in Q4:
        startdate = startdate.replace(month=10)
    # -------------------------------------------------------------------------
    # Set last day of qtr
    # -------------------------------------------------------------------------
    nextmonth = startdate + timedelta(days=4)  # this will never fail
    lastworkday = nextmonth - timedelta(days=nextmonth.day)
    # If lastworkday of qtr is a weekend then move to Friday
    # Saturday (subtract 1 day)
    if lastworkday.isoweekday() == 6:
        lastworkday -= timedelta(days=1)
    # Sunday (subtract 2 days)
    elif lastworkday.isoweekday() == 7:
        lastworkday -= timedelta(days=2)
    return lastworkday
