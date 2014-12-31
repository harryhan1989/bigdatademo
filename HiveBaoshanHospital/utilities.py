from calendar import monthrange
from datetime import datetime, timedelta
import time
import json
import ConfigParser


class utilities:

    @staticmethod
    def get_all_date_granularity(start_date,end_date):
        '''get month,quarter,half year,year
           0-year,1-querter,2-month,5-half year
           return [{"first":,"last":,"granularity":}...]
        '''
        date_granularities=[]

        dekta=utilities.monthdelta(start_date,end_date)

        for num in range(0,dekta+1):
            dt=utilities.convert_date(start_date)
            date=str(utilities.add_months(dt,num))
            date_of_date_granularities=utilities.list_date_granularity_from_date_str(date)
            date_granularities.extend(date_of_date_granularities)
        

        return utilities.array_distinct(date_granularities)
        #return date_granularities

    @staticmethod
    def monthdelta(start_date,end_date):
        '''return number'''
        date1=utilities.convert_date(start_date)
        date2=utilities.convert_date(end_date)
        delta = 0
        while True:
            mdays = monthrange(date1.year, date1.month)[1]
            date1 += timedelta(days=mdays)
            if date1 <= date2:
                delta += 1
            else:
                break
        return delta

    @staticmethod
    def list_date_granularity_from_date_str(date):
        '''list month,quarter,half year,year granularities from date,
           0-year,1-querter,2-month,5-half year
           return [{"first":,"last":,"granularity":}...]
        '''
        year_first_last_days=utilities.get_first_last_day_of_year_str(date)
        year_granularity={"first":year_first_last_days.get("first"),"last":year_first_last_days.get("last"),"granularity":"0"}

        halfyear_first_last_days=utilities.get_first_last_day_of_halfyear_str(date)
        halfyear_granularity={"first":halfyear_first_last_days.get("first"),"last":halfyear_first_last_days.get("last"),"granularity":"5"}

        quarter_first_last_days=utilities.get_first_last_day_of_quarter_str(date)
        quarter_granularity={"first":quarter_first_last_days.get("first"),"last":quarter_first_last_days.get("last"),"granularity":"1"}

        month_first_last_days=utilities.get_first_last_day_of_month_str(date)
        month_granularity={"first":month_first_last_days.get("first"),"last":month_first_last_days.get("last"),"granularity":"2"}
        
        date_granularities=[]
        date_granularities.append(year_granularity)
        date_granularities.append(halfyear_granularity)
        date_granularities.append(quarter_granularity)
        date_granularities.append(month_granularity)

        return date_granularities

    @staticmethod
    def convert_date(date_str,str_format="%Y-%m-%d"):
        # Expects "YYYY-MM-DD" string
        # returns a datetime object        
        e_seconds = time.mktime(time.strptime(date_str,str_format))
        return datetime.fromtimestamp(e_seconds)

    @staticmethod
    def format_date(date,str_format="%Y-%m-%d"):
        # format a datetime object as YYYY-MM-DD string and return
        return date.strftime(str_format)

    @staticmethod
    def add_months(dt,months):
        month = dt.month - 1 + months
        year = dt.year + month / 12
        month = month % 12 + 1
        day = min(dt.day,monthrange(year,month)[1])
        return utilities.format_date(dt.replace(year=year, month=month, day=day))

    @staticmethod
    def get_first_last_day_of_month_str(date):
        '''return obj:{first:,last:}'''

        converted_date=utilities.convert_date(date)
        month_start_dt=utilities.format_date(converted_date,"%Y-%m-01")

        d_year = converted_date.strftime("%Y")        #get the year
        d_month = str(int(converted_date.strftime("%m"))%12+1)#get next month, watch rollover
        d_day = "1"                               #first day of next month
        next_month = utilities.convert_date("%s-%s-%s"%(d_year,d_month,d_day))#make a datetime obj for 1st of next month
        delta = timedelta(seconds=1)    #create a delta of 1 second
        month_end_dt=next_month - delta
        return {"first":month_start_dt,"last":str(utilities.format_date(month_end_dt))}

    @staticmethod
    def get_first_last_day_of_quarter_str(datestr):
        '''return obj:{first:,last:}'''

        converted_date=utilities.convert_date(datestr)
        y = converted_date.year
        m = converted_date.month
        if m in (1,2,3):
            quarter_start_dt = datetime(y,1,1)
            quarter_end_dt = datetime(y,4,1) - timedelta(days=1)
        elif m in (4,5,6):
            quarter_start_dt = datetime(y,4,1)
            quarter_end_dt = datetime(y,7,1) - timedelta(days=1)
        elif m in (7,8,9):
            quarter_start_dt = datetime(y,7,1)
            quarter_end_dt = datetime(y,10,1) - timedelta(days=1)
        else:
            quarter_start_dt = datetime(y,10,1)
            quarter_end_dt = datetime(y+1,1,1) - timedelta(days=1)
        return {"first":str(utilities.format_date(quarter_start_dt)),"last":str(utilities.format_date(quarter_end_dt))}

    @staticmethod
    def get_first_last_day_of_halfyear_str(datestr):
        '''return obj:{first:,last:}'''

        converted_date=utilities.convert_date(datestr)
        y = converted_date.year
        m = converted_date.month
        if m in (1,2,3,4,5,6):
            halfyear_start_dt = datetime(y,1,1)
            halfyear_end_dt = datetime(y,7,1) - timedelta(days=1)
        else:
            halfyear_start_dt = datetime(y,7,1)
            halfyear_end_dt = datetime(y+1,1,1) - timedelta(days=1)
        return {"first":str(utilities.format_date(halfyear_start_dt)),"last":str(utilities.format_date(halfyear_end_dt))}

    @staticmethod
    def get_first_last_day_of_year_str(datestr):
        '''return obj:{first:,last:}'''

        converted_date=utilities.convert_date(datestr)
        y = converted_date.year
        year_start_dt = datetime(y,1,1)
        year_end_dt = datetime(y+1,1,1) - timedelta(days=1)
        return {"first":str(utilities.format_date(year_start_dt)),"last":str(utilities.format_date(year_end_dt))}

    @staticmethod
    def array_distinct(array_obj):
        new_obj=[]
        for obj in array_obj:
            if obj not in new_obj:
               new_obj.append(obj)
        return new_obj

    @staticmethod
    def inital_config_file():
        config = ConfigParser.RawConfigParser()
        config.add_section('Section1')
        config.set('Section1', 'tmp_data_path', '')
        config.set('Section1', 'tmp_data_name', 'tmpdata')
        config.set('Section1', 'max_pool_size', '20')
        config.set('Section1', 'formula_path', 'formulas')
        config.set('Section1', 'time_to_run', '01:00')
        config.set('Section1', 'f_tmp_start_date', '20130922')
        config.set('Section1', 'f_tmp_end_date', '20130923')
        # Writing our configuration file to 'example.cfg'
        with open('config.cfg', 'wb') as configfile:
            config.write(configfile)

    @staticmethod
    def get_config_value(config_name):
        config = ConfigParser.RawConfigParser()
        config.read('config.cfg')
        value=config.get('Section1',config_name)
        return value