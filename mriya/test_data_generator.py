__author__ = 'Volodymyr Varchuk'


import datetime
import random
import string


data_struct = {
    'Name':'text',
    'type':'ObjectName',
    'Billing_Address__c':'text',
    'Billing_City__c':'text',
    'Billing_County__c':'text',
    'Billing_Zip_Code__c':'int',
    'Shipping_Address__c':'text',
    'Shipping_City__c':'text',
    'Shipping_County__c':'text',
    'Shipping_Zip_Code__c':'int',
    'Account_Birthday__c':'date',
    'Website':'text'
}

defaults = {
    'Billing_City__c':'San Antonio',
    'Billing_County__c':'USA',
    'Shipping_City__c':'San Antonio',
    'Shipping_County__c':'USA',
    'Website':'www.rackspace.com'
}

prefixes = {
    'Name':'Account name_',
    'Billing_Address__c':'Street_Billing_Address_',
    'Shipping_Address__c':'Street_Shipping_Address_',
}


class DataGenerator:
    def __init__(self, data_struct, defaults, prefixes):
        self.raw_date = []
        self.data_struct = data_struct
        self.defaults = defaults
        self.prefixes = prefixes

    def gen_data(self, row_number, object_name):
        rows = []
        for i in range(row_number):
            row = self.data_struct.copy()
            found_object = False
            found_id = False
            for key in row:
                found_prefix = False
                found_default = False
                if row[key] == 'ObjectName':
                    row[key] = object_name
                    found_object = True
                    continue
                for def_key, def_val in self.defaults.iteritems():
                    if def_key == key:
                        row[key] = def_val
                        found_default = True
                        break
                if not found_default:
                    for pref_key, pref_val in self.prefixes.iteritems():
                        if pref_key == key:
                            row[key] = pref_val + self.gen_string(10)
                            found_prefix = True
                            break
                if not found_default and not found_prefix:
                    if row[key] == 'text':
                        row[key] = self.gen_string(20)
                    if row[key] == 'int':
                        row[key] = self.gen_number(10000, 100000)
                    if row[key] == 'date':
                        row[key] = self.gen_date()
            if not found_object:
                row['Type'] = object_name
            rows.append(row)
        return rows

    def gen_number(self, min_int = 0, max_int=100):
        return random.randint(min_int, max_int)

    def gen_string(self, length):
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))

    def gen_date(self, start = datetime.datetime.strptime('1/1/2000 1:30 PM', '%m/%d/%Y %I:%M %p'), end=datetime.datetime.strptime('1/1/2017 1:30 PM', '%m/%d/%Y %I:%M %p')):
        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = random.randrange(int_delta)
        return start + datetime.timedelta(seconds=random_second)

