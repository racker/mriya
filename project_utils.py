__author__ = 'Volodymyr Varchuk'


import sys


# Print iterations progress
def printProgress (iteration, total, prefix = '', suffix = '', decimals = 1, barLength = 100):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        barLength   - Optional  : character length of bar (Int)
    """
    formatStr       = "{0:." + str(decimals) + "f}"

    if total == 0:
        percents        = formatStr.format(100)
        return 0

    percents        = formatStr.format(100 * (iteration / float(total)))
    filledLength    = int(round(barLength * iteration / float(total)))
    bar             = '=' * filledLength + '-' * (barLength - filledLength)
    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),
    sys.stdout.flush()
    if iteration >= total:
        percents        = formatStr.format(100 * (total / float(total)))
        sys.stdout.write('\r%s |%s| %s%s %s\n' % (prefix, bar, percents, '%', suffix)),
        sys.stdout.flush()
        return 0
    return 1


def success_records_check( data, key_name='success'):
    success_count = 0
    for element in data:
        if element['success'] == True:
            success_count = success_count + 1
    return '{0}/{1}'.format(success_count, len(data))