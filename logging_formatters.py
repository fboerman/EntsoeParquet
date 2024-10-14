def logger_format(record):
    record['time'] = record['time'].strftime('%Y-%m-%d %H:%M:%S')
    level = record['level']
    record['level'] = level.name
    if level.no <= 50:
        record['color'] = 'red'
    if level.no <= 30:
        record['color'] = 'yellow'
    if level.no <= 20:
        record['color'] = 'green'
    if level.no <= 10:
        record['color'] = 'blue'
    if level.no == 30 or level.no == 50:
        return "<{color}>{time}\t|{level}\t| {message}</{color}>\n".format(**record)
    else:
        return "<{color}>{time}\t|{level}\t\t| {message}</{color}>\n".format(**record)