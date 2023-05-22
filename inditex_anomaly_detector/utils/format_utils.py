def format_number(data,index):
    if abs(data) >= 1000000:
        formatter = '{:1.1f}M'.format(data*0.000001)
    else:
        formatter = '{:1.1f}K'.format(data*0.001)
    return formatter