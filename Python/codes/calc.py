import logging

loggger = logging.getLogger(__name__)
loggger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')

file_handler = logging.FileHandler('calc.log')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.ERROR)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.DEBUG)

loggger.addHandler(file_handler)
loggger.addHandler(stream_handler)

def add(x, y):
    return x + y

def sub(x, y):
    return x -y 

def mul(x, y):
    return x * y

def div(x, y):
    try:
        result = x/y
    except ZeroDivisionError:
        # to trace the error
        loggger.exception('You are dividing by Zero')
    else:
        return result
    
if __name__ == "__main__":
    x = 10
    y = 0
    result = add(x,y)
    loggger.debug('Add: {} + {} = {}'.format(x,y,result))

    result = sub(x,y)
    loggger.debug('Sub: {} - {} = {}'.format(x,y,result))

    result = mul(x,y)
    loggger.debug('Mul: {} * {} = {}'.format(x,y,result))

    result = div(x,y)
    loggger.debug('Div: {} / {} = {}'.format(x,y,result))
