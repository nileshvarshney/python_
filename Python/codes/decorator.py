from functools import wraps
#########################################
# Example of decorator function
#########################################
def decorator_function(original_function):
    def wrapper_function(*args, **kwargs):
        print('wrapper execute this before {}'.format(original_function.__name__))
        return original_function(*args, *kwargs)
    return wrapper_function

@decorator_function
def display():
    print('display function ran')

@decorator_function
def display_info(name, age):
    print('display info ran with argument {} and {}'.format(name, age))


display()
display_info('john', 25)

#########################################
# Example of decorator class
#########################################
class decorator_class(object):

    def __init__(self, original_function):
        self.original_function = original_function

    def __call__(self,*args, **kwargs):
        print(
            'Call function executed before {}'.format(
            self.original_function.__name__))
        return self.original_function(*args, **kwargs)

@decorator_class
def display_c():
    print('display function ran')

@decorator_class
def display_info_c(name, age):
    print('display info ran with argument {} and {}'.format(
        name, age))

print("*****************************************")
display_c()
display_info_c('john', 25)

########################################
# Practical Example of decorator
########################################
# Suppose I want to log, how many time a function called 
# and what are the parameters passed

def my_logger(original_function):
    import logging
    logging.basicConfig(
        filename='{}.log'.format(original_function.__name__), 
        level = logging.INFO
    )
    @wraps(original_function)
    def wrapper(*args, **kwargs):
        logging.info('Ran with args {} and kwargs {}'.format(
            args, kwargs
        ))
        original_function(*args, **kwargs)
    return wrapper

def my_timer(original_function):
    import time

    @wraps(original_function)
    def wrapper(*args,**kwargs):
        t1 = time.time()
        result = original_function(*args, *kwargs)
        t2 = time.time() - t1
        print('{} Ran for {} seconds'.format(original_function.__name__, t2))
    return wrapper


@my_logger
@my_timer
def display_info_l(name, age):
    print('display info ran with argument {} and {}'.format(
        name, age))

display_info_l('Nilesh', 25)