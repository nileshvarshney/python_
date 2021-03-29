import re

def get_data(pattern): 
    with open('regex_data.txt', 'r') as f:
        contents = f.read()
        matches = pattern.finditer(contents)
        for match in matches:
            print(match)


# pattern = re.compile(r'\d{3}[.-]\d{3}[.-]\d{4}')
# get_data(pattern)


# get only phone # starting from 800/900
# pattern = re.compile(r'[89][0]{2}[.-]\d{3}[.-]\d{4}')
# get_data(pattern)

# get all emails
# pattern = re.compile(r'[a-zA-Z0-9._-]+@[a-zA-Z0-9_-]+\.[a-zA-Z0-9._-]+')
# get_data(pattern)


urls = '''
https://www.google.com
http://youtube.com
http://www.yahoo.com
https://nasa.gov
'''

pattern = re.compile(r'https?://(www\.)?\w+\.\w+')
matches = pattern.finditer(urls)

my_list =[]
for match in matches:
    print(match.group(0))
    my_list.append(match.group(0))

print(my_list)

