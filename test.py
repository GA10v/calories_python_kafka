import requests 
r = requests.get('https://www.allrecipes.com/recipe/14172/caesar-salad-supreme/')
print(r.text)
s = r.text
print()
print('==================================')
print()
print(s.strip())